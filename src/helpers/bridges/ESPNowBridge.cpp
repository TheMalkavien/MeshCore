#include "ESPNowBridge.h"

#include <WiFi.h>
#include <esp_wifi.h>

#ifdef WITH_ESPNOW_BRIDGE

// Static member to handle callbacks
ESPNowBridge *ESPNowBridge::_instance = nullptr;

// Static callback wrappers
void ESPNowBridge::recv_cb(const uint8_t *mac, const uint8_t *data, int32_t len) {
  if (_instance) {
    _instance->onDataRecv(mac, data, len);
  }
}

void ESPNowBridge::send_cb(const uint8_t *mac, esp_now_send_status_t status) {
  if (_instance) {
    _instance->onDataSent(mac, status);
  }
}

ESPNowBridge::ESPNowBridge(NodePrefs *prefs, mesh::PacketManager *mgr, mesh::RTCClock *rtc)
    : BridgeBase(prefs, mgr, rtc), _rx_ring_head(0), _rx_ring_tail(0),
      _rx_dropped(0), _rx_dropped_reported(0) {
  _instance = this;
}

void ESPNowBridge::begin() {
  BRIDGE_DEBUG_PRINTLN("Initializing...\n");

  // Reset the RX ring before the receive callback can fire
  _rx_ring_head = _rx_ring_tail = 0;

  // Initialize WiFi in station mode
  WiFi.mode(WIFI_STA);

#ifdef ESPNOW_BRIDGE_SHARED_WIFI
  // WiFi STA is shared with the application (e.g. companion WiFi interface):
  // the channel is dictated by the AP association, so it must not be forced here.
  // Modem power-save would make ESP-NOW reception unreliable while associated.
  WiFi.setSleep(false);
#else
  // Set Wi-Fi channel
  if (esp_wifi_set_channel(_prefs->bridge_channel, WIFI_SECOND_CHAN_NONE) != ESP_OK) {
    BRIDGE_DEBUG_PRINTLN("Error setting WIFI channel to %d\n", _prefs->bridge_channel);
    return;
  }
#endif

  // Initialize ESP-NOW
  if (esp_now_init() != ESP_OK) {
    BRIDGE_DEBUG_PRINTLN("Error initializing ESP-NOW\n");
    return;
  }

  // Register callbacks
  esp_now_register_recv_cb(recv_cb);
  esp_now_register_send_cb(send_cb);

  // Add broadcast peer
  esp_now_peer_info_t peerInfo = {};
  memset(&peerInfo, 0, sizeof(peerInfo));
  memset(peerInfo.peer_addr, 0xFF, ESP_NOW_ETH_ALEN); // Broadcast address
#ifdef ESPNOW_BRIDGE_SHARED_WIFI
  peerInfo.channel = 0;   // 0 = follow whatever channel the WiFi STA is currently on
#else
  peerInfo.channel = _prefs->bridge_channel;
#endif
  peerInfo.encrypt = false;

  if (esp_now_add_peer(&peerInfo) != ESP_OK) {
    BRIDGE_DEBUG_PRINTLN("Failed to add broadcast peer\n");
    return;
  }

  // Update bridge state
  _initialized = true;
}

void ESPNowBridge::end() {
  BRIDGE_DEBUG_PRINTLN("Stopping...\n");

  // Remove broadcast peer
  uint8_t broadcastAddress[] = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };
  if (esp_now_del_peer(broadcastAddress) != ESP_OK) {
    BRIDGE_DEBUG_PRINTLN("Error removing broadcast peer\n");
  }

  // Unregister callbacks
  esp_now_register_recv_cb(nullptr);
  esp_now_register_send_cb(nullptr);

  // Deinitialize ESP-NOW
  if (esp_now_deinit() != ESP_OK) {
    BRIDGE_DEBUG_PRINTLN("Error deinitializing ESP-NOW\n");
  }

#ifndef ESPNOW_BRIDGE_SHARED_WIFI
  // Turn off WiFi (owned by the bridge in standalone mode)
  WiFi.mode(WIFI_OFF);
#endif

  // Update bridge state
  _initialized = false;
}

void ESPNowBridge::loop() {
  // Decode the frames stashed by the ESP-NOW receive callback. The callback
  // runs in the WiFi task, where the packet pool and seen-packets table must
  // not be touched — all of that work happens here, on the main loop.
  while (true) {
    portENTER_CRITICAL(&_rx_ring_mux);
    if (_rx_ring_tail == _rx_ring_head) {
      portEXIT_CRITICAL(&_rx_ring_mux);
      break;
    }
    RxFrame frame = _rx_ring[_rx_ring_tail];
    _rx_ring_tail = (_rx_ring_tail + 1) % RX_RING_SIZE;
    portEXIT_CRITICAL(&_rx_ring_mux);

    processRxFrame(frame.data, frame.len);
  }

  if (_rx_dropped != _rx_dropped_reported) {
    BRIDGE_DEBUG_PRINTLN("RX ring full, total dropped=%u\n", (unsigned)_rx_dropped);
    _rx_dropped_reported = _rx_dropped;
  }
}

void ESPNowBridge::xorCrypt(uint8_t *data, size_t len) {
  size_t keyLen = strlen(_prefs->bridge_secret);
  for (size_t i = 0; i < len; i++) {
    data[i] ^= _prefs->bridge_secret[i % keyLen];
  }
}

void ESPNowBridge::onDataRecv(const uint8_t *mac, const uint8_t *data, int32_t len) {
  // Runs in the WiFi task: do not touch the packet pool, the seen-packets
  // table or Serial here. Just stash the raw frame; loop() decodes it on the
  // main loop.
  if (len < (int32_t)(BRIDGE_MAGIC_SIZE + BRIDGE_CHECKSUM_SIZE) ||
      len > (int32_t)MAX_ESPNOW_PACKET_SIZE) {
    return;   // can't be one of our frames
  }

  portENTER_CRITICAL(&_rx_ring_mux);
  int next = (_rx_ring_head + 1) % RX_RING_SIZE;
  if (next == _rx_ring_tail) {   // ring full, drop the frame (reported in loop())
    _rx_dropped++;
    portEXIT_CRITICAL(&_rx_ring_mux);
    return;
  }
  RxFrame *slot = &_rx_ring[_rx_ring_head];
  slot->len = (uint8_t)len;
  memcpy(slot->data, data, len);
  _rx_ring_head = next;
  portEXIT_CRITICAL(&_rx_ring_mux);
}

void ESPNowBridge::processRxFrame(const uint8_t *data, int len) {
  // Check packet header magic
  uint16_t received_magic = (data[0] << 8) | data[1];
  if (received_magic != BRIDGE_PACKET_MAGIC) {
    BRIDGE_DEBUG_PRINTLN("RX invalid magic 0x%04X\n", received_magic);
    return;
  }

  // Make a copy we can decrypt
  uint8_t decrypted[MAX_ESPNOW_PACKET_SIZE];
  const size_t encryptedDataLen = len - BRIDGE_MAGIC_SIZE;
  memcpy(decrypted, data + BRIDGE_MAGIC_SIZE, encryptedDataLen);

  // Try to decrypt (checksum + payload)
  xorCrypt(decrypted, encryptedDataLen);

  // Validate checksum
  uint16_t received_checksum = (decrypted[0] << 8) | decrypted[1];
  const size_t payloadLen = encryptedDataLen - BRIDGE_CHECKSUM_SIZE;

  if (!validateChecksum(decrypted + BRIDGE_CHECKSUM_SIZE, payloadLen, received_checksum)) {
    // Failed to decrypt - likely from a different network
    BRIDGE_DEBUG_PRINTLN("RX checksum mismatch, rcv=0x%04X\n", received_checksum);
    return;
  }

  BRIDGE_DEBUG_PRINTLN("RX, payload_len=%d\n", payloadLen);

  // Create mesh packet
  mesh::Packet *pkt = _mgr->allocNew();
  if (!pkt) return;

  if (pkt->readFrom(decrypted + BRIDGE_CHECKSUM_SIZE, payloadLen)) {
    onPacketReceived(pkt);
  } else {
    _mgr->free(pkt);
  }
}

void ESPNowBridge::onDataSent(const uint8_t *mac_addr, esp_now_send_status_t status) {
  // Could add transmission error handling here if needed
}

void ESPNowBridge::sendPacket(mesh::Packet *packet) {
  // Guard against uninitialized state
  if (_initialized == false) {
    return;
  }

  // First validate the packet pointer
  if (!packet) {
    BRIDGE_DEBUG_PRINTLN("TX invalid packet pointer\n");
    return;
  }

  if (!_seen_packets.wasSeen(packet)) {
    _seen_packets.markSeen(packet);
    // Create a temporary buffer just for size calculation and reuse for actual writing
    uint8_t sizingBuffer[MAX_PAYLOAD_SIZE];
    uint16_t meshPacketLen = packet->writeTo(sizingBuffer);

    // Check if packet fits within our maximum payload size
    if (meshPacketLen > MAX_PAYLOAD_SIZE) {
      BRIDGE_DEBUG_PRINTLN("TX packet too large (payload=%d, max=%d)\n", meshPacketLen,
                           MAX_PAYLOAD_SIZE);
      return;
    }

    uint8_t buffer[MAX_ESPNOW_PACKET_SIZE];

    // Write magic header (2 bytes)
    buffer[0] = (BRIDGE_PACKET_MAGIC >> 8) & 0xFF;
    buffer[1] = BRIDGE_PACKET_MAGIC & 0xFF;

    // Write packet payload starting after magic header and checksum
    const size_t packetOffset = BRIDGE_MAGIC_SIZE + BRIDGE_CHECKSUM_SIZE;
    memcpy(buffer + packetOffset, sizingBuffer, meshPacketLen);

    // Calculate and add checksum (only of the payload)
    uint16_t checksum = fletcher16(buffer + packetOffset, meshPacketLen);
    buffer[2] = (checksum >> 8) & 0xFF; // High byte
    buffer[3] = checksum & 0xFF;        // Low byte

    // Encrypt payload and checksum (not including magic header)
    xorCrypt(buffer + BRIDGE_MAGIC_SIZE, meshPacketLen + BRIDGE_CHECKSUM_SIZE);

    // Total packet size: magic header + checksum + payload
    const size_t totalPacketSize = BRIDGE_MAGIC_SIZE + BRIDGE_CHECKSUM_SIZE + meshPacketLen;

    // Broadcast using ESP-NOW
    uint8_t broadcastAddress[] = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };
    esp_err_t result = esp_now_send(broadcastAddress, buffer, totalPacketSize);

    if (result == ESP_OK) {
      BRIDGE_DEBUG_PRINTLN("TX, len=%d\n", meshPacketLen);
    } else {
      BRIDGE_DEBUG_PRINTLN("TX FAILED!\n");
    }
  }
}

void ESPNowBridge::onPacketReceived(mesh::Packet *packet) {
  handleReceivedPacket(packet);
}

#endif
