#pragma once

#include "MeshCore.h"
#include "esp_now.h"
#include "helpers/bridges/BridgeBase.h"

#include <freertos/FreeRTOS.h>

#ifdef WITH_ESPNOW_BRIDGE

/**
 * @brief Bridge implementation using ESP-NOW protocol for packet transport
 *
 * This bridge enables mesh packet transport over ESP-NOW, a connectionless communication
 * protocol provided by Espressif that allows ESP32 devices to communicate directly
 * without WiFi router infrastructure.
 *
 * Features:
 * - Broadcast-based communication (all bridges receive all packets)
 * - Network isolation using XOR encryption with shared secret
 * - Duplicate packet detection using SimpleMeshTables tracking
 * - Maximum packet size of 250 bytes (ESP-NOW limitation)
 *
 * Packet Structure:
 * [2 bytes] Magic Header - Used to identify ESPNowBridge packets
 * [2 bytes] Fletcher-16 checksum of encrypted payload (calculated over payload only)
 * [246 bytes max] Encrypted payload containing the mesh packet
 *
 * The Fletcher-16 checksum is used to validate packet integrity and detect
 * corrupted or tampered packets. It's calculated over the encrypted payload
 * and provides a simple but effective way to verify packets are both
 * uncorrupted and from the same network (since the checksum is calculated
 * after encryption).
 *
 * Configuration:
 * - Define WITH_ESPNOW_BRIDGE to enable this bridge
 * - Define _prefs->bridge_secret with a string to set the network encryption key
 *
 * Network Isolation:
 * Multiple independent mesh networks can coexist by using different
 * _prefs->bridge_secret values. Packets encrypted with a different key will
 * fail the checksum validation and be discarded.
 */
class ESPNowBridge : public BridgeBase {
private:
  static ESPNowBridge *_instance;
  static void recv_cb(const uint8_t *mac, const uint8_t *data, int32_t len);
  static void send_cb(const uint8_t *mac, esp_now_send_status_t status);

  /**
   * ESP-NOW Protocol Structure:
   * - ESP-NOW header: 20 bytes (handled by ESP-NOW protocol)
   * - ESP-NOW payload: 250 bytes maximum
   * Total ESP-NOW packet: 270 bytes
   *
   * Our Bridge Packet Structure (must fit in ESP-NOW payload):
   * - Magic header: 2 bytes
   * - Checksum: 2 bytes
   * - Available payload: 246 bytes
   */
  static const size_t MAX_ESPNOW_PACKET_SIZE = 250;

  /**
   * Size constants for packet parsing
   */
  static const size_t MAX_PAYLOAD_SIZE = MAX_ESPNOW_PACKET_SIZE - (BRIDGE_MAGIC_SIZE + BRIDGE_CHECKSUM_SIZE);

  /**
   * The ESP-NOW receive callback runs in the WiFi task, while the packet pool
   * and the seen-packets table are only safe to touch from the main loop.
   * The callback therefore just stashes raw frames in this ring buffer, and
   * loop() decodes them on the main loop.
   */
  static const int RX_RING_SIZE = 4;
  struct RxFrame {
    uint8_t len;
    uint8_t data[MAX_ESPNOW_PACKET_SIZE];
  };
  RxFrame _rx_ring[RX_RING_SIZE];
  volatile int _rx_ring_head;   // next slot to write; only advanced by the WiFi task
  volatile int _rx_ring_tail;   // next slot to read; only advanced by the main loop
  volatile uint32_t _rx_dropped;          // frames dropped because the ring was full
  uint32_t _rx_dropped_reported;
  portMUX_TYPE _rx_ring_mux = portMUX_INITIALIZER_UNLOCKED;

  /**
   * Decodes one raw ESP-NOW frame (magic, decrypt, checksum) and queues the
   * mesh packet. Must only be called from the main loop.
   *
   * @param data Raw frame bytes as received over ESP-NOW
   * @param len Length of the frame in bytes
   */
  void processRxFrame(const uint8_t *data, int len);

  /**
   * Performs XOR encryption/decryption of data
   * Used to isolate different mesh networks
   *
   * Uses _prefs->bridge_secret as the key in a simple XOR operation.
   * The same operation is used for both encryption and decryption.
   * While not cryptographically secure, it provides basic network isolation.
   *
   * @param data Pointer to data to encrypt/decrypt
   * @param len Length of data in bytes
   */
  void xorCrypt(uint8_t *data, size_t len);

  /**
   * ESP-NOW receive callback
   * Called by ESP-NOW when a packet is received. Runs in the WiFi task, so it
   * only copies the raw frame into the RX ring buffer (see loop()).
   *
   * @param mac Source MAC address
   * @param data Received data
   * @param len Length of received data
   */
  void onDataRecv(const uint8_t *mac, const uint8_t *data, int32_t len);

  /**
   * ESP-NOW send callback
   * Called by ESP-NOW after a transmission attempt
   *
   * @param mac_addr Destination MAC address
   * @param status Transmission status
   */
  void onDataSent(const uint8_t *mac_addr, esp_now_send_status_t status);

public:
  /**
   * Constructs an ESPNowBridge instance
   *
   * @param prefs Node preferences for configuration settings
   * @param mgr PacketManager for allocating and queuing packets
   * @param rtc RTCClock for timestamping debug messages
   */
  ESPNowBridge(NodePrefs *prefs, mesh::PacketManager *mgr, mesh::RTCClock *rtc);

  /**
   * Initializes the ESP-NOW bridge
   *
   * - Configures WiFi in station mode
   * - Initializes ESP-NOW protocol
   * - Registers callbacks
   * - Sets up broadcast peer
   */
  void begin() override;

  /**
   * Stops the ESP-NOW bridge
   *
   * - Removes broadcast peer
   * - Unregisters callbacks
   * - Deinitializes ESP-NOW protocol
   * - Turns off WiFi to release radio resources
   */
  void end() override;

  /**
   * Main loop handler
   * Decodes the frames stashed by the ESP-NOW receive callback, so that all
   * packet pool and seen-table access happens on the main loop.
   */
  void loop() override;

  /**
   * Called when a packet is received via ESP-NOW
   * Queues the packet for mesh processing if not seen before
   *
   * @param packet The received mesh packet
   */
  void onPacketReceived(mesh::Packet *packet) override;

  /**
   * Called when a packet needs to be transmitted via ESP-NOW
   * Encrypts and broadcasts the packet if not seen before
   *
   * @param packet The mesh packet to transmit
   */
  void sendPacket(mesh::Packet *packet) override;
};

#endif
