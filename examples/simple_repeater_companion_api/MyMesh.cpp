#include "MyMesh.h"
#include <algorithm>

extern "C" bool meshcore_board_usb_on_demand(void) __attribute__((weak));
extern "C" bool meshcore_board_usb_on_demand(void) {
  return false;
}
extern "C" bool meshcore_board_usb_off_demand(void) __attribute__((weak));
extern "C" bool meshcore_board_usb_off_demand(void) {
  return false;
}
extern "C" bool meshcore_board_usb_is_connected(void) __attribute__((weak));
extern "C" bool meshcore_board_usb_is_connected(void) {
  return false;
}

/* ------------------------------ Config -------------------------------- */

#ifndef LORA_FREQ
  #define LORA_FREQ 915.0
#endif
#ifndef LORA_BW
  #define LORA_BW 250
#endif
#ifndef LORA_SF
  #define LORA_SF 10
#endif
#ifndef LORA_CR
  #define LORA_CR 5
#endif
#ifndef LORA_TX_POWER
  #define LORA_TX_POWER 20
#endif

#ifndef ADVERT_NAME
  #define ADVERT_NAME "repeater"
#endif
#ifndef ADVERT_LAT
  #define ADVERT_LAT 0.0
#endif
#ifndef ADVERT_LON
  #define ADVERT_LON 0.0
#endif

#ifndef ADMIN_PASSWORD
  #define ADMIN_PASSWORD "password"
#endif
#define ADMIN_PASSWORD "123456" // For easy configuration

#ifndef SERVER_RESPONSE_DELAY
  #define SERVER_RESPONSE_DELAY 300
#endif

#ifndef TXT_ACK_DELAY
  #define TXT_ACK_DELAY 200
#endif

// Optional reliability helper: if no other repeater is heard forwarding the same
// flood group packet, re-send our already-forwarded packet up to N times.
#ifndef ENABLE_GROUP_FLOOD_CONDITIONAL_RETRY
  #define ENABLE_GROUP_FLOOD_CONDITIONAL_RETRY 1
#endif
#ifndef GROUP_FLOOD_RETRY_MAX_RETRANSMITS
  #define GROUP_FLOOD_RETRY_MAX_RETRANSMITS 3
#endif
#ifndef GROUP_FLOOD_RETRY_CONFIRM_WINDOW_MS
  #define GROUP_FLOOD_RETRY_CONFIRM_WINDOW_MS 1800
#endif
#ifndef GROUP_FLOOD_RETRY_GAP_MIN_MS
  #define GROUP_FLOOD_RETRY_GAP_MIN_MS 350
#endif
#ifndef GROUP_FLOOD_RETRY_GAP_MAX_MS
  #define GROUP_FLOOD_RETRY_GAP_MAX_MS 1200
#endif
#ifndef GROUP_FLOOD_RETRY_AIRTIME_FACTOR
  #define GROUP_FLOOD_RETRY_AIRTIME_FACTOR 6
#endif

static bool readPacketWireExact(mesh::Packet* dst, const uint8_t raw[], uint8_t raw_len) {
  return dst->readFrom(raw, raw_len);
}

#define FIRMWARE_VER_LEVEL       2

#define REQ_TYPE_GET_STATUS         0x01 // same as _GET_STATS
#define REQ_TYPE_KEEP_ALIVE         0x02
#define REQ_TYPE_GET_TELEMETRY_DATA 0x03
#define REQ_TYPE_GET_ACCESS_LIST    0x05
#define REQ_TYPE_GET_NEIGHBOURS     0x06
#define REQ_TYPE_GET_OWNER_INFO     0x07     // FIRMWARE_VER_LEVEL >= 2
#define REQ_TYPE_OTA_BINARY         0x70

#define RESP_SERVER_LOGIN_OK        0 // response to ANON_REQ

#define ANON_REQ_TYPE_REGIONS      0x01
#define ANON_REQ_TYPE_OWNER        0x02
#define ANON_REQ_TYPE_BASIC        0x03   // just remote clock

#define CLI_REPLY_DELAY_MILLIS      600
#ifndef OTA_CLI_REPLY_DELAY_MILLIS
  #define OTA_CLI_REPLY_DELAY_MILLIS 40
#endif

#ifndef OTA_TXT_ACK_DELAY
  #define OTA_TXT_ACK_DELAY 0
#endif

#define LAZY_CONTACTS_WRITE_DELAY    5000

#define COMPANION_FW_VER_CODE         9

#ifndef MAX_LORA_TX_POWER
  #define MAX_LORA_TX_POWER LORA_TX_POWER
#endif

// Companion command codes (subset needed for HA / meshcore-py)
#define CMD_APP_START                 1
#define CMD_SEND_TXT_MSG              2
#define CMD_GET_CONTACTS              4
#define CMD_SET_ADVERT_NAME           8
#define CMD_SYNC_NEXT_MESSAGE         10
#define CMD_RESET_PATH                13
#define CMD_GET_BATT_AND_STORAGE      20
#define CMD_DEVICE_QEURY              22
#define CMD_EXPORT_PRIVATE_KEY        23
#define CMD_SEND_LOGIN                26
#define CMD_SEND_STATUS_REQ           27
#define CMD_GET_CHANNEL               31
#define CMD_SET_OTHER_PARAMS          38
#define CMD_SEND_TELEMETRY_REQ        39
#define CMD_SEND_BINARY_REQ           50

// Companion response / push codes
#define RESP_CODE_OK                  0
#define RESP_CODE_ERR                 1
#define RESP_CODE_CONTACTS_START      2
#define RESP_CODE_CONTACT             3
#define RESP_CODE_END_OF_CONTACTS     4
#define RESP_CODE_SELF_INFO           5
#define RESP_CODE_SENT                6
#define RESP_CODE_CONTACT_MSG_RECV_V3 16
#define RESP_CODE_NO_MORE_MESSAGES    10
#define RESP_CODE_BATT_AND_STORAGE    12
#define RESP_CODE_DEVICE_INFO         13
#define RESP_CODE_PRIVATE_KEY         14
#define RESP_CODE_DISABLED            15
#define RESP_CODE_CHANNEL_INFO        18

#define PUSH_CODE_ADVERT              0x80
#define PUSH_CODE_PATH_UPDATED        0x81
#define PUSH_CODE_LOGIN_SUCCESS       0x85
#define PUSH_CODE_LOGIN_FAIL          0x86
#define PUSH_CODE_STATUS_RESPONSE     0x87
#define PUSH_CODE_TELEMETRY_RESPONSE  0x8B
#define PUSH_CODE_BINARY_RESPONSE     0x8C

#define ERR_CODE_UNSUPPORTED_CMD      1
#define ERR_CODE_NOT_FOUND            2
#define ERR_CODE_TABLE_FULL           3
#define ERR_CODE_BAD_STATE            4
#define ERR_CODE_ILLEGAL_ARG          6

#define MSG_SEND_FAILED               0
#define MSG_SEND_SENT_FLOOD           1
#define MSG_SEND_SENT_DIRECT          2

#define SEND_TIMEOUT_BASE_MILLIS        500
#define FLOOD_SEND_TIMEOUT_FACTOR       16.0f
#define DIRECT_SEND_PERHOP_FACTOR       6.0f
#define DIRECT_SEND_PERHOP_EXTRA_MILLIS 250

static uint32_t calcFloodTimeoutMillisFor(uint32_t pkt_airtime_millis) {
  return SEND_TIMEOUT_BASE_MILLIS + (FLOOD_SEND_TIMEOUT_FACTOR * pkt_airtime_millis);
}

static uint32_t calcDirectTimeoutMillisFor(uint32_t pkt_airtime_millis, uint8_t path_len) {
  return SEND_TIMEOUT_BASE_MILLIS +
         ((pkt_airtime_millis * DIRECT_SEND_PERHOP_FACTOR + DIRECT_SEND_PERHOP_EXTRA_MILLIS) * (path_len + 1));
}

mesh::DispatcherAction MyMesh::onRecvPacket(mesh::Packet* pkt) {
  mesh::DispatcherAction action = mesh::Mesh::onRecvPacket(pkt);

#if ENABLE_GROUP_FLOOD_CONDITIONAL_RETRY == 1
  if (pkt->isRouteFlood() &&
      (pkt->getPayloadType() == PAYLOAD_TYPE_GRP_TXT || pkt->getPayloadType() == PAYLOAD_TYPE_GRP_DATA)) {
    if (action > ACTION_MANUAL_HOLD) {
      trackGroupFloodForward(pkt, action);
    }
    markGroupFloodHeard(pkt);
  }
#endif

  return action;
}

void MyMesh::clearGroupFloodRetryState() {
  memset(_group_retry, 0, sizeof(_group_retry));
  _group_retry_tracked = 0;
  _group_retry_confirmed = 0;
  _group_retry_failed = 0;
  _group_retry_retransmits = 0;
}

void MyMesh::trackGroupFloodForward(const mesh::Packet* pkt, mesh::DispatcherAction action) {
#if ENABLE_GROUP_FLOOD_CONDITIONAL_RETRY == 1
  uint8_t hash[MAX_HASH_SIZE];
  pkt->calculatePacketHash(hash);

  int use_idx = -1;
  for (int i = 0; i < (int)(sizeof(_group_retry) / sizeof(_group_retry[0])); i++) {
    if (_group_retry[i].active && memcmp(_group_retry[i].hash, hash, MAX_HASH_SIZE) == 0) {
      use_idx = i;
      break;
    }
    if (!_group_retry[i].active && use_idx < 0) {
      use_idx = i;
    }
  }
  if (use_idx < 0) {
    // No free slot; replace oldest.
    use_idx = 0;
    for (int i = 1; i < (int)(sizeof(_group_retry) / sizeof(_group_retry[0])); i++) {
      if (_group_retry[i].created_at < _group_retry[use_idx].created_at) {
        use_idx = i;
      }
    }
  }

  auto& slot = _group_retry[use_idx];
  slot.raw_len = pkt->writeTo(slot.raw);
  if (slot.raw_len == 0) {
    slot.active = 0;
    return;
  }

  memcpy(slot.hash, hash, MAX_HASH_SIZE);
  slot.active = 1;
  slot.retries_sent = 0;
  slot.priority = (action >> 24) - 1;
  slot.created_at = millis();

  uint32_t base_delay = action & 0xFFFFFF;
  uint32_t est_airtime = _radio->getEstAirtimeFor(pkt->getRawLength());
  uint32_t wait_ms = est_airtime * (uint32_t)GROUP_FLOOD_RETRY_AIRTIME_FACTOR;
  if (wait_ms < (uint32_t)GROUP_FLOOD_RETRY_CONFIRM_WINDOW_MS) {
    wait_ms = (uint32_t)GROUP_FLOOD_RETRY_CONFIRM_WINDOW_MS;
  }
  slot.wait_ms = wait_ms;
  slot.next_retry_at = futureMillis(base_delay + wait_ms);

  _group_retry_tracked++;
#else
  (void)pkt;
  (void)action;
#endif
}

void MyMesh::markGroupFloodHeard(const mesh::Packet* pkt) {
#if ENABLE_GROUP_FLOOD_CONDITIONAL_RETRY == 1
  uint8_t hash[MAX_HASH_SIZE];
  pkt->calculatePacketHash(hash);

  for (int i = 0; i < (int)(sizeof(_group_retry) / sizeof(_group_retry[0])); i++) {
    auto& slot = _group_retry[i];
    if (!slot.active || memcmp(slot.hash, hash, MAX_HASH_SIZE) != 0) {
      continue;
    }

    bool heard_other_repeater = false;
    if (pkt->path_len >= PATH_HASH_SIZE) {
      uint8_t self_hash[PATH_HASH_SIZE];
      self_id.copyHashTo(self_hash);
      heard_other_repeater = memcmp(&pkt->path[pkt->path_len - PATH_HASH_SIZE], self_hash, PATH_HASH_SIZE) != 0;
    }

    if (heard_other_repeater) {
      slot.active = 0;
      _group_retry_confirmed++;
    }
    break;
  }
#else
  (void)pkt;
#endif
}

void MyMesh::processGroupFloodRetries() {
#if ENABLE_GROUP_FLOOD_CONDITIONAL_RETRY == 1
  for (int i = 0; i < (int)(sizeof(_group_retry) / sizeof(_group_retry[0])); i++) {
    auto& slot = _group_retry[i];
    if (!slot.active || !millisHasNowPassed(slot.next_retry_at)) {
      continue;
    }

    if (slot.retries_sent >= GROUP_FLOOD_RETRY_MAX_RETRANSMITS) {
      slot.active = 0;
      _group_retry_failed++;
      continue;
    }

    mesh::Packet* retry = obtainNewPacket();
    if (retry && readPacketWireExact(retry, slot.raw, slot.raw_len)) {
      sendPacket(retry, slot.priority, 0);
      slot.retries_sent++;
      _group_retry_retransmits++;
    } else if (retry) {
      releasePacket(retry);
    }

    uint32_t gap = GROUP_FLOOD_RETRY_GAP_MIN_MS;
    if (GROUP_FLOOD_RETRY_GAP_MAX_MS > GROUP_FLOOD_RETRY_GAP_MIN_MS) {
      gap = getRNG()->nextInt(GROUP_FLOOD_RETRY_GAP_MIN_MS, GROUP_FLOOD_RETRY_GAP_MAX_MS + 1);
    }
    slot.next_retry_at = futureMillis(slot.wait_ms + gap);
  }
#endif
}

static const char* skipCommandSpaces(const char* p) {
  while (*p == ' ') {
    p++;
  }
  return p;
}

static bool isHexChar(char c) {
  return (c >= '0' && c <= '9') ||
         (c >= 'a' && c <= 'f') ||
         (c >= 'A' && c <= 'F');
}

static size_t getCommandPrefixLen(const char* command) {
  const char* p = skipCommandSpaces(command);
  size_t n = 0;
  while (isHexChar(p[n])) {
    n++;
    if (n >= 8) {
      break;  // safety cap
    }
  }
  if (n >= 2 && p[n] == '|') {
    return n + 1;
  }
  return 0;
}

static bool isOtaCLICommand(const char* command) {
  if (command == NULL) {
    return false;
  }
  const char* p = skipCommandSpaces(command);
  size_t prefix_len = getCommandPrefixLen(p);
  if (prefix_len > 0) {  // optional "<token>|" prefix from mccli
    p += prefix_len;
  }
  p = skipCommandSpaces(p);

  if (memcmp(p, "start ota", 9) == 0 && (p[9] == 0 || p[9] == ' ')) {
    return true;
  }
  if (memcmp(p, "ota", 3) == 0 && (p[3] == 0 || p[3] == ' ')) {
    return true;
  }
  return false;
}

void MyMesh::writeOKFrame() {
  uint8_t frame[1] = { RESP_CODE_OK };
  if (_serial) {
    _serial->writeFrame(frame, 1);
  }
}

void MyMesh::writeErrFrame(uint8_t err_code) {
  uint8_t frame[2] = { RESP_CODE_ERR, err_code };
  if (_serial) {
    _serial->writeFrame(frame, 2);
  }
}

void MyMesh::writeContactFrame(uint8_t code, const uint8_t* pub_key, int8_t out_path_len, const uint8_t* out_path,
                               const char* name, uint32_t last_advert, uint32_t lastmod) {
  int i = 0;
  out_frame[i++] = code;
  memcpy(&out_frame[i], pub_key, PUB_KEY_SIZE);
  i += PUB_KEY_SIZE;
  out_frame[i++] = ADV_TYPE_REPEATER;
  out_frame[i++] = 0;  // flags
  out_frame[i++] = out_path_len;
  if (out_path) {
    memcpy(&out_frame[i], out_path, MAX_PATH_SIZE);
  } else {
    memset(&out_frame[i], 0, MAX_PATH_SIZE);
  }
  i += MAX_PATH_SIZE;
  memset(&out_frame[i], 0, 32);
  if (name) {
    StrHelper::strncpy((char *)&out_frame[i], name, 32);
  }
  i += 32;
  memcpy(&out_frame[i], &last_advert, 4);
  i += 4;
  int32_t lat = 0, lon = 0;
  memcpy(&out_frame[i], &lat, 4);
  i += 4;
  memcpy(&out_frame[i], &lon, 4);
  i += 4;
  memcpy(&out_frame[i], &lastmod, 4);
  i += 4;
  if (_serial) {
    _serial->writeFrame(out_frame, i);
  }
}

void MyMesh::startInterface(BaseSerialInterface& serial) {
  _serial = &serial;
  _serial->enable();
  clearPendingReqs();
}

void MyMesh::checkCompanionInterface() {
  if (_serial == NULL || !_serial->isEnabled()) {
    return;
  }
  size_t len = _serial->checkRecvFrame(cmd_frame);
  if (len > 0) {
    handleCompanionFrame(len);
  }
}

ClientInfo* MyMesh::getOrCreateClientByPubKey(const uint8_t* pub_key) {
  ClientInfo* c = acl.getClient(pub_key, PUB_KEY_SIZE);
  if (c == NULL) {
    mesh::Identity id(pub_key);
    c = acl.putClient(id, 0);
  }
  if (c) {
    self_id.calcSharedSecret(c->shared_secret, pub_key);
  }
  return c;
}

ClientInfo* MyMesh::getOrCreateClientByPrefix(const uint8_t* prefix6) {
  ClientInfo* c = acl.getClient(prefix6, 6);
  if (c) {
    return c;
  }

#if MAX_NEIGHBOURS
  const uint8_t* match = NULL;
  for (int i = 0; i < MAX_NEIGHBOURS; i++) {
    if (neighbours[i].heard_timestamp == 0) {
      continue;
    }
    if (memcmp(neighbours[i].id.pub_key, prefix6, 6) == 0) {
      if (match != NULL) {
        return NULL;  // ambiguous prefix
      }
      match = neighbours[i].id.pub_key;
    }
  }
  if (match != NULL) {
    return getOrCreateClientByPubKey(match);
  }
#endif
  return NULL;
}

int MyMesh::sendAnonReqTo(ClientInfo& recipient, const uint8_t* data, uint8_t len, uint32_t& tag, uint32_t& est_timeout) {
  if (len > MAX_PACKET_PAYLOAD - 4) {
    return MSG_SEND_FAILED;
  }

  uint8_t temp[MAX_PACKET_PAYLOAD];
  tag = getRTCClock()->getCurrentTimeUnique();
  memcpy(temp, &tag, 4);
  memcpy(&temp[4], data, len);

  auto pkt = createAnonDatagram(PAYLOAD_TYPE_ANON_REQ, self_id, recipient.id, recipient.shared_secret, temp, 4 + len);
  if (pkt == NULL) {
    return MSG_SEND_FAILED;
  }

  uint32_t airtime = _radio->getEstAirtimeFor(pkt->getRawLength());
  if (recipient.out_path_len < 0) {
    sendFlood(pkt);
    est_timeout = calcFloodTimeoutMillisFor(airtime);
    return MSG_SEND_SENT_FLOOD;
  }

  sendDirect(pkt, recipient.out_path, recipient.out_path_len);
  est_timeout = calcDirectTimeoutMillisFor(airtime, recipient.out_path_len);
  return MSG_SEND_SENT_DIRECT;
}

int MyMesh::sendRequestTo(ClientInfo& recipient, const uint8_t* data, uint8_t data_len, uint32_t& tag, uint32_t& est_timeout) {
  if (data_len > MAX_PACKET_PAYLOAD - 4) {
    return MSG_SEND_FAILED;
  }

  uint8_t temp[MAX_PACKET_PAYLOAD];
  tag = getRTCClock()->getCurrentTimeUnique();
  memcpy(temp, &tag, 4);
  memcpy(&temp[4], data, data_len);

  auto pkt = createDatagram(PAYLOAD_TYPE_REQ, recipient.id, recipient.shared_secret, temp, 4 + data_len);
  if (pkt == NULL) {
    return MSG_SEND_FAILED;
  }

  uint32_t airtime = _radio->getEstAirtimeFor(pkt->getRawLength());
  if (recipient.out_path_len < 0) {
    sendFlood(pkt);
    est_timeout = calcFloodTimeoutMillisFor(airtime);
    return MSG_SEND_SENT_FLOOD;
  }

  sendDirect(pkt, recipient.out_path, recipient.out_path_len);
  est_timeout = calcDirectTimeoutMillisFor(airtime, recipient.out_path_len);
  return MSG_SEND_SENT_DIRECT;
}

int MyMesh::sendRequestTo(ClientInfo& recipient, uint8_t req_type, uint32_t& tag, uint32_t& est_timeout) {
  uint8_t req_data[9];
  req_data[0] = req_type;
  memset(&req_data[1], 0, 4);  // reserved
  getRNG()->random(&req_data[5], 4);
  return sendRequestTo(recipient, req_data, sizeof(req_data), tag, est_timeout);
}

int MyMesh::sendCommandTo(ClientInfo& recipient, uint32_t timestamp, uint8_t attempt, const char* text, uint32_t& est_timeout) {
  size_t text_len = strlen(text);
  if (text_len > (MAX_PACKET_PAYLOAD - 5)) {
    return MSG_SEND_FAILED;
  }

  uint8_t temp[MAX_PACKET_PAYLOAD];
  memcpy(temp, &timestamp, 4);
  temp[4] = (attempt & 3) | (TXT_TYPE_CLI_DATA << 2);
  memcpy(&temp[5], text, text_len);

  auto pkt = createDatagram(PAYLOAD_TYPE_TXT_MSG, recipient.id, recipient.shared_secret, temp, 5 + text_len);
  if (pkt == NULL) {
    return MSG_SEND_FAILED;
  }

  uint32_t airtime = _radio->getEstAirtimeFor(pkt->getRawLength());
  if (recipient.out_path_len < 0) {
    sendFlood(pkt);
    est_timeout = calcFloodTimeoutMillisFor(airtime);
    return MSG_SEND_SENT_FLOOD;
  }

  sendDirect(pkt, recipient.out_path, recipient.out_path_len);
  est_timeout = calcDirectTimeoutMillisFor(airtime, recipient.out_path_len);
  return MSG_SEND_SENT_DIRECT;
}

void MyMesh::handleCompanionContactResponse(const ClientInfo& contact, const uint8_t* data, size_t len) {
  if (_serial == NULL || len < 4) {
    return;
  }

  uint32_t tag = 0;
  memcpy(&tag, data, 4);

  if (pending_login && memcmp(&pending_login, contact.id.pub_key, 4) == 0) {
    pending_login = 0;

    int i = 0;
    if (len > 4 && data[4] == RESP_SERVER_LOGIN_OK) {
      out_frame[i++] = PUSH_CODE_LOGIN_SUCCESS;
      out_frame[i++] = (len > 6) ? data[6] : 0;
      memcpy(&out_frame[i], contact.id.pub_key, 6);
      i += 6;
      memcpy(&out_frame[i], &tag, 4);
      i += 4;
      out_frame[i++] = (len > 7) ? data[7] : 0;
      out_frame[i++] = (len > 12) ? data[12] : 0;
    } else {
      out_frame[i++] = PUSH_CODE_LOGIN_FAIL;
      out_frame[i++] = 0;
      memcpy(&out_frame[i], contact.id.pub_key, 6);
      i += 6;
    }
    _serial->writeFrame(out_frame, i);
  } else if (len > 4 && pending_status && memcmp(&pending_status, contact.id.pub_key, 4) == 0) {
    pending_status = 0;
    int i = 0;
    out_frame[i++] = PUSH_CODE_STATUS_RESPONSE;
    out_frame[i++] = 0;
    memcpy(&out_frame[i], contact.id.pub_key, 6);
    i += 6;
    memcpy(&out_frame[i], &data[4], len - 4);
    i += (len - 4);
    _serial->writeFrame(out_frame, i);
  } else if (len > 4 && tag == pending_telemetry) {
    pending_telemetry = 0;
    int i = 0;
    out_frame[i++] = PUSH_CODE_TELEMETRY_RESPONSE;
    out_frame[i++] = 0;
    memcpy(&out_frame[i], contact.id.pub_key, 6);
    i += 6;
    memcpy(&out_frame[i], &data[4], len - 4);
    i += (len - 4);
    _serial->writeFrame(out_frame, i);
  } else if (len > 4 && tag == pending_req) {
    pending_req = 0;
    int i = 0;
    out_frame[i++] = PUSH_CODE_BINARY_RESPONSE;
    out_frame[i++] = 0;
    memcpy(&out_frame[i], &tag, 4);
    i += 4;
    memcpy(&out_frame[i], &data[4], len - 4);
    i += (len - 4);
    _serial->writeFrame(out_frame, i);
  }
}

bool MyMesh::emitCompanionTextMessage(const ClientInfo& from, mesh::Packet* pkt, const uint8_t* data, size_t len) {
  if (_serial == NULL || len <= 5) {
    return false;
  }

  uint32_t sender_timestamp;
  memcpy(&sender_timestamp, data, 4);
  uint8_t txt_type = (data[4] >> 2);
  if (!(txt_type == TXT_TYPE_PLAIN || txt_type == TXT_TYPE_CLI_DATA || txt_type == TXT_TYPE_SIGNED_PLAIN)) {
    return false;
  }

  const uint8_t* text = &data[5];
  size_t text_len = len - 5;
  int i = 0;
  out_frame[i++] = RESP_CODE_CONTACT_MSG_RECV_V3;
  out_frame[i++] = (int8_t)(pkt->getSNR() * 4);
  out_frame[i++] = 0;
  out_frame[i++] = 0;
  memcpy(&out_frame[i], from.id.pub_key, 6);
  i += 6;
  out_frame[i++] = pkt->isRouteFlood() ? pkt->path_len : 0xFF;
  out_frame[i++] = txt_type;
  memcpy(&out_frame[i], &sender_timestamp, 4);
  i += 4;

  if (txt_type == TXT_TYPE_SIGNED_PLAIN) {
    if (len <= 9) {
      return false;
    }
    memcpy(&out_frame[i], &data[5], 4);
    i += 4;
    text = &data[9];
    text_len = len - 9;
  }

  while (text_len > 0 && text[text_len - 1] == 0) {
    text_len--;
  }
  if (i + (int)text_len > MAX_FRAME_SIZE) {
    text_len = MAX_FRAME_SIZE - i;
  }
  memcpy(&out_frame[i], text, text_len);
  i += text_len;
  _serial->writeFrame(out_frame, i);
  return true;
}

void MyMesh::handleCompanionFrame(size_t len) {
  if (_serial == NULL || len == 0) {
    return;
  }

  if (cmd_frame[0] == CMD_DEVICE_QEURY && len >= 2) {
    int i = 0;
    out_frame[i++] = RESP_CODE_DEVICE_INFO;
    out_frame[i++] = COMPANION_FW_VER_CODE;
    out_frame[i++] = MAX_CLIENTS / 2;
    out_frame[i++] = 0;  // max_channels
    uint32_t pin = 0;
    memcpy(&out_frame[i], &pin, 4);
    i += 4;
    memset(&out_frame[i], 0, 12);
    StrHelper::strncpy((char *)&out_frame[i], FIRMWARE_BUILD_DATE, 12);
    i += 12;
    memset(&out_frame[i], 0, 40);
    StrHelper::strncpy((char *)&out_frame[i], board.getManufacturerName(), 40);
    i += 40;
    memset(&out_frame[i], 0, 20);
    StrHelper::strncpy((char *)&out_frame[i], FIRMWARE_VERSION, 20);
    i += 20;
    out_frame[i++] = _prefs.disable_fwd ? 0 : 1;
    _serial->writeFrame(out_frame, i);
  } else if (cmd_frame[0] == CMD_APP_START && len >= 8) {
    int i = 0;
    out_frame[i++] = RESP_CODE_SELF_INFO;
    out_frame[i++] = ADV_TYPE_REPEATER;
    out_frame[i++] = _prefs.tx_power_dbm;
    out_frame[i++] = MAX_LORA_TX_POWER;
    memcpy(&out_frame[i], self_id.pub_key, PUB_KEY_SIZE);
    i += PUB_KEY_SIZE;

    int32_t lat = (int32_t)(_prefs.node_lat * 1000000.0);
    int32_t lon = (int32_t)(_prefs.node_lon * 1000000.0);
    memcpy(&out_frame[i], &lat, 4);
    i += 4;
    memcpy(&out_frame[i], &lon, 4);
    i += 4;
    out_frame[i++] = _prefs.multi_acks;
    out_frame[i++] = _prefs.advert_loc_policy;
    out_frame[i++] = 0;  // telemetry mode (unsupported in repeater firmware)
    out_frame[i++] = 1;  // manual_add_contacts

    uint32_t freq = (uint32_t)(_prefs.freq * 1000.0f);
    uint32_t bw = (uint32_t)(_prefs.bw * 1000.0f);
    memcpy(&out_frame[i], &freq, 4);
    i += 4;
    memcpy(&out_frame[i], &bw, 4);
    i += 4;
    out_frame[i++] = _prefs.sf;
    out_frame[i++] = _prefs.cr;

    int nlen = strlen(_prefs.node_name);
    if (i + nlen > MAX_FRAME_SIZE) {
      nlen = MAX_FRAME_SIZE - i;
    }
    memcpy(&out_frame[i], _prefs.node_name, nlen);
    i += nlen;
    _serial->writeFrame(out_frame, i);
  } else if (cmd_frame[0] == CMD_GET_BATT_AND_STORAGE) {
    uint8_t reply[11];
    int i = 0;
    reply[i++] = RESP_CODE_BATT_AND_STORAGE;
    uint16_t battery_millivolts = board.getBattMilliVolts();
    uint32_t used = 0;
    uint32_t total = 0;
    memcpy(&reply[i], &battery_millivolts, 2);
    i += 2;
    memcpy(&reply[i], &used, 4);
    i += 4;
    memcpy(&reply[i], &total, 4);
    i += 4;
    _serial->writeFrame(reply, i);
  } else if (cmd_frame[0] == CMD_EXPORT_PRIVATE_KEY) {
#if ENABLE_PRIVATE_KEY_EXPORT
    uint8_t reply[65];
    reply[0] = RESP_CODE_PRIVATE_KEY;
    self_id.writeTo(&reply[1], 64);
    _serial->writeFrame(reply, 65);
#else
    uint8_t reply[1] = { RESP_CODE_DISABLED };
    _serial->writeFrame(reply, 1);
#endif
  } else if (cmd_frame[0] == CMD_SET_OTHER_PARAMS) {
    if (len >= 5) {
      _prefs.multi_acks = cmd_frame[4];
      savePrefs();
    }
    writeOKFrame();
  } else if (cmd_frame[0] == CMD_GET_CHANNEL && len >= 2) {
    writeErrFrame(ERR_CODE_NOT_FOUND);
  } else if (cmd_frame[0] == CMD_SYNC_NEXT_MESSAGE) {
    uint8_t reply[1] = { RESP_CODE_NO_MORE_MESSAGES };
    _serial->writeFrame(reply, 1);
  } else if (cmd_frame[0] == CMD_SET_ADVERT_NAME && len >= 2) {
    int nlen = len - 1;
    if (nlen > (int)sizeof(_prefs.node_name) - 1) {
      nlen = sizeof(_prefs.node_name) - 1;
    }
    memcpy(_prefs.node_name, &cmd_frame[1], nlen);
    _prefs.node_name[nlen] = 0;
    savePrefs();
    writeOKFrame();
  } else if (cmd_frame[0] == CMD_GET_CONTACTS) {
    uint32_t since = 0;
    if (len >= 5) {
      memcpy(&since, &cmd_frame[1], 4);
    }

    uint32_t count = 0;
    uint32_t most_recent_lastmod = 0;
    for (int i = 0; i < acl.getNumClients(); i++) {
      auto c = acl.getClientByIdx(i);
      uint32_t lastmod = c->last_activity;
      if (since == 0 || lastmod > since) {
        count++;
      }
      if (lastmod > most_recent_lastmod) {
        most_recent_lastmod = lastmod;
      }
    }
#if MAX_NEIGHBOURS
    for (int i = 0; i < MAX_NEIGHBOURS; i++) {
      if (neighbours[i].heard_timestamp == 0) {
        continue;
      }
      if (acl.getClient(neighbours[i].id.pub_key, PUB_KEY_SIZE) != NULL) {
        continue;
      }
      uint32_t lastmod = neighbours[i].heard_timestamp;
      if (since == 0 || lastmod > since) {
        count++;
      }
      if (lastmod > most_recent_lastmod) {
        most_recent_lastmod = lastmod;
      }
    }
#endif

    uint8_t start[5];
    start[0] = RESP_CODE_CONTACTS_START;
    memcpy(&start[1], &count, 4);
    _serial->writeFrame(start, 5);

    for (int i = 0; i < acl.getNumClients(); i++) {
      auto c = acl.getClientByIdx(i);
      uint32_t lastmod = c->last_activity;
      if (since != 0 && lastmod <= since) {
        continue;
      }
      char name[32];
      memset(name, 0, sizeof(name));
      char hex[13];
      mesh::Utils::toHex(hex, c->id.pub_key, 6);
      snprintf(name, sizeof(name), "rpt-%s", hex);
      writeContactFrame(RESP_CODE_CONTACT, c->id.pub_key, c->out_path_len, c->out_path, name, lastmod, lastmod);
    }

#if MAX_NEIGHBOURS
    for (int i = 0; i < MAX_NEIGHBOURS; i++) {
      if (neighbours[i].heard_timestamp == 0) {
        continue;
      }
      if (acl.getClient(neighbours[i].id.pub_key, PUB_KEY_SIZE) != NULL) {
        continue;
      }
      uint32_t lastmod = neighbours[i].heard_timestamp;
      if (since != 0 && lastmod <= since) {
        continue;
      }
      char name[32];
      memset(name, 0, sizeof(name));
      char hex[13];
      mesh::Utils::toHex(hex, neighbours[i].id.pub_key, 6);
      snprintf(name, sizeof(name), "rpt-%s", hex);
      writeContactFrame(RESP_CODE_CONTACT, neighbours[i].id.pub_key, -1, NULL, name, neighbours[i].advert_timestamp, lastmod);
    }
#endif

    uint8_t end[5];
    end[0] = RESP_CODE_END_OF_CONTACTS;
    memcpy(&end[1], &most_recent_lastmod, 4);
    _serial->writeFrame(end, 5);
  } else if (cmd_frame[0] == CMD_RESET_PATH && len >= 1 + PUB_KEY_SIZE) {
    auto c = acl.getClient(&cmd_frame[1], PUB_KEY_SIZE);
    if (c) {
      c->out_path_len = -1;
      writeOKFrame();
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND);
    }
  } else if (cmd_frame[0] == CMD_SEND_LOGIN && len >= 1 + PUB_KEY_SIZE) {
    auto c = getOrCreateClientByPubKey(&cmd_frame[1]);
    if (c == NULL) {
      writeErrFrame(ERR_CODE_TABLE_FULL);
      return;
    }

    cmd_frame[len] = 0;
    uint8_t* pwd = &cmd_frame[1 + PUB_KEY_SIZE];
    uint8_t pwd_len = strlen((char*)pwd);
    uint32_t tag, est_timeout;
    int rc = sendAnonReqTo(*c, pwd, pwd_len, tag, est_timeout);
    if (rc == MSG_SEND_FAILED) {
      writeErrFrame(ERR_CODE_TABLE_FULL);
    } else {
      clearPendingReqs();
      memcpy(&pending_login, c->id.pub_key, 4);
      out_frame[0] = RESP_CODE_SENT;
      out_frame[1] = (rc == MSG_SEND_SENT_FLOOD) ? 1 : 0;
      memcpy(&out_frame[2], &pending_login, 4);
      memcpy(&out_frame[6], &est_timeout, 4);
      _serial->writeFrame(out_frame, 10);
    }
  } else if (cmd_frame[0] == CMD_SEND_STATUS_REQ && len >= 1 + PUB_KEY_SIZE) {
    auto c = getOrCreateClientByPubKey(&cmd_frame[1]);
    if (c == NULL) {
      writeErrFrame(ERR_CODE_TABLE_FULL);
      return;
    }

    uint32_t tag, est_timeout;
    int rc = sendRequestTo(*c, REQ_TYPE_GET_STATUS, tag, est_timeout);
    if (rc == MSG_SEND_FAILED) {
      writeErrFrame(ERR_CODE_TABLE_FULL);
    } else {
      clearPendingReqs();
      memcpy(&pending_status, c->id.pub_key, 4);
      out_frame[0] = RESP_CODE_SENT;
      out_frame[1] = (rc == MSG_SEND_SENT_FLOOD) ? 1 : 0;
      memcpy(&out_frame[2], &tag, 4);
      memcpy(&out_frame[6], &est_timeout, 4);
      _serial->writeFrame(out_frame, 10);
    }
  } else if (cmd_frame[0] == CMD_SEND_TELEMETRY_REQ && len == 4) {
    telemetry.reset();
    telemetry.addVoltage(TELEM_CHANNEL_SELF, (float)board.getBattMilliVolts() / 1000.0f);
    sensors.querySensors(0xFF, telemetry);
    float temperature = board.getMCUTemperature();
    if (!isnan(temperature)) {
      telemetry.addTemperature(TELEM_CHANNEL_SELF, temperature);
    }

    int i = 0;
    out_frame[i++] = PUSH_CODE_TELEMETRY_RESPONSE;
    out_frame[i++] = 0;
    memcpy(&out_frame[i], self_id.pub_key, 6);
    i += 6;
    uint8_t tlen = telemetry.getSize();
    memcpy(&out_frame[i], telemetry.getBuffer(), tlen);
    i += tlen;
    _serial->writeFrame(out_frame, i);
  } else if (cmd_frame[0] == CMD_SEND_TELEMETRY_REQ && len >= 4 + PUB_KEY_SIZE) {
    auto c = getOrCreateClientByPubKey(&cmd_frame[4]);
    if (c == NULL) {
      writeErrFrame(ERR_CODE_TABLE_FULL);
      return;
    }

    uint32_t tag, est_timeout;
    int rc = sendRequestTo(*c, REQ_TYPE_GET_TELEMETRY_DATA, tag, est_timeout);
    if (rc == MSG_SEND_FAILED) {
      writeErrFrame(ERR_CODE_TABLE_FULL);
    } else {
      clearPendingReqs();
      pending_telemetry = tag;
      out_frame[0] = RESP_CODE_SENT;
      out_frame[1] = (rc == MSG_SEND_SENT_FLOOD) ? 1 : 0;
      memcpy(&out_frame[2], &tag, 4);
      memcpy(&out_frame[6], &est_timeout, 4);
      _serial->writeFrame(out_frame, 10);
    }
  } else if (cmd_frame[0] == CMD_SEND_BINARY_REQ && len > 1 + PUB_KEY_SIZE) {
    auto c = getOrCreateClientByPubKey(&cmd_frame[1]);
    if (c == NULL) {
      writeErrFrame(ERR_CODE_TABLE_FULL);
      return;
    }

    uint8_t* req = &cmd_frame[1 + PUB_KEY_SIZE];
    uint8_t req_len = len - (1 + PUB_KEY_SIZE);
    uint32_t tag, est_timeout;
    int rc = sendRequestTo(*c, req, req_len, tag, est_timeout);
    if (rc == MSG_SEND_FAILED) {
      writeErrFrame(ERR_CODE_TABLE_FULL);
    } else {
      clearPendingReqs();
      pending_req = tag;
      out_frame[0] = RESP_CODE_SENT;
      out_frame[1] = (rc == MSG_SEND_SENT_FLOOD) ? 1 : 0;
      memcpy(&out_frame[2], &tag, 4);
      memcpy(&out_frame[6], &est_timeout, 4);
      _serial->writeFrame(out_frame, 10);
    }
  } else if (cmd_frame[0] == CMD_SEND_TXT_MSG && len >= 14) {
    int i = 1;
    uint8_t txt_type = cmd_frame[i++];
    uint8_t attempt = cmd_frame[i++];
    uint32_t msg_timestamp = 0;
    memcpy(&msg_timestamp, &cmd_frame[i], 4);
    i += 4;
    auto c = getOrCreateClientByPrefix(&cmd_frame[i]);
    i += 6;
    if (c == NULL) {
      writeErrFrame(ERR_CODE_NOT_FOUND);
      return;
    }
    if (!(txt_type == TXT_TYPE_PLAIN || txt_type == TXT_TYPE_CLI_DATA)) {
      writeErrFrame(ERR_CODE_UNSUPPORTED_CMD);
      return;
    }

    cmd_frame[len] = 0;
    char* text = (char*)&cmd_frame[i];
    uint32_t est_timeout;
    int rc = sendCommandTo(*c, msg_timestamp, attempt, text, est_timeout);
    if (rc == MSG_SEND_FAILED) {
      writeErrFrame(ERR_CODE_TABLE_FULL);
    } else {
      out_frame[0] = RESP_CODE_SENT;
      out_frame[1] = (rc == MSG_SEND_SENT_FLOOD) ? 1 : 0;
      uint32_t expected_ack = 0;
      memcpy(&out_frame[2], &expected_ack, 4);
      memcpy(&out_frame[6], &est_timeout, 4);
      _serial->writeFrame(out_frame, 10);
    }
  } else {
    writeErrFrame(ERR_CODE_UNSUPPORTED_CMD);
  }
}

void MyMesh::putNeighbour(const mesh::Identity &id, uint32_t timestamp, float snr) {
#if MAX_NEIGHBOURS // check if neighbours enabled
  // find existing neighbour, else use least recently updated
  uint32_t oldest_timestamp = 0xFFFFFFFF;
  NeighbourInfo *neighbour = &neighbours[0];
  for (int i = 0; i < MAX_NEIGHBOURS; i++) {
    // if neighbour already known, we should update it
    if (id.matches(neighbours[i].id)) {
      neighbour = &neighbours[i];
      break;
    }

    // otherwise we should update the least recently updated neighbour
    if (neighbours[i].heard_timestamp < oldest_timestamp) {
      neighbour = &neighbours[i];
      oldest_timestamp = neighbour->heard_timestamp;
    }
  }

  // update neighbour info
  neighbour->id = id;
  neighbour->advert_timestamp = timestamp;
  neighbour->heard_timestamp = getRTCClock()->getCurrentTime();
  neighbour->snr = (int8_t)(snr * 4);
#endif
}

uint8_t MyMesh::handleLoginReq(const mesh::Identity& sender, const uint8_t* secret, uint32_t sender_timestamp, const uint8_t* data, bool is_flood) {
  ClientInfo* client = NULL;
  if (data[0] == 0) {   // blank password, just check if sender is in ACL
    client = acl.getClient(sender.pub_key, PUB_KEY_SIZE);
    if (client == NULL) {
    #if MESH_DEBUG
      MESH_DEBUG_PRINTLN("Login, sender not in ACL");
    #endif
    }
  }
  if (client == NULL) {
    uint8_t perms;
    if (strcmp((char *)data, _prefs.password) == 0) { // check for valid admin password
      perms = PERM_ACL_ADMIN;
    } else if (strcmp((char *)data, _prefs.guest_password) == 0) { // check guest password
      perms = PERM_ACL_GUEST;
    } else {
#if MESH_DEBUG
      MESH_DEBUG_PRINTLN("Invalid password: %s", data);
#endif
      return 0;
    }

    client = acl.putClient(sender, 0);  // add to contacts (if not already known)
    if (sender_timestamp <= client->last_timestamp) {
      MESH_DEBUG_PRINTLN("Possible login replay attack!");
      return 0;  // FATAL: client table is full -OR- replay attack
    }

    MESH_DEBUG_PRINTLN("Login success!");
    client->last_timestamp = sender_timestamp;
    client->last_activity = getRTCClock()->getCurrentTime();
    client->permissions &= ~0x03;
    client->permissions |= perms;
    memcpy(client->shared_secret, secret, PUB_KEY_SIZE);

    if (perms != PERM_ACL_GUEST) {   // keep number of FS writes to a minimum
      dirty_contacts_expiry = futureMillis(LAZY_CONTACTS_WRITE_DELAY);
    }
  }

  if (is_flood) {
    client->out_path_len = -1;  // need to rediscover out_path
  }

  uint32_t now = getRTCClock()->getCurrentTimeUnique();
  memcpy(reply_data, &now, 4);   // response packets always prefixed with timestamp
  reply_data[4] = RESP_SERVER_LOGIN_OK;
  reply_data[5] = 0;  // Legacy: was recommended keep-alive interval (secs / 16)
  reply_data[6] = client->isAdmin() ? 1 : 0;
  reply_data[7] = client->permissions;
  getRNG()->random(&reply_data[8], 4);   // random blob to help packet-hash uniqueness
  reply_data[12] = FIRMWARE_VER_LEVEL;  // New field

  return 13;  // reply length
}

uint8_t MyMesh::handleAnonRegionsReq(const mesh::Identity& sender, uint32_t sender_timestamp, const uint8_t* data) {
  if (anon_limiter.allow(rtc_clock.getCurrentTime())) {
    // request data has: {reply-path-len}{reply-path}
    reply_path_len = *data++ & 0x3F;
    memcpy(reply_path, data, reply_path_len);
    // data += reply_path_len;

    memcpy(reply_data, &sender_timestamp, 4);   // prefix with sender_timestamp, like a tag
    uint32_t now = getRTCClock()->getCurrentTime();
    memcpy(&reply_data[4], &now, 4);     // include our clock (for easy clock sync, and packet hash uniqueness)

    return 8 + region_map.exportNamesTo((char *) &reply_data[8], sizeof(reply_data) - 12, REGION_DENY_FLOOD);   // reply length
  }
  return 0;
}

uint8_t MyMesh::handleAnonOwnerReq(const mesh::Identity& sender, uint32_t sender_timestamp, const uint8_t* data) {
  if (anon_limiter.allow(rtc_clock.getCurrentTime())) {
    // request data has: {reply-path-len}{reply-path}
    reply_path_len = *data++ & 0x3F;
    memcpy(reply_path, data, reply_path_len);
    // data += reply_path_len;

    memcpy(reply_data, &sender_timestamp, 4);   // prefix with sender_timestamp, like a tag
    uint32_t now = getRTCClock()->getCurrentTime();
    memcpy(&reply_data[4], &now, 4);     // include our clock (for easy clock sync, and packet hash uniqueness)
    sprintf((char *) &reply_data[8], "%s\n%s", _prefs.node_name, _prefs.owner_info);

    return 8 + strlen((char *) &reply_data[8]);   // reply length
  }
  return 0;
}

uint8_t MyMesh::handleAnonClockReq(const mesh::Identity& sender, uint32_t sender_timestamp, const uint8_t* data) {
  if (anon_limiter.allow(rtc_clock.getCurrentTime())) {
    // request data has: {reply-path-len}{reply-path}
    reply_path_len = *data++ & 0x3F;
    memcpy(reply_path, data, reply_path_len);
    // data += reply_path_len;

    memcpy(reply_data, &sender_timestamp, 4);   // prefix with sender_timestamp, like a tag
    uint32_t now = getRTCClock()->getCurrentTime();
    memcpy(&reply_data[4], &now, 4);     // include our clock (for easy clock sync, and packet hash uniqueness)
    reply_data[8] = 0;  // features
#ifdef WITH_RS232_BRIDGE
    reply_data[8] |= 0x01;  // is bridge, type UART
#elif WITH_ESPNOW_BRIDGE
    reply_data[8] |= 0x03;  // is bridge, type ESP-NOW
#endif
    if (_prefs.disable_fwd) {   // is this repeater currently disabled
      reply_data[8] |= 0x80;  // is disabled
    }
    // TODO:  add some kind of moving-window utilisation metric, so can query 'how busy' is this repeater
    return 9;   // reply length
  }
  return 0;
}

int MyMesh::handleRequest(ClientInfo *sender, uint32_t sender_timestamp, uint8_t *payload, size_t payload_len) {
  // uint32_t now = getRTCClock()->getCurrentTimeUnique();
  // memcpy(reply_data, &now, 4);   // response packets always prefixed with timestamp
  memcpy(reply_data, &sender_timestamp, 4); // reflect sender_timestamp back in response packet (kind of like a 'tag')

  if (payload[0] == REQ_TYPE_GET_STATUS) {  // guests can also access this now
    RepeaterStats stats;
    stats.batt_milli_volts = board.getBattMilliVolts();
    stats.curr_tx_queue_len = _mgr->getOutboundCount(0xFFFFFFFF);
    stats.noise_floor = (int16_t)_radio->getNoiseFloor();
    stats.last_rssi = (int16_t)radio_driver.getLastRSSI();
    stats.n_packets_recv = radio_driver.getPacketsRecv();
    stats.n_packets_sent = radio_driver.getPacketsSent();
    stats.total_air_time_secs = getTotalAirTime() / 1000;
    stats.total_up_time_secs = uptime_millis / 1000;
    stats.n_sent_flood = getNumSentFlood();
    stats.n_sent_direct = getNumSentDirect();
    stats.n_recv_flood = getNumRecvFlood();
    stats.n_recv_direct = getNumRecvDirect();
    stats.err_events = _err_flags;
    stats.last_snr = (int16_t)(radio_driver.getLastSNR() * 4);
    stats.n_direct_dups = ((SimpleMeshTables *)getTables())->getNumDirectDups();
    stats.n_flood_dups = ((SimpleMeshTables *)getTables())->getNumFloodDups();
    stats.total_rx_air_time_secs = getReceiveAirTime() / 1000;
    stats.n_recv_errors = radio_driver.getPacketsRecvErrors();
    memcpy(&reply_data[4], &stats, sizeof(stats));

    return 4 + sizeof(stats); //  reply_len
  }
  if (payload[0] == REQ_TYPE_GET_TELEMETRY_DATA) {
    uint8_t perm_mask = ~(payload[1]); // NEW: first reserved byte (of 4), is now inverse mask to apply to permissions

    telemetry.reset();
    telemetry.addVoltage(TELEM_CHANNEL_SELF, (float)board.getBattMilliVolts() / 1000.0f);

    // query other sensors -- target specific
    if ((sender->permissions & PERM_ACL_ROLE_MASK) == PERM_ACL_GUEST) {
      perm_mask = 0x00;  // just base telemetry allowed
    }
    sensors.querySensors(perm_mask, telemetry);

	// This default temperature will be overridden by external sensors (if any)
    float temperature = board.getMCUTemperature();
    if(!isnan(temperature)) { // Supported boards with built-in temperature sensor. ESP32-C3 may return NAN
      telemetry.addTemperature(TELEM_CHANNEL_SELF, temperature); // Built-in MCU Temperature
    }

    uint8_t tlen = telemetry.getSize();
    memcpy(&reply_data[4], telemetry.getBuffer(), tlen);
    return 4 + tlen; // reply_len
  }
  if (payload[0] == REQ_TYPE_GET_ACCESS_LIST && sender->isAdmin()) {
    uint8_t res1 = payload[1];   // reserved for future  (extra query params)
    uint8_t res2 = payload[2];
    if (res1 == 0 && res2 == 0) {
      uint8_t ofs = 4;
      for (int i = 0; i < acl.getNumClients() && ofs + 7 <= sizeof(reply_data) - 4; i++) {
        auto c = acl.getClientByIdx(i);
        if (c->permissions == 0) continue;  // skip deleted entries
        memcpy(&reply_data[ofs], c->id.pub_key, 6); ofs += 6;  // just 6-byte pub_key prefix
        reply_data[ofs++] = c->permissions;
      }
      return ofs;
    }
  }
  if (payload[0] == REQ_TYPE_GET_NEIGHBOURS) {
    uint8_t request_version = payload[1];
    if (request_version == 0) {

      // reply data offset (after response sender_timestamp/tag)
      int reply_offset = 4;

      // get request params
      uint8_t count = payload[2]; // how many neighbours to fetch (0-255)
      uint16_t offset;
      memcpy(&offset, &payload[3], 2); // offset from start of neighbours list (0-65535)
      uint8_t order_by = payload[5]; // how to order neighbours. 0=newest_to_oldest, 1=oldest_to_newest, 2=strongest_to_weakest, 3=weakest_to_strongest
      uint8_t pubkey_prefix_length = payload[6]; // how many bytes of neighbour pub key we want
      // we also send a 4 byte random blob in payload[7...10] to help packet uniqueness

      MESH_DEBUG_PRINTLN("REQ_TYPE_GET_NEIGHBOURS count=%d, offset=%d, order_by=%d, pubkey_prefix_length=%d", count, offset, order_by, pubkey_prefix_length);

      // clamp pub key prefix length to max pub key length
      if(pubkey_prefix_length > PUB_KEY_SIZE){
        pubkey_prefix_length = PUB_KEY_SIZE;
        MESH_DEBUG_PRINTLN("REQ_TYPE_GET_NEIGHBOURS invalid pubkey_prefix_length=%d clamping to %d", pubkey_prefix_length, PUB_KEY_SIZE);
      }

      // create copy of neighbours list, skipping empty entries so we can sort it separately from main list
      int16_t neighbours_count = 0;
#if MAX_NEIGHBOURS
      NeighbourInfo* sorted_neighbours[MAX_NEIGHBOURS];
      for (int i = 0; i < MAX_NEIGHBOURS; i++) {
        auto neighbour = &neighbours[i];
        if (neighbour->heard_timestamp > 0) {
          sorted_neighbours[neighbours_count] = neighbour;
          neighbours_count++;
        }
      }

      // sort neighbours based on order
      if (order_by == 0) {
        // sort by newest to oldest
        MESH_DEBUG_PRINTLN("REQ_TYPE_GET_NEIGHBOURS sorting newest to oldest");
        std::sort(sorted_neighbours, sorted_neighbours + neighbours_count, [](const NeighbourInfo* a, const NeighbourInfo* b) {
          return a->heard_timestamp > b->heard_timestamp; // desc
        });
      } else if (order_by == 1) {
        // sort by oldest to newest
        MESH_DEBUG_PRINTLN("REQ_TYPE_GET_NEIGHBOURS sorting oldest to newest");
        std::sort(sorted_neighbours, sorted_neighbours + neighbours_count, [](const NeighbourInfo* a, const NeighbourInfo* b) {
          return a->heard_timestamp < b->heard_timestamp; // asc
        });
      } else if (order_by == 2) {
        // sort by strongest to weakest
        MESH_DEBUG_PRINTLN("REQ_TYPE_GET_NEIGHBOURS sorting strongest to weakest");
        std::sort(sorted_neighbours, sorted_neighbours + neighbours_count, [](const NeighbourInfo* a, const NeighbourInfo* b) {
          return a->snr > b->snr; // desc
        });
      } else if (order_by == 3) {
        // sort by weakest to strongest
        MESH_DEBUG_PRINTLN("REQ_TYPE_GET_NEIGHBOURS sorting weakest to strongest");
        std::sort(sorted_neighbours, sorted_neighbours + neighbours_count, [](const NeighbourInfo* a, const NeighbourInfo* b) {
          return a->snr < b->snr; // asc
        });
      }
#endif

      // build results buffer
      int results_count = 0;
      int results_offset = 0;
      uint8_t results_buffer[130];
      for(int index = 0; index < count && index + offset < neighbours_count; index++){
        
        // stop if we can't fit another entry in results
        int entry_size = pubkey_prefix_length + 4 + 1;
        if(results_offset + entry_size > sizeof(results_buffer)){
          MESH_DEBUG_PRINTLN("REQ_TYPE_GET_NEIGHBOURS no more entries can fit in results buffer");
          break;
        }

#if MAX_NEIGHBOURS
        // add next neighbour to results
        auto neighbour = sorted_neighbours[index + offset];
        uint32_t heard_seconds_ago = getRTCClock()->getCurrentTime() - neighbour->heard_timestamp;
        memcpy(&results_buffer[results_offset], neighbour->id.pub_key, pubkey_prefix_length); results_offset += pubkey_prefix_length;
        memcpy(&results_buffer[results_offset], &heard_seconds_ago, 4); results_offset += 4;
        memcpy(&results_buffer[results_offset], &neighbour->snr, 1); results_offset += 1;
        results_count++;
#endif

      }

      // build reply
      MESH_DEBUG_PRINTLN("REQ_TYPE_GET_NEIGHBOURS neighbours_count=%d results_count=%d", neighbours_count, results_count);
      memcpy(&reply_data[reply_offset], &neighbours_count, 2); reply_offset += 2;
      memcpy(&reply_data[reply_offset], &results_count, 2); reply_offset += 2;
      memcpy(&reply_data[reply_offset], &results_buffer, results_offset); reply_offset += results_offset;

      return reply_offset;
    }
  } else if (payload[0] == REQ_TYPE_GET_OWNER_INFO) {
    sprintf((char *) &reply_data[4], "%s\n%s\n%s", FIRMWARE_VERSION, _prefs.node_name, _prefs.owner_info);
    return 4 + strlen((char *) &reply_data[4]);
  } else if (payload[0] == REQ_TYPE_OTA_BINARY) {
#if defined(RP2040_PLATFORM)
    if (sender == NULL || !sender->isAdmin()) {
      strcpy((char*)&reply_data[4], "Err - admin required");
      return 4 + strlen((char*)&reply_data[4]);
    }
    if (payload_len < 2) {
      strcpy((char*)&reply_data[4], "Err - bad OTA req");
      return 4 + strlen((char*)&reply_data[4]);
    }

    uint8_t opcode = payload[1];
    const uint8_t *req_payload = payload_len > 2 ? &payload[2] : NULL;
    size_t req_len = payload_len > 2 ? payload_len - 2 : 0;

    char ota_reply[160];
    ota_reply[0] = 0;
    if (!board.handleOTABinaryCommand(opcode, req_payload, req_len, ota_reply)) {
      strcpy(ota_reply, "Err - OTA unsupported");
    }

    size_t ota_reply_len = strlen(ota_reply);
    if (ota_reply_len == 0) {
      return 0; // no reply for intermediate write chunks
    }
    if (ota_reply_len > MAX_PACKET_PAYLOAD - 4) {
      ota_reply_len = MAX_PACKET_PAYLOAD - 4;
    }
    memcpy(&reply_data[4], ota_reply, ota_reply_len);
    return 4 + ota_reply_len;
#endif
  }
  return 0; // unknown command
}

mesh::Packet *MyMesh::createSelfAdvert() {
  uint8_t app_data[MAX_ADVERT_DATA_SIZE];
  uint8_t app_data_len = _cli.buildAdvertData(ADV_TYPE_REPEATER, app_data);

  return createAdvert(self_id, app_data, app_data_len);
}

File MyMesh::openAppend(const char *fname) {
#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
  return _fs->open(fname, FILE_O_WRITE);
#elif defined(RP2040_PLATFORM)
  return _fs->open(fname, "a");
#else
  return _fs->open(fname, "a", true);
#endif
}

bool MyMesh::allowPacketForward(const mesh::Packet *packet) {
  if (_prefs.disable_fwd) return false;
  if (packet->isRouteFlood() && packet->path_len >= _prefs.flood_max) return false;
  if (packet->isRouteFlood() && recv_pkt_region == NULL) {
    MESH_DEBUG_PRINTLN("allowPacketForward: unknown transport code, or wildcard not allowed for FLOOD packet");
    return false;
  }
  return true;
}

const char *MyMesh::getLogDateTime() {
  static char tmp[32];
  uint32_t now = getRTCClock()->getCurrentTime();
  DateTime dt = DateTime(now);
  sprintf(tmp, "%02d:%02d:%02d - %d/%d/%d U", dt.hour(), dt.minute(), dt.second(), dt.day(), dt.month(),
          dt.year());
  return tmp;
}

void MyMesh::logRxRaw(float snr, float rssi, const uint8_t raw[], int len) {
#if MESH_PACKET_LOGGING
  Serial.print(getLogDateTime());
  Serial.print(" RAW: ");
  mesh::Utils::printHex(Serial, raw, len);
  Serial.println();
#endif
}

void MyMesh::logRx(mesh::Packet *pkt, int len, float score) {
#ifdef WITH_BRIDGE
  if (_prefs.bridge_pkt_src == 1) {
    bridge.sendPacket(pkt);
  }
#endif

  if (_logging) {
    File f = openAppend(PACKET_LOG_FILE);
    if (f) {
      f.print(getLogDateTime());
      f.printf(": RX, len=%d (type=%d, route=%s, payload_len=%d) SNR=%d RSSI=%d score=%d", len,
               pkt->getPayloadType(), pkt->isRouteDirect() ? "D" : "F", pkt->payload_len,
               (int)_radio->getLastSNR(), (int)_radio->getLastRSSI(), (int)(score * 1000));

      if (pkt->getPayloadType() == PAYLOAD_TYPE_PATH || pkt->getPayloadType() == PAYLOAD_TYPE_REQ ||
          pkt->getPayloadType() == PAYLOAD_TYPE_RESPONSE || pkt->getPayloadType() == PAYLOAD_TYPE_TXT_MSG) {
        f.printf(" [%02X -> %02X]\n", (uint32_t)pkt->payload[1], (uint32_t)pkt->payload[0]);
      } else {
        f.printf("\n");
      }
      f.close();
    }
  }
}

void MyMesh::logTx(mesh::Packet *pkt, int len) {
#ifdef WITH_BRIDGE
  if (_prefs.bridge_pkt_src == 0) {
    bridge.sendPacket(pkt);
  }
#endif

  if (_logging) {
    File f = openAppend(PACKET_LOG_FILE);
    if (f) {
      f.print(getLogDateTime());
      f.printf(": TX, len=%d (type=%d, route=%s, payload_len=%d)", len, pkt->getPayloadType(),
               pkt->isRouteDirect() ? "D" : "F", pkt->payload_len);

      if (pkt->getPayloadType() == PAYLOAD_TYPE_PATH || pkt->getPayloadType() == PAYLOAD_TYPE_REQ ||
          pkt->getPayloadType() == PAYLOAD_TYPE_RESPONSE || pkt->getPayloadType() == PAYLOAD_TYPE_TXT_MSG) {
        f.printf(" [%02X -> %02X]\n", (uint32_t)pkt->payload[1], (uint32_t)pkt->payload[0]);
      } else {
        f.printf("\n");
      }
      f.close();
    }
  }
}

void MyMesh::logTxFail(mesh::Packet *pkt, int len) {
  if (_logging) {
    File f = openAppend(PACKET_LOG_FILE);
    if (f) {
      f.print(getLogDateTime());
      f.printf(": TX FAIL!, len=%d (type=%d, route=%s, payload_len=%d)\n", len, pkt->getPayloadType(),
               pkt->isRouteDirect() ? "D" : "F", pkt->payload_len);
      f.close();
    }
  }
}

int MyMesh::calcRxDelay(float score, uint32_t air_time) const {
  if (_prefs.rx_delay_base <= 0.0f) return 0;
  return (int)((pow(_prefs.rx_delay_base, 0.85f - score) - 1.0) * air_time);
}

uint32_t MyMesh::getRetransmitDelay(const mesh::Packet *packet) {
  uint32_t t = (_radio->getEstAirtimeFor(packet->path_len + packet->payload_len + 2) * _prefs.tx_delay_factor);
  return getRNG()->nextInt(0, 5*t + 1);
}
uint32_t MyMesh::getDirectRetransmitDelay(const mesh::Packet *packet) {
  uint32_t t = (_radio->getEstAirtimeFor(packet->path_len + packet->payload_len + 2) * _prefs.direct_tx_delay_factor);
  return getRNG()->nextInt(0, 5*t + 1);
}

bool MyMesh::filterRecvFloodPacket(mesh::Packet* pkt) {
  // just try to determine region for packet (apply later in allowPacketForward())
  if (pkt->getRouteType() == ROUTE_TYPE_TRANSPORT_FLOOD) {
    recv_pkt_region = region_map.findMatch(pkt, REGION_DENY_FLOOD);
  } else if (pkt->getRouteType() == ROUTE_TYPE_FLOOD) {
    if (region_map.getWildcard().flags & REGION_DENY_FLOOD) {
      recv_pkt_region = NULL;
    } else {
      recv_pkt_region =  &region_map.getWildcard();
    }
  } else {
    recv_pkt_region = NULL;
  }
  // do normal processing
  return false;
}

void MyMesh::onAnonDataRecv(mesh::Packet *packet, const uint8_t *secret, const mesh::Identity &sender,
                            uint8_t *data, size_t len) {
  if (packet->getPayloadType() == PAYLOAD_TYPE_ANON_REQ) { // received an initial request by a possible admin
                                                           // client (unknown at this stage)
    uint32_t timestamp;
    memcpy(&timestamp, data, 4);

    data[len] = 0;  // ensure null terminator
    uint8_t reply_len;

    reply_path_len = -1;
    if (data[4] == 0 || data[4] >= ' ') {   // is password, ie. a login request
      reply_len = handleLoginReq(sender, secret, timestamp, &data[4], packet->isRouteFlood());
    } else if (data[4] == ANON_REQ_TYPE_REGIONS && packet->isRouteDirect()) {
      reply_len = handleAnonRegionsReq(sender, timestamp, &data[5]);
    } else if (data[4] == ANON_REQ_TYPE_OWNER && packet->isRouteDirect()) {
      reply_len = handleAnonOwnerReq(sender, timestamp, &data[5]);
    } else if (data[4] == ANON_REQ_TYPE_BASIC && packet->isRouteDirect()) {
      reply_len = handleAnonClockReq(sender, timestamp, &data[5]);
    } else {
      reply_len = 0;  // unknown/invalid request type
    }

    if (reply_len == 0) return;   // invalid request

    if (packet->isRouteFlood()) {
      // let this sender know path TO here, so they can use sendDirect(), and ALSO encode the response
      mesh::Packet* path = createPathReturn(sender, secret, packet->path, packet->path_len,
                                            PAYLOAD_TYPE_RESPONSE, reply_data, reply_len);
      if (path) sendFlood(path, SERVER_RESPONSE_DELAY);
    } else if (reply_path_len < 0) {
      mesh::Packet* reply = createDatagram(PAYLOAD_TYPE_RESPONSE, sender, secret, reply_data, reply_len);
      if (reply) sendFlood(reply, SERVER_RESPONSE_DELAY);
    } else {
      mesh::Packet* reply = createDatagram(PAYLOAD_TYPE_RESPONSE, sender, secret, reply_data, reply_len);
      if (reply) sendDirect(reply, reply_path, reply_path_len, SERVER_RESPONSE_DELAY);
    }
  }
}

int MyMesh::searchPeersByHash(const uint8_t *hash) {
  int n = 0;
  for (int i = 0; i < acl.getNumClients(); i++) {
    if (acl.getClientByIdx(i)->id.isHashMatch(hash)) {
      matching_peer_indexes[n++] = i; // store the INDEXES of matching contacts (for subsequent 'peer' methods)
    }
  }
  return n;
}

void MyMesh::getPeerSharedSecret(uint8_t *dest_secret, int peer_idx) {
  int i = matching_peer_indexes[peer_idx];
  if (i >= 0 && i < acl.getNumClients()) {
    // lookup pre-calculated shared_secret
    memcpy(dest_secret, acl.getClientByIdx(i)->shared_secret, PUB_KEY_SIZE);
  } else {
    MESH_DEBUG_PRINTLN("getPeerSharedSecret: Invalid peer idx: %d", i);
  }
}

static bool isShare(const mesh::Packet *packet) {
  if (packet->hasTransportCodes()) {
    return packet->transport_codes[0] == 0 && packet->transport_codes[1] == 0;  // codes { 0, 0 } means 'send to nowhere'
  }
  return false;
}

void MyMesh::onAdvertRecv(mesh::Packet *packet, const mesh::Identity &id, uint32_t timestamp,
                          const uint8_t *app_data, size_t app_data_len) {
  mesh::Mesh::onAdvertRecv(packet, id, timestamp, app_data, app_data_len); // chain to super impl

  if (_serial && _serial->isConnected()) {
    out_frame[0] = PUSH_CODE_ADVERT;
    memcpy(&out_frame[1], id.pub_key, PUB_KEY_SIZE);
    _serial->writeFrame(out_frame, 1 + PUB_KEY_SIZE);
  }

  // if this a zero hop advert (and not via 'Share'), add it to neighbours
  if (packet->path_len == 0 && !isShare(packet)) {
    AdvertDataParser parser(app_data, app_data_len);
    if (parser.isValid() && parser.getType() == ADV_TYPE_REPEATER) { // just keep neigbouring Repeaters
      putNeighbour(id, timestamp, packet->getSNR());
    }
  }
}

void MyMesh::onPeerDataRecv(mesh::Packet *packet, uint8_t type, int sender_idx, const uint8_t *secret,
                            uint8_t *data, size_t len) {
  int i = matching_peer_indexes[sender_idx];
  if (i < 0 || i >= acl.getNumClients()) { // get from our known_clients table (sender SHOULD already be known in this context)
    MESH_DEBUG_PRINTLN("onPeerDataRecv: invalid peer idx: %d", i);
    return;
  }
  ClientInfo* client = acl.getClientByIdx(i);

  if (type == PAYLOAD_TYPE_REQ) { // request (from a Known admin client!)
    uint32_t timestamp;
    memcpy(&timestamp, data, 4);

    if (timestamp > client->last_timestamp) { // prevent replay attacks
      int reply_len = handleRequest(client, timestamp, &data[4], len - 4);
      if (reply_len == 0) return; // invalid command

      client->last_timestamp = timestamp;
      client->last_activity = getRTCClock()->getCurrentTime();

      if (packet->isRouteFlood()) {
        // let this sender know path TO here, so they can use sendDirect(), and ALSO encode the response
        mesh::Packet *path = createPathReturn(client->id, secret, packet->path, packet->path_len,
                                              PAYLOAD_TYPE_RESPONSE, reply_data, reply_len);
        if (path) sendFlood(path, SERVER_RESPONSE_DELAY);
      } else {
        mesh::Packet *reply =
            createDatagram(PAYLOAD_TYPE_RESPONSE, client->id, secret, reply_data, reply_len);
        if (reply) {
          if (client->out_path_len >= 0) { // we have an out_path, so send DIRECT
            sendDirect(reply, client->out_path, client->out_path_len, SERVER_RESPONSE_DELAY);
          } else {
            sendFlood(reply, SERVER_RESPONSE_DELAY);
          }
        }
      }
    } else {
      MESH_DEBUG_PRINTLN("onPeerDataRecv: possible replay attack detected");
    }
  } else if (type == PAYLOAD_TYPE_TXT_MSG && len > 5 && client->isAdmin()) { // a CLI command
    uint32_t sender_timestamp;
    memcpy(&sender_timestamp, data, 4); // timestamp (by sender's RTC clock - which could be wrong)
    uint8_t flags = (data[4] >> 2);        // message attempt number, and other flags

    if (!(flags == TXT_TYPE_PLAIN || flags == TXT_TYPE_CLI_DATA)) {
      MESH_DEBUG_PRINTLN("onPeerDataRecv: unsupported text type received: flags=%02x", (uint32_t)flags);
    } else if (sender_timestamp >= client->last_timestamp) { // prevent replay attacks
      bool is_retry = (sender_timestamp == client->last_timestamp);
      client->last_timestamp = sender_timestamp;
      client->last_activity = getRTCClock()->getCurrentTime();

      // len can be > original length, but 'text' will be padded with zeroes
      data[len] = 0; // need to make a C string again, with null terminator
      char *command = (char *)&data[5];
      bool ota_command = isOtaCLICommand(command);

      if (flags == TXT_TYPE_PLAIN) { // for legacy CLI, send Acks
        uint32_t ack_hash; // calc truncated hash of the message timestamp + text + sender pub_key, to prove
                           // to sender that we got it
        mesh::Utils::sha256((uint8_t *)&ack_hash, 4, data, 5 + strlen((char *)&data[5]), client->id.pub_key,
                            PUB_KEY_SIZE);

        mesh::Packet *ack = createAck(ack_hash);
        if (ack) {
          uint32_t ack_delay = ota_command ? OTA_TXT_ACK_DELAY : TXT_ACK_DELAY;
          if (client->out_path_len < 0) {
            sendFlood(ack, ack_delay);
          } else {
            sendDirect(ack, client->out_path, client->out_path_len, ack_delay);
          }
        }
      }

      uint8_t temp[166];
      char *reply = (char *)&temp[5];
      if (is_retry) {
        *reply = 0;
      } else {
        handleCommand(sender_timestamp, command, reply);
      }
      int text_len = strlen(reply);
      if (text_len > 0) {
        uint32_t timestamp = getRTCClock()->getCurrentTimeUnique();
        if (timestamp == sender_timestamp) {
          // WORKAROUND: the two timestamps need to be different, in the CLI view
          timestamp++;
        }
        memcpy(temp, &timestamp, 4);        // mostly an extra blob to help make packet_hash unique
        temp[4] = (TXT_TYPE_CLI_DATA << 2); // NOTE: legacy was: TXT_TYPE_PLAIN

        auto reply = createDatagram(PAYLOAD_TYPE_TXT_MSG, client->id, secret, temp, 5 + text_len);
        if (reply) {
          uint32_t reply_delay = ota_command ? OTA_CLI_REPLY_DELAY_MILLIS : CLI_REPLY_DELAY_MILLIS;
          if (client->out_path_len < 0) {
            sendFlood(reply, reply_delay);
          } else {
            sendDirect(reply, client->out_path, client->out_path_len, reply_delay);
          }
        }
      }
    } else {
      MESH_DEBUG_PRINTLN("onPeerDataRecv: possible replay attack detected");
    }
  } else if (type == PAYLOAD_TYPE_RESPONSE && len >= 4) {
    client->last_activity = getRTCClock()->getCurrentTime();
    handleCompanionContactResponse(*client, data, len);
  } else if (type == PAYLOAD_TYPE_TXT_MSG && len > 5) {
    client->last_activity = getRTCClock()->getCurrentTime();
    emitCompanionTextMessage(*client, packet, data, len);
  }
}

bool MyMesh::onPeerPathRecv(mesh::Packet *packet, int sender_idx, const uint8_t *secret, uint8_t *path,
                            uint8_t path_len, uint8_t extra_type, uint8_t *extra, uint8_t extra_len) {
  // TODO: prevent replay attacks
  int i = matching_peer_indexes[sender_idx];

  if (i >= 0 && i < acl.getNumClients()) { // get from our known_clients table (sender SHOULD already be known in this context)
    MESH_DEBUG_PRINTLN("PATH to client, path_len=%d", (uint32_t)path_len);
    auto client = acl.getClientByIdx(i);

    memcpy(client->out_path, path, client->out_path_len = path_len); // store a copy of path, for sendDirect()
    client->last_activity = getRTCClock()->getCurrentTime();

    if (_serial && _serial->isConnected()) {
      out_frame[0] = PUSH_CODE_PATH_UPDATED;
      memcpy(&out_frame[1], client->id.pub_key, PUB_KEY_SIZE);
      _serial->writeFrame(out_frame, 1 + PUB_KEY_SIZE);
    }

    if (extra_type == PAYLOAD_TYPE_RESPONSE && extra_len >= 4) {
      handleCompanionContactResponse(*client, extra, extra_len);
    }
  } else {
    MESH_DEBUG_PRINTLN("onPeerPathRecv: invalid peer idx: %d", i);
  }

  // NOTE: no reciprocal path send!!
  return false;
}

#define CTL_TYPE_NODE_DISCOVER_REQ   0x80
#define CTL_TYPE_NODE_DISCOVER_RESP  0x90

void MyMesh::onControlDataRecv(mesh::Packet* packet) {
  uint8_t type = packet->payload[0] & 0xF0;    // just test upper 4 bits
  if (type == CTL_TYPE_NODE_DISCOVER_REQ && packet->payload_len >= 6
      && !_prefs.disable_fwd && discover_limiter.allow(rtc_clock.getCurrentTime())
  ) {
    int i = 1;
    uint8_t  filter = packet->payload[i++];
    uint32_t tag;
    memcpy(&tag, &packet->payload[i], 4); i += 4;
    uint32_t since;
    if (packet->payload_len >= i+4) {   // optional since field
      memcpy(&since, &packet->payload[i], 4); i += 4;
    } else {
      since = 0;
    }

    if ((filter & (1 << ADV_TYPE_REPEATER)) != 0 && _prefs.discovery_mod_timestamp >= since) {
      bool prefix_only = packet->payload[0] & 1;
      uint8_t data[6 + PUB_KEY_SIZE];
      data[0] = CTL_TYPE_NODE_DISCOVER_RESP | ADV_TYPE_REPEATER;   // low 4-bits for node type
      data[1] = packet->_snr;   // let sender know the inbound SNR ( x 4)
      memcpy(&data[2], &tag, 4);     // include tag from request, for client to match to
      memcpy(&data[6], self_id.pub_key, PUB_KEY_SIZE);
      auto resp = createControlData(data, prefix_only ? 6 + 8 : 6 + PUB_KEY_SIZE);
      if (resp) {
        sendZeroHop(resp, getRetransmitDelay(resp)*4);  // apply random delay (widened x4), as multiple nodes can respond to this
      }
    }
  } else if (type == CTL_TYPE_NODE_DISCOVER_RESP && packet->payload_len >= 6) {
    uint8_t node_type = packet->payload[0] & 0x0F;
    if (node_type != ADV_TYPE_REPEATER) {
      return;
    }
    if (packet->payload_len < 6 + PUB_KEY_SIZE) {
      MESH_DEBUG_PRINTLN("onControlDataRecv: DISCOVER_RESP pubkey too short: %d", (uint32_t)packet->payload_len);
      return;
    }

    if (pending_discover_tag == 0 || millisHasNowPassed(pending_discover_until)) {
      pending_discover_tag = 0;
      return;
    }
    uint32_t tag;
    memcpy(&tag, &packet->payload[2], 4);
    if (tag != pending_discover_tag) {
      return;
    }

    mesh::Identity id(&packet->payload[6]);
    if (id.matches(self_id)) {
      return;
    }
    putNeighbour(id, rtc_clock.getCurrentTime(), packet->getSNR());
  }
}

void MyMesh::sendNodeDiscoverReq() {
  uint8_t data[10];
  data[0] = CTL_TYPE_NODE_DISCOVER_REQ; // prefix_only=0
  data[1] = (1 << ADV_TYPE_REPEATER);
  getRNG()->random(&data[2], 4); // tag
  memcpy(&pending_discover_tag, &data[2], 4);
  pending_discover_until = futureMillis(60000);
  uint32_t since = 0;
  memcpy(&data[6], &since, 4);

  auto pkt = createControlData(data, sizeof(data));
  if (pkt) {
    sendZeroHop(pkt);
  }
}

MyMesh::MyMesh(mesh::MainBoard &board, mesh::Radio &radio, mesh::MillisecondClock &ms, mesh::RNG &rng,
               mesh::RTCClock &rtc, mesh::MeshTables &tables)
    : mesh::Mesh(radio, ms, rng, rtc, *new StaticPoolPacketManager(32), tables),
      _cli(board, rtc, sensors, acl, &_prefs, this), telemetry(MAX_PACKET_PAYLOAD - 4), region_map(key_store), temp_map(key_store),
      discover_limiter(4, 120),  // max 4 every 2 minutes
      anon_limiter(4, 180)   // max 4 every 3 minutes
#if defined(WITH_RS232_BRIDGE)
      , bridge(&_prefs, WITH_RS232_BRIDGE, _mgr, &rtc)
#endif
#if defined(WITH_ESPNOW_BRIDGE)
      , bridge(&_prefs, _mgr, &rtc)
#endif
{
  last_millis = 0;
  uptime_millis = 0;
  next_local_advert = next_flood_advert = 0;
  dirty_contacts_expiry = 0;
  set_radio_at = revert_radio_at = 0;
  _logging = false;
  region_load_active = false;
  _serial = NULL;
  clearPendingReqs();
  memset(cmd_frame, 0, sizeof(cmd_frame));
  memset(out_frame, 0, sizeof(out_frame));
  clearGroupFloodRetryState();

#if MAX_NEIGHBOURS
  memset(neighbours, 0, sizeof(neighbours));
#endif

  // defaults
  memset(&_prefs, 0, sizeof(_prefs));
  _prefs.airtime_factor = 1.0;   // one half
  _prefs.rx_delay_base = 0.0f;   // turn off by default, was 10.0;
  _prefs.tx_delay_factor = 0.5f; // was 0.25f
  _prefs.direct_tx_delay_factor = 0.3f; // was 0.2
  StrHelper::strncpy(_prefs.node_name, ADVERT_NAME, sizeof(_prefs.node_name));
  _prefs.node_lat = ADVERT_LAT;
  _prefs.node_lon = ADVERT_LON;
  StrHelper::strncpy(_prefs.password, ADMIN_PASSWORD, sizeof(_prefs.password));
  _prefs.freq = LORA_FREQ;
  _prefs.sf = LORA_SF;
  _prefs.bw = LORA_BW;
  _prefs.cr = LORA_CR;
  _prefs.tx_power_dbm = LORA_TX_POWER;
  _prefs.advert_interval = 1;        // default to 2 minutes for NEW installs
  _prefs.flood_advert_interval = 12; // 12 hours
  _prefs.flood_max = 64;
  _prefs.interference_threshold = 0; // disabled

  // bridge defaults
  _prefs.bridge_enabled = 1;    // enabled
  _prefs.bridge_delay   = 500;  // milliseconds
  _prefs.bridge_pkt_src = 0;    // logTx
  _prefs.bridge_baud = 115200;  // baud rate
  _prefs.bridge_channel = 1;    // channel 1

  StrHelper::strncpy(_prefs.bridge_secret, "LVSITANOS", sizeof(_prefs.bridge_secret));

  // GPS defaults
  _prefs.gps_enabled = 0;
  _prefs.gps_interval = 0;
  _prefs.advert_loc_policy = ADVERT_LOC_PREFS;

  _prefs.adc_multiplier = 0.0f; // 0.0f means use default board multiplier

  pending_discover_tag = 0;
  pending_discover_until = 0;
}

void MyMesh::begin(FILESYSTEM *fs) {
  mesh::Mesh::begin();
  _fs = fs;
  // load persisted prefs
  _cli.loadPrefs(_fs);
  acl.load(_fs, self_id);
  // TODO: key_store.begin();
  region_map.load(_fs);

#if defined(WITH_BRIDGE)
  if (_prefs.bridge_enabled) {
    bridge.begin();
  }
#endif

  radio_set_params(_prefs.freq, _prefs.bw, _prefs.sf, _prefs.cr);
  radio_set_tx_power(_prefs.tx_power_dbm);

  updateAdvertTimer();
  updateFloodAdvertTimer();

  board.setAdcMultiplier(_prefs.adc_multiplier);

#if ENV_INCLUDE_GPS == 1
  applyGpsPrefs();
#endif
}

void MyMesh::applyTempRadioParams(float freq, float bw, uint8_t sf, uint8_t cr, int timeout_mins) {
  set_radio_at = futureMillis(2000); // give CLI reply some time to be sent back, before applying temp radio params
  pending_freq = freq;
  pending_bw = bw;
  pending_sf = sf;
  pending_cr = cr;

  revert_radio_at = futureMillis(2000 + timeout_mins * 60 * 1000); // schedule when to revert radio params
}

bool MyMesh::formatFileSystem() {
#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
  return InternalFS.format();
#elif defined(RP2040_PLATFORM)
  return LittleFS.format();
#elif defined(ESP32)
  return SPIFFS.format();
#else
#error "need to implement file system erase"
  return false;
#endif
}

void MyMesh::sendSelfAdvertisement(int delay_millis, bool flood) {
  mesh::Packet *pkt = createSelfAdvert();
  if (pkt) {
    if (flood) {
      sendFlood(pkt, delay_millis);
    } else {
      sendZeroHop(pkt, delay_millis);
    }
  } else {
    MESH_DEBUG_PRINTLN("ERROR: unable to create advertisement packet!");
  }
}

void MyMesh::updateAdvertTimer() {
  if (_prefs.advert_interval > 0) { // schedule local advert timer
    next_local_advert = futureMillis(((uint32_t)_prefs.advert_interval) * 2 * 60 * 1000);
  } else {
    next_local_advert = 0; // stop the timer
  }
}

void MyMesh::updateFloodAdvertTimer() {
  if (_prefs.flood_advert_interval > 0) { // schedule flood advert timer
    next_flood_advert = futureMillis(((uint32_t)_prefs.flood_advert_interval) * 60 * 60 * 1000);
  } else {
    next_flood_advert = 0; // stop the timer
  }
}

void MyMesh::dumpLogFile() {
#if defined(RP2040_PLATFORM)
  File f = _fs->open(PACKET_LOG_FILE, "r");
#else
  File f = _fs->open(PACKET_LOG_FILE);
#endif
  if (f) {
    while (f.available()) {
      int c = f.read();
      if (c < 0) break;
      Serial.print((char)c);
    }
    f.close();
  }
}

void MyMesh::setTxPower(int8_t power_dbm) {
  radio_set_tx_power(power_dbm);
}

void MyMesh::formatNeighborsReply(char *reply) {
  char *dp = reply;

#if MAX_NEIGHBOURS
  // create copy of neighbours list, skipping empty entries so we can sort it separately from main list
  int16_t neighbours_count = 0;
  NeighbourInfo* sorted_neighbours[MAX_NEIGHBOURS];
  for (int i = 0; i < MAX_NEIGHBOURS; i++) {
    auto neighbour = &neighbours[i];
    if (neighbour->heard_timestamp > 0) {
      sorted_neighbours[neighbours_count] = neighbour;
      neighbours_count++;
    }
  }

  // sort neighbours newest to oldest
  std::sort(sorted_neighbours, sorted_neighbours + neighbours_count, [](const NeighbourInfo* a, const NeighbourInfo* b) {
    return a->heard_timestamp > b->heard_timestamp; // desc
  });

  for (int i = 0; i < neighbours_count && dp - reply < 134; i++) {
    NeighbourInfo *neighbour = sorted_neighbours[i];

    // add new line if not first item
    if (i > 0) *dp++ = '\n';

    char hex[10];
    // get 4 bytes of neighbour id as hex
    mesh::Utils::toHex(hex, neighbour->id.pub_key, 4);

    // add next neighbour
    uint32_t secs_ago = getRTCClock()->getCurrentTime() - neighbour->heard_timestamp;
    sprintf(dp, "%s:%d:%d", hex, secs_ago, neighbour->snr);
    while (*dp)
      dp++; // find end of string
  }
#endif
  if (dp == reply) { // no neighbours, need empty response
    strcpy(dp, "-none-");
    dp += 6;
  }
  *dp = 0; // null terminator
}

void MyMesh::removeNeighbor(const uint8_t *pubkey, int key_len) {
#if MAX_NEIGHBOURS
  for (int i = 0; i < MAX_NEIGHBOURS; i++) {
    NeighbourInfo *neighbour = &neighbours[i];
    if (memcmp(neighbour->id.pub_key, pubkey, key_len) == 0) {
      neighbours[i] = NeighbourInfo(); // clear neighbour entry
    }
  }
#endif
}

void MyMesh::formatStatsReply(char *reply) {
  StatsFormatHelper::formatCoreStats(reply, board, *_ms, _err_flags, _mgr);
}

void MyMesh::formatRadioStatsReply(char *reply) {
  StatsFormatHelper::formatRadioStats(reply, _radio, radio_driver, getTotalAirTime(), getReceiveAirTime());
}

void MyMesh::formatPacketStatsReply(char *reply) {
  StatsFormatHelper::formatPacketStats(reply, radio_driver, getNumSentFlood(), getNumSentDirect(), 
                                       getNumRecvFlood(), getNumRecvDirect());
#if ENABLE_GROUP_FLOOD_CONDITIONAL_RETRY == 1
  uint32_t loss_permille = (_group_retry_tracked == 0) ? 0 : (_group_retry_failed * 1000UL) / _group_retry_tracked;
  sprintf(&reply[strlen(reply)],
          "\nrelay.grp trk=%lu ok=%lu fail=%lu retry=%lu loss=%lu.%lu%%",
          _group_retry_tracked,
          _group_retry_confirmed,
          _group_retry_failed,
          _group_retry_retransmits,
          loss_permille / 10,
          loss_permille % 10);
#endif
}

void MyMesh::saveIdentity(const mesh::LocalIdentity &new_id) {
#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
  IdentityStore store(*_fs, "");
#elif defined(ESP32)
  IdentityStore store(*_fs, "/identity");
#elif defined(RP2040_PLATFORM)
  IdentityStore store(*_fs, "/identity");
#else
#error "need to define saveIdentity()"
#endif
  store.save("_main", new_id);
}

void MyMesh::clearStats() {
  radio_driver.resetStats();
  resetStats();
  ((SimpleMeshTables *)getTables())->resetStats();
  clearGroupFloodRetryState();
}

void MyMesh::handleCommand(uint32_t sender_timestamp, char *command, char *reply) {
  if (region_load_active) {
    if (StrHelper::isBlank(command)) {  // empty/blank line, signal to terminate 'load' operation
      region_map = temp_map;  // copy over the temp instance as new current map
      region_load_active = false;

      sprintf(reply, "OK - loaded %d regions", region_map.getCount());
    } else {
      char *np = command;
      while (*np == ' ') np++;   // skip indent
      int indent = np - command;

      char *ep = np;
      while (RegionMap::is_name_char(*ep)) ep++;
      if (*ep) { *ep++ = 0; }  // set null terminator for end of name

      while (*ep && *ep != 'F') ep++;  // look for (optional) flags

      if (indent > 0 && indent < 8 && strlen(np) > 0) {
        auto parent = load_stack[indent - 1];
        if (parent) {
          auto old = region_map.findByName(np);
          auto nw = temp_map.putRegion(np, parent->id, old ? old->id : 0);  // carry-over the current ID (if name already exists)
          if (nw) {
            nw->flags = old ? old->flags : (*ep == 'F' ? 0 : REGION_DENY_FLOOD);   // carry-over flags from curr

            load_stack[indent] = nw;  // keep pointers to parent regions, to resolve parent_id's
          }
        }
      }
      reply[0] = 0;
    }
    return;
  }

  command = (char *) skipCommandSpaces(command);

  size_t prefix_len = getCommandPrefixLen(command);
  if (prefix_len > 0) { // optional prefix (for companion radio CLI)
    memcpy(reply, command, prefix_len); // reflect the prefix back
    reply += prefix_len;
    command += prefix_len;
  }

  // handle ACL related commands
  if (memcmp(command, "setperm ", 8) == 0) {   // format:  setperm {pubkey-hex} {permissions-int8}
    char* hex = &command[8];
    char* sp = strchr(hex, ' ');   // look for separator char
    if (sp == NULL) {
      strcpy(reply, "Err - bad params");
    } else {
      *sp++ = 0;   // replace space with null terminator

      uint8_t pubkey[PUB_KEY_SIZE];
      int hex_len = min(sp - hex, PUB_KEY_SIZE*2);
      if (mesh::Utils::fromHex(pubkey, hex_len / 2, hex)) {
        uint8_t perms = atoi(sp);
        if (acl.applyPermissions(self_id, pubkey, hex_len / 2, perms)) {
          dirty_contacts_expiry = futureMillis(LAZY_CONTACTS_WRITE_DELAY);   // trigger acl.save()
          strcpy(reply, "OK");
        } else {
          strcpy(reply, "Err - invalid params");
        }
      } else {
        strcpy(reply, "Err - bad pubkey");
      }
    }
  } else if (sender_timestamp == 0 && strcmp(command, "get acl") == 0) {
    Serial.println("ACL:");
    for (int i = 0; i < acl.getNumClients(); i++) {
      auto c = acl.getClientByIdx(i);
      if (c->permissions == 0) continue;  // skip deleted (or guest) entries

      Serial.printf("%02X ", c->permissions);
      mesh::Utils::printHex(Serial, c->id.pub_key, PUB_KEY_SIZE);
      Serial.printf("\n");
    }
    reply[0] = 0;
  } else if (memcmp(command, "region", 6) == 0) {
    reply[0] = 0;

    const char* parts[4];
    int n = mesh::Utils::parseTextParts(command, parts, 4, ' ');
    if (n == 1) {
      region_map.exportTo(reply, 160);
    } else if (n >= 2 && strcmp(parts[1], "load") == 0) {
      temp_map.resetFrom(region_map);   // rebuild regions in a temp instance
      memset(load_stack, 0, sizeof(load_stack));
      load_stack[0] = &temp_map.getWildcard();
      region_load_active = true;
    } else if (n >= 2 && strcmp(parts[1], "save") == 0) {
      _prefs.discovery_mod_timestamp = rtc_clock.getCurrentTime();   // this node is now 'modified' (for discovery info)
      savePrefs();
      bool success = region_map.save(_fs);
      strcpy(reply, success ? "OK" : "Err - save failed");
    } else if (n >= 3 && strcmp(parts[1], "allowf") == 0) {
      auto region = region_map.findByNamePrefix(parts[2]);
      if (region) {
        region->flags &= ~REGION_DENY_FLOOD;
        strcpy(reply, "OK");
      } else {
        strcpy(reply, "Err - unknown region");
      }
    } else if (n >= 3 && strcmp(parts[1], "denyf") == 0) {
      auto region = region_map.findByNamePrefix(parts[2]);
      if (region) {
        region->flags |= REGION_DENY_FLOOD;
        strcpy(reply, "OK");
      } else {
        strcpy(reply, "Err - unknown region");
      }
    } else if (n >= 3 && strcmp(parts[1], "get") == 0) {
      auto region = region_map.findByNamePrefix(parts[2]);
      if (region) {
        auto parent = region_map.findById(region->parent);
        if (parent && parent->id != 0) {
          sprintf(reply, " %s (%s) %s", region->name, parent->name, (region->flags & REGION_DENY_FLOOD) ? "" : "F");
        } else {
          sprintf(reply, " %s %s", region->name, (region->flags & REGION_DENY_FLOOD) ? "" : "F");
        }
      } else {
        strcpy(reply, "Err - unknown region");
      }
    } else if (n >= 3 && strcmp(parts[1], "home") == 0) {
      auto home = region_map.findByNamePrefix(parts[2]);
      if (home) {
        region_map.setHomeRegion(home);
        sprintf(reply, " home is now %s", home->name);
      } else {
        strcpy(reply, "Err - unknown region");
      }
    } else if (n == 2 && strcmp(parts[1], "home") == 0) {
      auto home = region_map.getHomeRegion();
      sprintf(reply, " home is %s", home ? home->name : "*");
    } else if (n >= 3 && strcmp(parts[1], "put") == 0) {
      auto parent = n >= 4 ? region_map.findByNamePrefix(parts[3]) : &region_map.getWildcard();
      if (parent == NULL) {
        strcpy(reply, "Err - unknown parent");
      } else {
        auto region = region_map.putRegion(parts[2], parent->id);
        if (region == NULL) {
          strcpy(reply, "Err - unable to put");
        } else {
          strcpy(reply, "OK");
        }
      }
    } else if (n >= 3 && strcmp(parts[1], "remove") == 0) {
      auto region = region_map.findByName(parts[2]);
      if (region) {
        if (region_map.removeRegion(*region)) {
          strcpy(reply, "OK");
        } else {
          strcpy(reply, "Err - not empty");
        }
      } else {
        strcpy(reply, "Err - not found");
      }
    } else if (n >= 3 && strcmp(parts[1], "list") == 0) {
      uint8_t mask = 0;
      bool invert = false;
      
      if (strcmp(parts[2], "allowed") == 0) {
        mask = REGION_DENY_FLOOD;
        invert = false;  // list regions that DON'T have DENY flag
      } else if (strcmp(parts[2], "denied") == 0) {
        mask = REGION_DENY_FLOOD;
        invert = true;   // list regions that DO have DENY flag
      } else {
        strcpy(reply, "Err - use 'allowed' or 'denied'");
        return;
      }
      
      int len = region_map.exportNamesTo(reply, 160, mask, invert);
      if (len == 0) {
        strcpy(reply, "-none-");
      }
    } else {
      strcpy(reply, "Err - ??");
    }
  } else if (strcmp(command, "usb on") == 0) {
    if (meshcore_board_usb_on_demand()) {
      strcpy(reply, "OK - USB enabled");
    } else {
      strcpy(reply, "Err - USB unsupported");
    }
  } else if (strcmp(command, "usb off") == 0) {
    if (meshcore_board_usb_off_demand()) {
      strcpy(reply, "OK - USB disabled");
    } else {
      strcpy(reply, "Err - USB unsupported");
    }
  } else if (strcmp(command, "usb status") == 0) {
    strcpy(reply, meshcore_board_usb_is_connected() ? "> on" : "> off");
  } else if (memcmp(command, "discover.neighbors", 18) == 0) {
    const char* sub = command + 18;
    while (*sub == ' ') sub++;
    if (*sub != 0) {
      strcpy(reply, "Err - discover.neighbors has no options");
    } else {
      sendNodeDiscoverReq();
      strcpy(reply, "OK - Discover sent");
    }
  } else{
    _cli.handleCommand(sender_timestamp, command, reply);  // common CLI commands
  }
}

void MyMesh::loop() {
#ifdef WITH_BRIDGE
  bridge.loop();
#endif

  checkCompanionInterface();

  mesh::Mesh::loop();
  processGroupFloodRetries();

  if (next_flood_advert && millisHasNowPassed(next_flood_advert)) {
    mesh::Packet *pkt = createSelfAdvert();
    if (pkt) sendFlood(pkt);

    updateFloodAdvertTimer(); // schedule next flood advert
    updateAdvertTimer();      // also schedule local advert (so they don't overlap)
  } else if (next_local_advert && millisHasNowPassed(next_local_advert)) {
    mesh::Packet *pkt = createSelfAdvert();
    if (pkt) sendZeroHop(pkt);

    updateAdvertTimer(); // schedule next local advert
  }

  if (set_radio_at && millisHasNowPassed(set_radio_at)) { // apply pending (temporary) radio params
    set_radio_at = 0;                                     // clear timer
    radio_set_params(pending_freq, pending_bw, pending_sf, pending_cr);
    MESH_DEBUG_PRINTLN("Temp radio params");
  }

  if (revert_radio_at && millisHasNowPassed(revert_radio_at)) { // revert radio params to orig
    revert_radio_at = 0;                                        // clear timer
    radio_set_params(_prefs.freq, _prefs.bw, _prefs.sf, _prefs.cr);
    MESH_DEBUG_PRINTLN("Radio params restored");
  }

  // is pending dirty contacts write needed?
  if (dirty_contacts_expiry && millisHasNowPassed(dirty_contacts_expiry)) {
    acl.save(_fs);
    dirty_contacts_expiry = 0;
  }

  // update uptime
  uint32_t now = millis();
  uptime_millis += now - last_millis;
  last_millis = now;
}

// To check if there is pending work
bool MyMesh::hasPendingWork() const {
#if defined(WITH_BRIDGE)
  if (bridge.isRunning()) return true;  // bridge needs WiFi radio, can't sleep
#endif
  if (_serial && _serial->isEnabled()) return true;  // companion API must stay reachable
  return _mgr->getOutboundCount(0xFFFFFFFF) > 0;
}
