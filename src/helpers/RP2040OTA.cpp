#if defined(RP2040_PLATFORM)

#include "RP2040OTA.h"

#include <Arduino.h>
#include <LittleFS.h>
#include <Updater.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#define OTA_CMD_MAX_CHUNK_BYTES  96
#define OTA_PROGRESS_LOG_STEP_BYTES 4096

#ifndef RP2040_OTA_SERIAL_DEBUG
  #define RP2040_OTA_SERIAL_DEBUG 1
#endif

#if RP2040_OTA_SERIAL_DEBUG
  #define OTA_DEBUG_PRINTLN(F, ...) Serial.printf("[OTA] " F "\n", ##__VA_ARGS__)
#else
  #define OTA_DEBUG_PRINTLN(...)
#endif

RP2040OTAController::RP2040OTAController() {
  clearState();
}

void RP2040OTAController::clearState() {
  _armed = false;
  _active = false;
  _expected_size = 0;
  _received_size = 0;
  _next_progress_log = 0;
  _ack_every_chunks = 1;
  _chunks_since_ack = 0;
}

const char *RP2040OTAController::skipSpaces(const char *p) {
  while (*p == ' ') {
    p++;
  }
  return p;
}

bool RP2040OTAController::isValidHexChar(char c) {
  return (c >= '0' && c <= '9') ||
         (c >= 'a' && c <= 'f') ||
         (c >= 'A' && c <= 'F');
}

bool RP2040OTAController::decodeHex(const char *hex, size_t hex_len, uint8_t *out, size_t max_out, size_t *out_len) {
  if ((hex_len & 1) != 0) return false;
  size_t bytes = hex_len / 2;
  if (bytes > max_out) return false;

  for (size_t i = 0; i < bytes; i++) {
    char hi = hex[i * 2];
    char lo = hex[i * 2 + 1];
    if (!isValidHexChar(hi) || !isValidHexChar(lo)) {
      return false;
    }

    uint8_t hv = (uint8_t)(isdigit((unsigned char)hi) ? (hi - '0') : (tolower((unsigned char)hi) - 'a' + 10));
    uint8_t lv = (uint8_t)(isdigit((unsigned char)lo) ? (lo - '0') : (tolower((unsigned char)lo) - 'a' + 10));
    out[i] = (uint8_t)((hv << 4) | lv);
  }

  *out_len = bytes;
  return true;
}

bool RP2040OTAController::startSession(const char *id, char reply[]) {
  OTA_DEBUG_PRINTLN("start requested by %s", id ? id : "?");

  if (!LittleFS.begin()) {
    strcpy(reply, "Err - LittleFS unavailable");
    OTA_DEBUG_PRINTLN("LittleFS unavailable");
    clearState();
    return true;
  }

  if (Update.isRunning()) {
    strcpy(reply, "Err - OTA already running");
    OTA_DEBUG_PRINTLN("rejected: OTA already running");
    return true;
  }

  clearState();
  _armed = true;

  FSInfo fs_info;
  if (LittleFS.info(fs_info) && fs_info.totalBytes >= fs_info.usedBytes) {
    uint32_t free_kb = (uint32_t)((fs_info.totalBytes - fs_info.usedBytes) / 1024ULL);
    sprintf(reply, "OK - OTA ready (%lukB free)", (unsigned long) free_kb);
    OTA_DEBUG_PRINTLN("session armed, free=%lu kB", (unsigned long) free_kb);
  } else {
    strcpy(reply, "OK - OTA ready");
    OTA_DEBUG_PRINTLN("session armed, fs info unavailable");
  }
  return true;
}

bool RP2040OTAController::handleCommand(const char *command, char reply[]) {
  const char *sub = skipSpaces(command);

  if (*sub == 0 || strcmp(sub, "help") == 0) {
    strcpy(reply, "OTA: begin/write/end/status/abort");
    return true;
  }

  if (strncmp(sub, "status", 6) == 0 && (sub[6] == 0 || sub[6] == ' ')) {
    if (_active) {
      sprintf(reply, "OTA active %lu/%lu", (unsigned long) _received_size, (unsigned long) _expected_size);
    } else if (_armed) {
      strcpy(reply, "OTA armed");
    } else {
      strcpy(reply, "OTA idle");
    }
    return true;
  }

  if (strncmp(sub, "abort", 5) == 0 && (sub[5] == 0 || sub[5] == ' ')) {
    if (Update.isRunning()) {
      Update.end(true);  // force reset of update state
    }
    clearState();
    OTA_DEBUG_PRINTLN("aborted");
    strcpy(reply, "OK - OTA aborted");
    return true;
  }

  if (strncmp(sub, "begin", 5) == 0 && (sub[5] == 0 || sub[5] == ' ')) {
    if (!_armed) {
      OTA_DEBUG_PRINTLN("begin rejected: not armed");
      strcpy(reply, "Err - run 'start ota' first");
      return true;
    }
    if (Update.isRunning()) {
      OTA_DEBUG_PRINTLN("begin rejected: OTA already running");
      strcpy(reply, "Err - OTA already running");
      return true;
    }

    sub = skipSpaces(sub + 5);
    if (*sub == 0) {
      strcpy(reply, "Err - ota begin <size> [md5] [ack_every]");
      return true;
    }

    char *endptr = nullptr;
    unsigned long size_ul = strtoul(sub, &endptr, 10);
    if (endptr == sub || size_ul == 0) {
      OTA_DEBUG_PRINTLN("begin rejected: bad size");
      strcpy(reply, "Err - bad OTA size");
      return true;
    }
    sub = skipSpaces(endptr);

    bool has_md5 = false;
    uint16_t ack_every = 1;
    char md5[33];
    memset(md5, 0, sizeof(md5));
    if (*sub != 0) {
      const char *tok1 = sub;
      const char *tok1_end = tok1;
      while (*tok1_end && *tok1_end != ' ') {
        tok1_end++;
      }
      size_t tok1_len = (size_t)(tok1_end - tok1);

      bool tok1_is_md5 = (tok1_len == 32);
      if (tok1_is_md5) {
        for (size_t i = 0; i < tok1_len; i++) {
          if (!isValidHexChar(tok1[i])) {
            tok1_is_md5 = false;
            break;
          }
        }
      }

      if (tok1_is_md5) {
        memcpy(md5, tok1, 32);
        has_md5 = true;
        sub = skipSpaces(tok1_end);
      } else {
        char *ack_end = nullptr;
        unsigned long ack_ul = strtoul(tok1, &ack_end, 10);
        if (ack_end == tok1 || ack_end != tok1_end) {
          if (tok1_len == 32) {
            OTA_DEBUG_PRINTLN("begin rejected: bad md5 value");
            strcpy(reply, "Err - bad MD5 value");
          } else {
            OTA_DEBUG_PRINTLN("begin rejected: bad ack_every token");
            strcpy(reply, "Err - bad ack_every");
          }
          return true;
        }
        if (ack_ul < 1 || ack_ul > 64) {
          OTA_DEBUG_PRINTLN("begin rejected: bad ack_every %lu", ack_ul);
          strcpy(reply, "Err - ack_every range 1..64");
          return true;
        }
        ack_every = (uint16_t) ack_ul;
        sub = skipSpaces(tok1_end);
      }

      if (*sub != 0) {
        const char *tok2 = sub;
        const char *tok2_end = tok2;
        while (*tok2_end && *tok2_end != ' ') {
          tok2_end++;
        }
        char *ack_end = nullptr;
        unsigned long ack_ul = strtoul(tok2, &ack_end, 10);
        if (ack_end == tok2 || ack_end != tok2_end) {
          OTA_DEBUG_PRINTLN("begin rejected: bad ack_every token");
          strcpy(reply, "Err - bad ack_every");
          return true;
        }
        if (ack_ul < 1 || ack_ul > 64) {
          OTA_DEBUG_PRINTLN("begin rejected: bad ack_every %lu", ack_ul);
          strcpy(reply, "Err - ack_every range 1..64");
          return true;
        }
        ack_every = (uint16_t) ack_ul;
        sub = skipSpaces(tok2_end);
      }

      if (*sub != 0) {
        OTA_DEBUG_PRINTLN("begin rejected: too many args");
        strcpy(reply, "Err - too many args");
        return true;
      }
    }

    if (!Update.begin((size_t) size_ul, U_FLASH)) {
      sprintf(reply, "Err - begin failed (%u)", (uint32_t) Update.getError());
      OTA_DEBUG_PRINTLN("begin failed, err=%u", (uint32_t) Update.getError());
      clearState();
      return true;
    }

    if (has_md5 && !Update.setMD5(md5)) {
      Update.end(true);
      clearState();
      OTA_DEBUG_PRINTLN("begin failed: invalid md5 format");
      strcpy(reply, "Err - invalid MD5");
      return true;
    }

    _active = true;
    _expected_size = (size_t) size_ul;
    _received_size = 0;
    _next_progress_log = OTA_PROGRESS_LOG_STEP_BYTES;
    _ack_every_chunks = ack_every;
    _chunks_since_ack = 0;
    OTA_DEBUG_PRINTLN("begin ok, size=%lu md5=%s ack_every=%u",
                      (unsigned long) _expected_size,
                      has_md5 ? md5 : "(none)",
                      (unsigned int) _ack_every_chunks);
    strcpy(reply, "OK - OTA begin");
    return true;
  }

  if (strncmp(sub, "write", 5) == 0 && (sub[5] == 0 || sub[5] == ' ')) {
    if (!_active || !Update.isRunning()) {
      OTA_DEBUG_PRINTLN("write rejected: not started");
      strcpy(reply, "Err - OTA not started");
      return true;
    }

    sub = skipSpaces(sub + 5);
    if (*sub == 0) {
      OTA_DEBUG_PRINTLN("write rejected: missing chunk");
      strcpy(reply, "Err - missing OTA chunk");
      return true;
    }

    const char *hex_start = sub;
    size_t expected_offset = _received_size;

    // optional format: ota write <offset> <hex>
    const char *sp = sub;
    while (*sp && *sp != ' ') {
      sp++;
    }
    if (*sp == ' ') {
      char *endptr = nullptr;
      unsigned long offset = strtoul(sub, &endptr, 10);
      if (endptr && endptr > sub && endptr == sp) {
        expected_offset = (size_t) offset;
        hex_start = skipSpaces(sp);
      }
    }

    if (expected_offset != _received_size) {
      OTA_DEBUG_PRINTLN("write rejected: bad offset %lu expected %lu",
                        (unsigned long) expected_offset,
                        (unsigned long) _received_size);
      sprintf(reply, "Err - offset %lu expected %lu", (unsigned long) expected_offset, (unsigned long) _received_size);
      return true;
    }

    size_t hex_len = strlen(hex_start);
    sub = hex_start;
    while (hex_len > 0 && sub[hex_len - 1] == ' ') {
      hex_len--;
    }
    if (hex_len == 0) {
      OTA_DEBUG_PRINTLN("write rejected: empty hex");
      strcpy(reply, "Err - missing OTA chunk");
      return true;
    }

    uint8_t chunk[OTA_CMD_MAX_CHUNK_BYTES];
    size_t chunk_len = 0;
    if (!decodeHex(sub, hex_len, chunk, sizeof(chunk), &chunk_len)) {
      OTA_DEBUG_PRINTLN("write rejected: bad hex");
      strcpy(reply, "Err - bad OTA hex");
      return true;
    }
    if (chunk_len == 0) {
      OTA_DEBUG_PRINTLN("write rejected: decoded empty chunk");
      strcpy(reply, "Err - empty OTA chunk");
      return true;
    }
    if (_received_size + chunk_len > _expected_size) {
      OTA_DEBUG_PRINTLN("write rejected: overflow");
      strcpy(reply, "Err - OTA overflow");
      return true;
    }

    size_t written = Update.write(chunk, chunk_len);
    if (written != chunk_len) {
      if (Update.isRunning()) {
        Update.end(true);
      }
      sprintf(reply, "Err - write failed (%u)", (uint32_t) Update.getError());
      OTA_DEBUG_PRINTLN("write failed at %lu, err=%u",
                        (unsigned long) _received_size,
                        (uint32_t) Update.getError());
      clearState();
      return true;
    }

    _received_size += chunk_len;
    _chunks_since_ack++;
    if (_received_size >= _next_progress_log || _received_size == _expected_size) {
      OTA_DEBUG_PRINTLN("progress %lu/%lu",
                        (unsigned long) _received_size,
                        (unsigned long) _expected_size);
      _next_progress_log = _received_size + OTA_PROGRESS_LOG_STEP_BYTES;
    }

    bool should_ack_now = (_ack_every_chunks <= 1)
                       || (_chunks_since_ack >= _ack_every_chunks)
                       || (_received_size == _expected_size);
    if (should_ack_now) {
      _chunks_since_ack = 0;
      strcpy(reply, "OK");
    } else {
      reply[0] = 0;  // suppress intermediate ack to reduce return traffic
    }
    return true;
  }

  if (strncmp(sub, "end", 3) == 0 && (sub[3] == 0 || sub[3] == ' ')) {
    if (!_active || !Update.isRunning()) {
      OTA_DEBUG_PRINTLN("end rejected: not started");
      strcpy(reply, "Err - OTA not started");
      return true;
    }
    if (_received_size != _expected_size) {
      OTA_DEBUG_PRINTLN("end rejected: size mismatch %lu/%lu",
                        (unsigned long) _received_size,
                        (unsigned long) _expected_size);
      sprintf(reply, "Err - size mismatch (%lu/%lu)", (unsigned long)_received_size, (unsigned long)_expected_size);
      return true;
    }

    if (!Update.end(false)) {
      sprintf(reply, "Err - end failed (%u)", (uint32_t) Update.getError());
      OTA_DEBUG_PRINTLN("end failed, err=%u", (uint32_t) Update.getError());
      clearState();
      return true;
    }

    OTA_DEBUG_PRINTLN("end ok, image staged");
    clearState();
    strcpy(reply, "OK - OTA staged, reboot now");
    return true;
  }

  OTA_DEBUG_PRINTLN("unknown command: %s", sub);
  strcpy(reply, "Err - unknown OTA cmd");
  return true;
}

#endif
