#if defined(RP2040_PLATFORM)

#include "RP2040OTA.h"

#include <Arduino.h>
#include <LittleFS.h>
#include <Updater.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#define OTA_CMD_MAX_CHUNK_BYTES  160
#define OTA_PROGRESS_LOG_STEP_BYTES 4096

// A session that stays armed/active with no OTA traffic (client gone, lost
// abort packet) would otherwise inhibit sleep and hold the OTA clock profile
// forever. After this idle window the session is safely discarded on the next
// OTA command, and isSleepInhibited() reports false so boards restore their
// active clock profile.
#ifndef RP2040_OTA_SESSION_TIMEOUT_MS
  #define RP2040_OTA_SESSION_TIMEOUT_MS (5UL * 60UL * 1000UL)
#endif

// RAM staging buffer: chunks accumulate here and hit the flash (LittleFS) only
// once per buffer-full. Every flash flush freezes the RP2040 (~50ms per 4KB
// sector) and the OTA client must checkpoint around each freeze, so a bigger
// staging block means 4x fewer mandatory checkpoints (the dominant transfer
// cost over slow links). The active block size is advertised in the 'start
// ota' reply as 'blk=' so the client aligns its checkpoints.
#ifndef RP2040_OTA_STAGE_BUFFER_BYTES
  #define RP2040_OTA_STAGE_BUFFER_BYTES 16384
#endif

// The Updater library's own internal buffer: flash gels happen at this
// granularity when staging is unavailable.
#define OTA_UPDATER_INTERNAL_BLOCK 4096

#ifndef RP2040_OTA_SERIAL_DEBUG
  #define RP2040_OTA_SERIAL_DEBUG 1
#endif

#if RP2040_OTA_SERIAL_DEBUG
  #define OTA_DEBUG_PRINTLN(F, ...) Serial.printf("[OTA] " F "\n", ##__VA_ARGS__)
#else
  #define OTA_DEBUG_PRINTLN(...)
#endif

RP2040OTAController::RP2040OTAController() {
  _stage = NULL;
  _stage_size = 0;
  _stage_base = 0;
  _stage_range_count = 0;
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
  _last_activity_millis = millis();
  if (_stage) {
    free(_stage);
    _stage = NULL;
  }
  _stage_size = 0;
  _stage_base = 0;
  _stage_range_count = 0;
}

size_t RP2040OTAController::stageExtent() const {
  if (_stage_size == 0 || _expected_size <= _stage_base) return 0;
  size_t left = _expected_size - _stage_base;
  return left < _stage_size ? left : _stage_size;
}

size_t RP2040OTAController::stagePrefix() const {
  if (_stage_range_count == 0 || _stage_ranges[0].start != 0) return 0;
  return _stage_ranges[0].end;
}

bool RP2040OTAController::insertStageRange(size_t start, size_t end) {
  int i = 0;
  while (i < _stage_range_count && _stage_ranges[i].end < start) i++;

  if (i == _stage_range_count || _stage_ranges[i].start > end) {
    if (_stage_range_count >= MAX_STAGE_RANGES) return false;
    memmove(&_stage_ranges[i + 1], &_stage_ranges[i],
            (_stage_range_count - i) * sizeof(StageRange));
    _stage_ranges[i].start = (uint16_t) start;
    _stage_ranges[i].end = (uint16_t) end;
    _stage_range_count++;
    return true;
  }

  if (start < _stage_ranges[i].start) _stage_ranges[i].start = (uint16_t) start;
  if (end > _stage_ranges[i].end) _stage_ranges[i].end = (uint16_t) end;
  int j = i + 1;
  while (j < _stage_range_count && _stage_ranges[j].start <= _stage_ranges[i].end) {
    if (_stage_ranges[j].end > _stage_ranges[i].end) _stage_ranges[i].end = _stage_ranges[j].end;
    j++;
  }
  if (j > i + 1) {
    memmove(&_stage_ranges[i + 1], &_stage_ranges[j],
            (_stage_range_count - j) * sizeof(StageRange));
    _stage_range_count -= (uint8_t)(j - i - 1);
  }
  return true;
}

void RP2040OTAController::appendMissList(char reply[], size_t max_len) const {
  if (_stage_range_count == 0) return;
  size_t len = strlen(reply);
  int added = 0;
  size_t hole_start = 0;
  for (int k = 0; k < _stage_range_count; k++) {
    if (_stage_ranges[k].start > hole_start) {
      char item[32];
      snprintf(item, sizeof(item), "%s%lu+%lu",
               added == 0 ? " miss=" : ",",
               (unsigned long)(_stage_base + hole_start),
               (unsigned long)(_stage_ranges[k].start - hole_start));
      size_t item_len = strlen(item);
      if (len + item_len >= max_len) break;
      strcpy(reply + len, item);
      len += item_len;
      if (++added >= 8) break;
    }
    hole_start = _stage_ranges[k].end;
  }
}

size_t RP2040OTAController::flushBlockBytes() const {
  return _stage_size > 0 ? _stage_size : OTA_UPDATER_INTERNAL_BLOCK;
}

bool RP2040OTAController::isSleepInhibited() const {
  return (_armed || _active) && !sessionExpired();
}

bool RP2040OTAController::sessionExpired() const {
  if (!_armed && !_active) return false;
  return (uint32_t)(millis() - _last_activity_millis) > RP2040_OTA_SESSION_TIMEOUT_MS;
}

void RP2040OTAController::expireIfIdle() {
  if (!sessionExpired()) return;
  OTA_DEBUG_PRINTLN("session idle > %lu ms, auto-abort", (unsigned long) RP2040_OTA_SESSION_TIMEOUT_MS);
  abortUpdate();
  clearState();
}

void RP2040OTAController::abortUpdate() {
  // Update.end(true) must NEVER be used to abandon a session: with
  // evenIfRemaining=true the framework finalizes whatever was received and,
  // if no MD5 mismatch stops it, stages the truncated image for flashing on
  // the next boot. end(false) takes the reset path mid-transfer. If the
  // transfer happens to be 100% received, the poisoned MD5 below forces the
  // verification to fail; the framework's MD5-mismatch branch skips _reset(),
  // so a second end() is needed to clear that error state.
  if (!Update.isRunning()) {
    Update.clearError();
    return;
  }
  Update.setMD5("00000000000000000000000000000000");
  if (!Update.end(false) && Update.isRunning()) {
    Update.end(false);
  }
  Update.clearError();
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

static void bytesToHex(const uint8_t *src, size_t len, char *dst) {
  static const char kHex[] = "0123456789abcdef";
  for (size_t i = 0; i < len; i++) {
    dst[i * 2] = kHex[(src[i] >> 4) & 0x0F];
    dst[i * 2 + 1] = kHex[src[i] & 0x0F];
  }
  dst[len * 2] = 0;
}

bool RP2040OTAController::startSession(const char *id, char reply[]) {
  OTA_DEBUG_PRINTLN("start requested by %s", id ? id : "?");
  expireIfIdle();

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
  _last_activity_millis = millis();

  _stage = (uint8_t *) malloc(RP2040_OTA_STAGE_BUFFER_BYTES);
  _stage_size = _stage ? RP2040_OTA_STAGE_BUFFER_BYTES : 0;
  _stage_base = 0;
  _stage_range_count = 0;
  if (_stage_size == 0) {
    OTA_DEBUG_PRINTLN("stage buffer alloc failed, falling back to %u", OTA_UPDATER_INTERNAL_BLOCK);
  }

  FSInfo fs_info;
  if (LittleFS.info(fs_info) && fs_info.totalBytes >= fs_info.usedBytes) {
    uint32_t free_kb = (uint32_t)((fs_info.totalBytes - fs_info.usedBytes) / 1024ULL);
    sprintf(reply, "OK - OTA ready (%lukB free, blk=%u, nack=miss)",
            (unsigned long) free_kb, (unsigned int) flushBlockBytes());
    OTA_DEBUG_PRINTLN("session armed, free=%lu kB, blk=%u", (unsigned long) free_kb, (unsigned int) flushBlockBytes());
  } else {
    sprintf(reply, "OK - OTA ready (blk=%u, nack=miss)", (unsigned int) flushBlockBytes());
    OTA_DEBUG_PRINTLN("session armed, fs info unavailable");
  }
  return true;
}

bool RP2040OTAController::handleCommand(const char *command, char reply[]) {
  expireIfIdle();
  if (_armed || _active) {
    _last_activity_millis = millis();
  }
  const char *sub = skipSpaces(command);

  if (*sub == 0 || strcmp(sub, "help") == 0) {
    strcpy(reply, "OTA: begin/write/end/status/abort");
    return true;
  }

  if (strncmp(sub, "status", 6) == 0 && (sub[6] == 0 || sub[6] == ' ')) {
    if (_active) {
      sprintf(reply, "OTA active %lu/%lu nack=miss blk=%u",
              (unsigned long) _received_size, (unsigned long) _expected_size,
              (unsigned int) flushBlockBytes());
      appendMissList(reply, 140);
    } else if (_armed) {
      strcpy(reply, "OTA armed");
    } else {
      strcpy(reply, "OTA idle");
    }
    return true;
  }

  if (strncmp(sub, "abort", 5) == 0 && (sub[5] == 0 || sub[5] == ' ')) {
    abortUpdate();
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
      abortUpdate();
      clearState();
      OTA_DEBUG_PRINTLN("begin failed: invalid md5 format");
      strcpy(reply, "Err - invalid MD5");
      return true;
    }

    _active = true;
    _expected_size = (size_t) size_ul;
    _received_size = 0;
    _stage_base = 0;
    _stage_range_count = 0;
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

    return writeChunk(expected_offset, chunk, chunk_len, reply);
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

    if (!flushStage(reply)) {
      return true;  // reply set by the failed flush
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

bool RP2040OTAController::writeChunk(size_t offset, const uint8_t *chunk, size_t chunk_len, char reply[]) {
  if (!_active || !Update.isRunning()) {
    OTA_DEBUG_PRINTLN("write rejected: not started");
    strcpy(reply, "Err - OTA not started");
    return true;
  }
  if (chunk_len == 0) {
    OTA_DEBUG_PRINTLN("write rejected: empty chunk");
    strcpy(reply, "Err - empty OTA chunk");
    return true;
  }
  if (offset + chunk_len > _expected_size) {
    OTA_DEBUG_PRINTLN("write rejected: overflow");
    strcpy(reply, "Err - OTA overflow");
    return true;
  }

  bool was_contiguous = (offset == _received_size);

  if (_stage_size > 0) {
    const uint8_t *src = chunk;
    size_t off = offset;
    size_t remaining = chunk_len;
    bool consumed_any = false;
    while (remaining > 0) {
      if (off < _stage_base) {
        size_t skip = _stage_base - off;
        if (skip > remaining) skip = remaining;
        off += skip;
        src += skip;
        remaining -= skip;
        consumed_any = true;
        continue;
      }

      size_t extent = stageExtent();
      size_t win_end = _stage_base + extent;
      if (extent == 0 || off >= win_end) {
        if (!consumed_any) {
          OTA_DEBUG_PRINTLN("write rejected: bad offset %lu expected %lu",
                            (unsigned long) off, (unsigned long) _received_size);
          sprintf(reply, "Err - offset %lu expected %lu",
                  (unsigned long) off, (unsigned long) _received_size);
          return true;
        }
        OTA_DEBUG_PRINTLN("write tail dropped at %lu (window end %lu)",
                          (unsigned long) off, (unsigned long) win_end);
        break;
      }

      size_t n = win_end - off;
      if (n > remaining) n = remaining;
      size_t rel = off - _stage_base;
      if (!insertStageRange(rel, rel + n)) {
        if (!consumed_any) {
          OTA_DEBUG_PRINTLN("write rejected: range table full at %lu (done %lu)",
                            (unsigned long) off, (unsigned long) _received_size);
          sprintf(reply, "Err - offset %lu expected %lu",
                  (unsigned long) off, (unsigned long) _received_size);
          return true;
        }
        break;
      }
      memcpy(_stage + rel, src, n);
      off += n;
      src += n;
      remaining -= n;
      consumed_any = true;
      _received_size = _stage_base + stagePrefix();
      if (stagePrefix() == extent && !flushStage(reply)) {
        return true;  // reply set by the failed flush
      }
    }
  } else {
    if (!was_contiguous) {
      OTA_DEBUG_PRINTLN("write rejected: bad offset %lu expected %lu",
                        (unsigned long) offset, (unsigned long) _received_size);
      sprintf(reply, "Err - offset %lu expected %lu",
              (unsigned long) offset, (unsigned long) _received_size);
      return true;
    }
    if (!writeToUpdater(chunk, chunk_len, reply)) return true;
    _received_size += chunk_len;
  }

  if (_received_size >= _next_progress_log || _received_size == _expected_size) {
    OTA_DEBUG_PRINTLN("progress %lu/%lu",
                      (unsigned long) _received_size,
                      (unsigned long) _expected_size);
    _next_progress_log = _received_size + OTA_PROGRESS_LOG_STEP_BYTES;
  }

  if (!was_contiguous) {
    sprintf(reply, "Err - offset %lu expected %lu",
            (unsigned long) offset, (unsigned long) _received_size);
    return true;
  }

  _chunks_since_ack++;

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

bool RP2040OTAController::writeToUpdater(const uint8_t *data, size_t len, char reply[]) {
  size_t written = Update.write((uint8_t *) data, len);
  if (written != len) {
    uint32_t err = (uint32_t) Update.getError();
    abortUpdate();
    sprintf(reply, "Err - write failed (%u)", err);
    OTA_DEBUG_PRINTLN("write failed at %lu, err=%u", (unsigned long) _received_size, err);
    clearState();
    return false;
  }
  return true;
}

bool RP2040OTAController::flushStage(char reply[]) {
  size_t extent = stageExtent();
  if (extent == 0 || stagePrefix() < extent) return true;
  if (!writeToUpdater(_stage, extent, reply)) return false;
  _stage_base += extent;
  _stage_range_count = 0;
  _received_size = _stage_base;
  return true;
}

bool RP2040OTAController::handleBinaryCommand(uint8_t opcode, const uint8_t *payload, size_t payload_len, char reply[]) {
  if (reply == NULL) {
    return false;
  }

  if (payload == NULL && payload_len > 0) {
    strcpy(reply, "Err - bad payload");
    return true;
  }

  expireIfIdle();
  if (_armed || _active) {
    _last_activity_millis = millis();
  }

  switch (opcode) {
    case BIN_OP_START:
      return startSession("binary", reply);

    case BIN_OP_BEGIN: {
      if (payload_len < 5) {
        strcpy(reply, "Err - bad begin payload");
        return true;
      }

      uint32_t size = (uint32_t) payload[0]
                    | ((uint32_t) payload[1] << 8)
                    | ((uint32_t) payload[2] << 16)
                    | ((uint32_t) payload[3] << 24);
      // ack_every is validated and stored for parity with the text transport,
      // but it has no effect in binary transport: every intermediate write ACK
      // is suppressed there (see BIN_OP_WRITE), so the checkpoint cadence is
      // decided entirely by the client via BIN_OP_STATUS.
      uint8_t ack_every = payload[4];
      if (ack_every < 1 || ack_every > 64) {
        strcpy(reply, "Err - ack_every range 1..64");
        return true;
      }

      if (payload_len >= 21) {
        char md5_hex[33];
        bytesToHex(&payload[5], 16, md5_hex);

        char cmd[96];
        snprintf(cmd, sizeof(cmd), "begin %lu %s %u",
                 (unsigned long) size, md5_hex, (unsigned int) ack_every);
        return handleCommand(cmd, reply);
      }

      char cmd[48];
      snprintf(cmd, sizeof(cmd), "begin %lu %u",
               (unsigned long) size, (unsigned int) ack_every);
      return handleCommand(cmd, reply);
    }

    case BIN_OP_WRITE: {
      // Binary write payload layout:
      //   <offset:u32 little-endian><chunk_len:u8><chunk_bytes...>
      // The packet payload can include trailing cipher padding bytes.
      // Use explicit chunk_len so we ignore padding.
      bool ok = true;
      if (payload_len >= 6) {
        uint32_t offset = (uint32_t) payload[0]
                        | ((uint32_t) payload[1] << 8)
                        | ((uint32_t) payload[2] << 16)
                        | ((uint32_t) payload[3] << 24);
        uint8_t declared_len = payload[4];
        if (declared_len >= 1 && declared_len <= OTA_CMD_MAX_CHUNK_BYTES
            && payload_len >= (size_t)(5 + declared_len)) {
          ok = writeChunk((size_t) offset, &payload[5], declared_len, reply);
        }
      }
      // Binary transport: suppress EVERY per-write reply — the OK acks AND the
      // "Err - offset" rejections. The radio is half-duplex: replying mid-batch
      // blinds the receiver while the client is still streaming, so one RF-lost
      // chunk would trigger an error reply for each subsequent chunk of the
      // batch, each TX losing yet more incoming chunks (loss amplification).
      // Progress and resync are handled exclusively via BIN_OP_STATUS.
      reply[0] = 0;
      return ok;
    }

    case BIN_OP_END:
      return handleCommand("end", reply);

    case BIN_OP_STATUS:
      return handleCommand("status", reply);

    case BIN_OP_ABORT:
      return handleCommand("abort", reply);

    default:
      strcpy(reply, "Err - unknown OTA bin cmd");
      return true;
  }
}

#endif
