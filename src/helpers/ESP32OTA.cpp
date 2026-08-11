#if defined(ESP_PLATFORM)

#include "ESP32OTA.h"

#include <Arduino.h>
#include <MD5Builder.h>
#include <Update.h>
#include <esp_ota_ops.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include <new>

// Streaming gzip inflate uses the tinfl decompressor baked into the SoC ROM
// (esp32/s2/s3/c3 all export it; the esptool flasher stub relies on the same
// code). SoCs whose Arduino SDK does not expose rom/miniz.h fall back to
// raw-only OTA and advertise 'gz=0' so the web client sends an uncompressed
// image.
#ifndef ESP32_OTA_GZIP
  #if __has_include(<rom/miniz.h>)
    #define ESP32_OTA_GZIP 1
  #else
    #define ESP32_OTA_GZIP 0
  #endif
#endif

#if ESP32_OTA_GZIP
  #include <rom/miniz.h>
#endif

#define OTA_CMD_MAX_CHUNK_BYTES  160
#define OTA_PROGRESS_LOG_STEP_BYTES 4096

// A session that stays armed/active with no OTA traffic (client gone, lost
// abort packet) would otherwise inhibit sleep forever. After this idle window
// the session is safely discarded on the next OTA command, and
// isSleepInhibited() reports false so boards resume normal power management.
#ifndef ESP32_OTA_SESSION_TIMEOUT_MS
  #define ESP32_OTA_SESSION_TIMEOUT_MS (5UL * 60UL * 1000UL)
#endif

// RAM staging buffer: chunks accumulate here and hit the flash only once per
// buffer-full. Flash erase/write suspends the cache, stalling flash-resident
// code (including radio servicing) for tens of ms per 4KB sector, so the OTA
// client checkpoints around each flush. The active block size is advertised
// in the 'start ota' reply as 'blk=' so the client aligns its checkpoints.
#ifndef ESP32_OTA_STAGE_BUFFER_BYTES
  #define ESP32_OTA_STAGE_BUFFER_BYTES 16384
#endif

// The Update library's own internal buffer: flash writes happen at this
// granularity when staging is unavailable.
#define OTA_UPDATER_INTERNAL_BLOCK 4096

// MD5Builder::add takes a uint16_t length; the staging buffer is the largest
// block ever fed to the gzip-mode hasher in one call.
static_assert(ESP32_OTA_STAGE_BUFFER_BYTES <= 65535,
              "stage buffer must fit MD5Builder's uint16_t add() length");

#ifndef ESP32_OTA_SERIAL_DEBUG
  #define ESP32_OTA_SERIAL_DEBUG 1
#endif

#if ESP32_OTA_SERIAL_DEBUG
  #define OTA_DEBUG_PRINTLN(F, ...) Serial.printf("[OTA] " F "\n", ##__VA_ARGS__)
#else
  #define OTA_DEBUG_PRINTLN(...)
#endif

#if ESP32_OTA_GZIP

// Push-mode gzip decompressor: compressed bytes are fed in arrival order,
// inflated output is written straight to the Update partition. Also hashes
// the compressed stream (the client's MD5 covers the bytes it sent, which in
// gzip mode is the .gz payload — Update's own MD5 would hash the inflated
// image instead) and keeps the trailing 8 bytes so the gzip footer's ISIZE
// can be checked at 'end'.
class ESP32OTAGzInflater {
public:
  MD5Builder md5;

  ESP32OTAGzInflater() {
    tinfl_init(&_inflator);
    _window_pos = 0;
    _total_out = 0;
    _done = false;
    _hdr_state = HDR_FIXED;
    _hdr_fill = 0;
    _hdr_flags = 0;
    _extra_remaining = 0;
    _tail_len = 0;
    md5.begin();
  }

  // Consume 'len' compressed bytes; returns false on error with a short
  // cause in err[] (max ~40 chars).
  bool feed(const uint8_t *data, size_t len, char err[]) {
    md5.add((uint8_t *) data, len);
    updateTail(data, len);

    while (len > 0) {
      if (_done) {
        return true;  // trailing bytes are the gzip footer, already hashed
      }
      if (_hdr_state != HDR_DONE) {
        if (!consumeHeader(&data, &len, err)) return false;
        continue;
      }

      size_t in_sz = len;
      size_t out_sz = TINFL_LZ_DICT_SIZE - _window_pos;
      tinfl_status st = tinfl_decompress(&_inflator, data, &in_sz,
                                         _window, _window + _window_pos, &out_sz,
                                         TINFL_FLAG_HAS_MORE_INPUT);
      if (out_sz > 0) {
        size_t written = Update.write(_window + _window_pos, out_sz);
        if (written != out_sz) {
          strcpy(err, "flash write failed");
          return false;
        }
        _total_out += out_sz;
        _window_pos = (_window_pos + out_sz) & (TINFL_LZ_DICT_SIZE - 1);
      }
      data += in_sz;
      len -= in_sz;

      if (st == TINFL_STATUS_DONE) {
        _done = true;
      } else if (st < TINFL_STATUS_DONE) {
        strcpy(err, "gzip data corrupt");
        return false;
      } else if (st == TINFL_STATUS_HAS_MORE_OUTPUT && out_sz == 0 && in_sz == 0) {
        strcpy(err, "gzip decoder stall");
        return false;
      }
    }
    return true;
  }

  bool isDone() const { return _done; }
  uint32_t totalOut() const { return _total_out; }

  // gzip footer: CRC32 (skipped, MD5 already covers the stream) then ISIZE,
  // the decompressed size mod 2^32.
  bool checkFooterSize() const {
    if (_tail_len < 8) return false;
    uint32_t isize = (uint32_t) _tail[4]
                   | ((uint32_t) _tail[5] << 8)
                   | ((uint32_t) _tail[6] << 16)
                   | ((uint32_t) _tail[7] << 24);
    return isize == _total_out;
  }

private:
  enum HdrState : uint8_t { HDR_FIXED, HDR_XLEN, HDR_EXTRA, HDR_NAME, HDR_COMMENT, HDR_HCRC, HDR_DONE };
  enum : uint8_t { FLG_FHCRC = 0x02, FLG_FEXTRA = 0x04, FLG_FNAME = 0x08, FLG_FCOMMENT = 0x10 };

  tinfl_decompressor _inflator;
  uint8_t _window[TINFL_LZ_DICT_SIZE];
  size_t _window_pos;
  uint32_t _total_out;
  bool _done;

  uint8_t _hdr_state;
  uint8_t _hdr_buf[10];
  uint8_t _hdr_fill;
  uint8_t _hdr_flags;
  uint16_t _extra_remaining;

  uint8_t _tail[8];
  uint8_t _tail_len;

  void updateTail(const uint8_t *data, size_t len) {
    if (len >= 8) {
      memcpy(_tail, data + len - 8, 8);
      _tail_len = 8;
      return;
    }
    size_t keep = 8 - len;
    if (_tail_len > keep) {
      memmove(_tail, _tail + (_tail_len - keep), keep);
      _tail_len = keep;
    }
    memcpy(_tail + _tail_len, data, len);
    _tail_len += len;
  }

  // Incremental gzip header parser; fields may span chunk boundaries.
  bool consumeHeader(const uint8_t **pdata, size_t *plen, char err[]) {
    const uint8_t *data = *pdata;
    size_t len = *plen;

    while (len > 0 && _hdr_state != HDR_DONE) {
      uint8_t b = *data;
      switch (_hdr_state) {
        case HDR_FIXED:
          _hdr_buf[_hdr_fill++] = b;
          data++; len--;
          if (_hdr_fill == 10) {
            if (_hdr_buf[0] != 0x1f || _hdr_buf[1] != 0x8b) {
              strcpy(err, "bad gzip magic");
              return false;
            }
            if (_hdr_buf[2] != 8) {  // CM must be deflate
              strcpy(err, "bad gzip method");
              return false;
            }
            _hdr_flags = _hdr_buf[3];
            _hdr_fill = 0;
            _hdr_state = (_hdr_flags & FLG_FEXTRA) ? HDR_XLEN : afterExtra();
          }
          break;

        case HDR_XLEN:
          _hdr_buf[_hdr_fill++] = b;
          data++; len--;
          if (_hdr_fill == 2) {
            _extra_remaining = (uint16_t) _hdr_buf[0] | ((uint16_t) _hdr_buf[1] << 8);
            _hdr_fill = 0;
            _hdr_state = _extra_remaining > 0 ? HDR_EXTRA : afterExtra();
          }
          break;

        case HDR_EXTRA: {
          size_t n = len < _extra_remaining ? len : _extra_remaining;
          data += n; len -= n;
          _extra_remaining -= n;
          if (_extra_remaining == 0) _hdr_state = afterExtra();
          break;
        }

        case HDR_NAME:
          data++; len--;
          if (b == 0) _hdr_state = afterName();
          break;

        case HDR_COMMENT:
          data++; len--;
          if (b == 0) _hdr_state = afterComment();
          break;

        case HDR_HCRC:
          _hdr_fill++;
          data++; len--;
          if (_hdr_fill == 2) {
            _hdr_fill = 0;
            _hdr_state = HDR_DONE;
          }
          break;

        default:
          break;
      }
    }

    *pdata = data;
    *plen = len;
    return true;
  }

  uint8_t afterExtra() const { return (_hdr_flags & FLG_FNAME) ? HDR_NAME : afterName(); }
  uint8_t afterName() const { return (_hdr_flags & FLG_FCOMMENT) ? HDR_COMMENT : afterComment(); }
  uint8_t afterComment() const { return (_hdr_flags & FLG_FHCRC) ? HDR_HCRC : HDR_DONE; }
};

#endif  // ESP32_OTA_GZIP

ESP32OTAController::ESP32OTAController() {
  _stage = NULL;
  _stage_size = 0;
  _stage_base = 0;
  _stage_range_count = 0;
  _gz = NULL;
  clearState();
}

void ESP32OTAController::clearState() {
  _armed = false;
  _active = false;
  _update_started = false;
  _gz_mode = false;
  _md5[0] = 0;
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
#if ESP32_OTA_GZIP
  if (_gz) {
    delete _gz;
    _gz = NULL;
  }
#endif
}

// Size of the staging block currently being received (the last block of the
// transfer can be shorter than the buffer).
size_t ESP32OTAController::stageExtent() const {
  if (_stage_size == 0 || _expected_size <= _stage_base) return 0;
  size_t left = _expected_size - _stage_base;
  return left < _stage_size ? left : _stage_size;
}

// Contiguous bytes received from the start of the current block.
size_t ESP32OTAController::stagePrefix() const {
  if (_stage_range_count == 0 || _stage_ranges[0].start != 0) return 0;
  return _stage_ranges[0].end;
}

// Insert [start,end) into the sorted disjoint range set, merging overlapping
// and adjacent segments. Returns false only when a brand-new hole-bounded
// segment would overflow the table (the caller then degrades to
// sequential-only acceptance; strictly in-order streams always merge into
// ranges[0] and cannot fail).
bool ESP32OTAController::insertStageRange(size_t start, size_t end) {
  int i = 0;
  while (i < _stage_range_count && _stage_ranges[i].end < start) {
    i++;
  }

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
    if (_stage_ranges[j].end > _stage_ranges[i].end) {
      _stage_ranges[i].end = _stage_ranges[j].end;
    }
    j++;
  }
  if (j > i + 1) {
    memmove(&_stage_ranges[i + 1], &_stage_ranges[j],
            (_stage_range_count - j) * sizeof(StageRange));
    _stage_range_count -= (uint8_t)(j - i - 1);
  }
  return true;
}

// Append ' miss=<abs>+<len>,...' for the holes BETWEEN received segments of
// the current block (the tail past the last segment is data the client simply
// hasn't sent yet, not a loss). Old clients ignore the suffix: their status
// parser only reads the leading done/total pair.
void ESP32OTAController::appendMissList(char reply[], size_t max_len) const {
  if (_stage_range_count == 0) return;

  size_t len = strlen(reply);
  int added = 0;
  size_t hole_start = 0;  // block-relative
  for (int k = 0; k <= _stage_range_count - 1; k++) {
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
      added++;
      if (added >= 8) break;
    }
    hole_start = _stage_ranges[k].end;
  }
}

size_t ESP32OTAController::flushBlockBytes() const {
  return _stage_size > 0 ? _stage_size : OTA_UPDATER_INTERNAL_BLOCK;
}

bool ESP32OTAController::isSleepInhibited() const {
  return (_armed || _active) && !sessionExpired();
}

bool ESP32OTAController::sessionExpired() const {
  if (!_armed && !_active) return false;
  return (uint32_t)(millis() - _last_activity_millis) > ESP32_OTA_SESSION_TIMEOUT_MS;
}

void ESP32OTAController::expireIfIdle() {
  if (!sessionExpired()) return;
  OTA_DEBUG_PRINTLN("session idle > %lu ms, auto-abort", (unsigned long) ESP32_OTA_SESSION_TIMEOUT_MS);
  abortUpdate();
  clearState();
}

void ESP32OTAController::abortUpdate() {
  // Unlike the RP2040 Updater, arduino-esp32 has a proper abort: it discards
  // the partially written partition without touching the boot selection.
  if (Update.isRunning()) {
    Update.abort();
  }
  Update.clearError();
}

const char *ESP32OTAController::skipSpaces(const char *p) {
  while (*p == ' ') {
    p++;
  }
  return p;
}

bool ESP32OTAController::isValidHexChar(char c) {
  return (c >= '0' && c <= '9') ||
         (c >= 'a' && c <= 'f') ||
         (c >= 'A' && c <= 'F');
}

bool ESP32OTAController::decodeHex(const char *hex, size_t hex_len, uint8_t *out, size_t max_out, size_t *out_len) {
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

bool ESP32OTAController::startSession(const char *id, char reply[]) {
  OTA_DEBUG_PRINTLN("start requested by %s", id ? id : "?");
  expireIfIdle();

  if (_active || Update.isRunning()) {
    strcpy(reply, "Err - OTA already running");
    OTA_DEBUG_PRINTLN("rejected: OTA already running");
    return true;
  }

  const esp_partition_t *part = esp_ota_get_next_update_partition(NULL);
  if (part == NULL) {
    strcpy(reply, "Err - no OTA partition");
    OTA_DEBUG_PRINTLN("no OTA partition available");
    clearState();
    return true;
  }

  clearState();
  _armed = true;
  _last_activity_millis = millis();

  _stage = (uint8_t *) malloc(ESP32_OTA_STAGE_BUFFER_BYTES);
  _stage_size = _stage ? ESP32_OTA_STAGE_BUFFER_BYTES : 0;
  _stage_base = 0;
  _stage_range_count = 0;
  if (_stage_size == 0) {
    OTA_DEBUG_PRINTLN("stage buffer alloc failed, falling back to %u", OTA_UPDATER_INTERNAL_BLOCK);
  }

  uint32_t free_kb = (uint32_t)(part->size / 1024UL);
  sprintf(reply, "OK - OTA ready (%lukB free, blk=%u, gz=%d, nack=miss)",
          (unsigned long) free_kb, (unsigned int) flushBlockBytes(),
          ESP32_OTA_GZIP ? 1 : 0);
  OTA_DEBUG_PRINTLN("session armed, part=%s free=%lu kB, blk=%u, gz=%d",
                    part->label, (unsigned long) free_kb,
                    (unsigned int) flushBlockBytes(), ESP32_OTA_GZIP ? 1 : 0);
  return true;
}

bool ESP32OTAController::handleCommand(const char *command, char reply[]) {
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
      // Capability marker lets the web client distinguish an old target from
      // a selective-NACK target that simply has no holes to report.
      // NACK sélectif : annonce les trous du bloc courant pour que le client
      // ne réémette qu'eux. Suffixe ignoré par les anciens clients (leur
      // parseur ne lit que done/total).
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
    if (_active || Update.isRunning()) {
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

    // The received stream can be a raw app image or a gzip stream, and the
    // two need different Update.begin() setups (exact size + framework MD5
    // vs unknown size + own MD5 over compressed bytes). The mode is only
    // known once the first bytes arrive, so Update.begin() is deferred to
    // the first write; here we only sanity-check capacity.
    const esp_partition_t *part = esp_ota_get_next_update_partition(NULL);
    if (part == NULL) {
      OTA_DEBUG_PRINTLN("begin failed: no OTA partition");
      strcpy(reply, "Err - no OTA partition");
      clearState();
      return true;
    }
    if ((size_t) size_ul > part->size) {
      OTA_DEBUG_PRINTLN("begin failed: image larger than partition (%lu > %lu)",
                        size_ul, (unsigned long) part->size);
      strcpy(reply, "Err - image too big");
      return true;
    }

    _active = true;
    _update_started = false;
    _gz_mode = false;
    memcpy(_md5, md5, sizeof(_md5));
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
    if (!_active) {
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
    if (!_active) {
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

    if (!_update_started) {
      OTA_DEBUG_PRINTLN("end rejected: nothing written");
      strcpy(reply, "Err - OTA not started");
      return true;
    }

#if ESP32_OTA_GZIP
    if (_gz_mode) {
      if (!_gz->isDone()) {
        OTA_DEBUG_PRINTLN("end failed: gzip stream truncated");
        abortUpdate();
        clearState();
        strcpy(reply, "Err - gzip stream truncated");
        return true;
      }
      if (_md5[0]) {
        _gz->md5.calculate();
        if (strcasecmp(_gz->md5.toString().c_str(), _md5) != 0) {
          OTA_DEBUG_PRINTLN("end failed: MD5 mismatch");
          abortUpdate();
          clearState();
          strcpy(reply, "Err - MD5 mismatch");
          return true;
        }
      }
      if (!_gz->checkFooterSize()) {
        OTA_DEBUG_PRINTLN("end failed: gzip ISIZE mismatch (out=%lu)",
                          (unsigned long) _gz->totalOut());
        abortUpdate();
        clearState();
        strcpy(reply, "Err - gzip size mismatch");
        return true;
      }
      OTA_DEBUG_PRINTLN("gzip stream complete, %lu -> %lu bytes",
                        (unsigned long) _expected_size, (unsigned long) _gz->totalOut());
      // Update.begin() ran with unknown size (decompressed size is only in
      // the gzip footer), so end(true) is required to truncate to what was
      // written. Safe here: the deflate stream is complete and MD5-verified.
      if (!Update.end(true)) {
        sprintf(reply, "Err - end failed (%u)", (uint32_t) Update.getError());
        OTA_DEBUG_PRINTLN("end failed, err=%u", (uint32_t) Update.getError());
        abortUpdate();
        clearState();
        return true;
      }
    } else
#endif
    {
      if (!Update.end(false)) {
        sprintf(reply, "Err - end failed (%u)", (uint32_t) Update.getError());
        OTA_DEBUG_PRINTLN("end failed, err=%u", (uint32_t) Update.getError());
        abortUpdate();
        clearState();
        return true;
      }
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

// Decide raw vs gzip from the first received bytes and set the Update
// library up accordingly. A raw ESP32 app image starts with the 0xE9 image
// magic, a gzip stream with 1f 8b — no ambiguity.
bool ESP32OTAController::beginUpdater(const uint8_t *first_bytes, size_t first_len, char reply[]) {
  bool is_gzip = first_len >= 2 && first_bytes[0] == 0x1f && first_bytes[1] == 0x8b;

  if (is_gzip) {
#if ESP32_OTA_GZIP
    _gz = new (std::nothrow) ESP32OTAGzInflater();
    if (_gz == NULL) {
      OTA_DEBUG_PRINTLN("begin failed: gzip state alloc failed");
      clearState();
      strcpy(reply, "Err - no mem for gzip");
      return false;
    }
    if (!Update.begin(UPDATE_SIZE_UNKNOWN, U_FLASH)) {
      sprintf(reply, "Err - begin failed (%u)", (uint32_t) Update.getError());
      OTA_DEBUG_PRINTLN("Update.begin failed, err=%u", (uint32_t) Update.getError());
      clearState();
      return false;
    }
    _gz_mode = true;
    OTA_DEBUG_PRINTLN("gzip stream detected, inflating on the fly");
#else
    OTA_DEBUG_PRINTLN("begin failed: gzip stream but no gzip support");
    clearState();
    strcpy(reply, "Err - gzip unsupported");
    return false;
#endif
  } else {
    if (!Update.begin(_expected_size, U_FLASH)) {
      sprintf(reply, "Err - begin failed (%u)", (uint32_t) Update.getError());
      OTA_DEBUG_PRINTLN("Update.begin failed, err=%u", (uint32_t) Update.getError());
      clearState();
      return false;
    }
    if (_md5[0] && !Update.setMD5(_md5)) {
      abortUpdate();
      clearState();
      OTA_DEBUG_PRINTLN("begin failed: invalid md5 format");
      strcpy(reply, "Err - invalid MD5");
      return false;
    }
  }

  _update_started = true;
  return true;
}

bool ESP32OTAController::writeChunk(size_t offset, const uint8_t *chunk, size_t chunk_len, char reply[]) {
  if (!_active) {
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
    // Out-of-order acceptance within the current staging block: the stage is
    // random-access RAM, so a chunk landing past a lost one is stored at its
    // place instead of being rejected (holes are advertised via 'miss=' in
    // the status reply). Duplicates and rewound resends from legacy clients
    // simply overwrite the same bytes. Data leaves the stage — towards
    // Update/gzip, which are strictly sequential — only as complete blocks.
    //
    // Externally the legacy contract is preserved: any non-contiguous chunk
    // still gets the 'Err - offset' reply (see below) so sequential clients
    // that rely on it keep rewinding exactly as before — they just find the
    // ahead-data already accepted, which shortens their rewind rounds. The
    // binary transport suppresses per-write replies either way.
    const uint8_t *src = chunk;
    size_t off = offset;
    size_t remaining = chunk_len;
    bool consumed_any = false;

    while (remaining > 0) {
      if (off < _stage_base) {  // duplicate of already-flushed data: idempotent
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
          // Entirely past the receive window: same reject as the legacy
          // sequential rule, so old clients rewind exactly as before.
          OTA_DEBUG_PRINTLN("write rejected: bad offset %lu expected %lu",
                            (unsigned long) off, (unsigned long) _received_size);
          sprintf(reply, "Err - offset %lu expected %lu",
                  (unsigned long) off, (unsigned long) _received_size);
          return true;
        }
        // Straddler tail into a block that can't flush yet (holes remain):
        // drop it, the contiguous 'done' will make the client resend it.
        OTA_DEBUG_PRINTLN("write tail dropped at %lu (window end %lu)",
                          (unsigned long) off, (unsigned long) win_end);
        break;
      }

      size_t n = win_end - off;
      if (n > remaining) n = remaining;
      size_t rel = off - _stage_base;
      if (!insertStageRange(rel, rel + n)) {
        // Range table full: degrade to sequential-only for this chunk.
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
    // No staging buffer (alloc failed): strict sequential mode, as before.
    if (!was_contiguous) {
      OTA_DEBUG_PRINTLN("write rejected: bad offset %lu expected %lu",
                        (unsigned long) offset,
                        (unsigned long) _received_size);
      sprintf(reply, "Err - offset %lu expected %lu", (unsigned long) offset, (unsigned long) _received_size);
      return true;
    }
    if (!writePayload(chunk, chunk_len, reply)) {
      return true;
    }
    _received_size += chunk_len;
  }

  if (_received_size >= _next_progress_log || _received_size == _expected_size) {
    OTA_DEBUG_PRINTLN("progress %lu/%lu",
                      (unsigned long) _received_size,
                      (unsigned long) _expected_size);
    _next_progress_log = _received_size + OTA_PROGRESS_LOG_STEP_BYTES;
  }

  if (!was_contiguous) {
    // Accepted into the block, but not contiguous: reply the legacy offset
    // error so rewind-based clients resync as they always did (suppressed in
    // binary transport, where recovery is 'miss='-driven instead).
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

bool ESP32OTAController::writePayload(const uint8_t *data, size_t len, char reply[]) {
  if (!_update_started && !beginUpdater(data, len, reply)) {
    return false;
  }

#if ESP32_OTA_GZIP
  if (_gz_mode) {
    char cause[48];
    cause[0] = 0;
    if (!_gz->feed(data, len, cause)) {
      abortUpdate();
      sprintf(reply, "Err - %s", cause[0] ? cause : "gzip failed");
      OTA_DEBUG_PRINTLN("gzip feed failed at %lu: %s", (unsigned long) _received_size, cause);
      clearState();
      return false;
    }
    return true;
  }
#endif

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

bool ESP32OTAController::flushStage(char reply[]) {
  size_t extent = stageExtent();
  if (extent == 0 || stagePrefix() < extent) {
    return true;  // block incomplete (holes or nothing staged): nothing to flush
  }
  if (!writePayload(_stage, extent, reply)) {
    return false;  // fatal, state cleared by writePayload
  }
  _stage_base += extent;
  _stage_range_count = 0;
  _received_size = _stage_base;
  return true;
}

bool ESP32OTAController::handleBinaryCommand(uint8_t opcode, const uint8_t *payload, size_t payload_len, char reply[]) {
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
