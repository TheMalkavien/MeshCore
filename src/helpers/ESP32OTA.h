#pragma once

#if defined(ESP_PLATFORM)

#include <stddef.h>
#include <stdint.h>

class ESP32OTAGzInflater;

// Mesh OTA controller for ESP32 targets. Speaks the exact same text and
// binary command protocol as RP2040OTAController (start/begin/write/end/
// status/abort), so the web OTA tool drives both platforms identically.
// Differences from the RP2040 implementation:
//  - firmware bytes go straight to the inactive OTA app partition via the
//    arduino-esp32 Update library (no LittleFS staging file), and the boot
//    partition switch only happens after a fully verified 'end'
//  - gzip streams are inflated on the fly using the ROM tinfl decompressor
//    (when available for the SoC), with MD5 checked over the received
//    compressed stream
class ESP32OTAController {
public:
  enum BinaryOpcode : uint8_t {
    BIN_OP_START  = 1,
    BIN_OP_BEGIN  = 2,
    BIN_OP_WRITE  = 3,
    BIN_OP_END    = 4,
    BIN_OP_STATUS = 5,
    BIN_OP_ABORT  = 6
  };

  ESP32OTAController();

  bool startSession(const char *id, char reply[]);
  bool handleCommand(const char *command, char reply[]);
  bool handleBinaryCommand(uint8_t opcode, const uint8_t *payload, size_t payload_len, char reply[]);
  bool isSleepInhibited() const;

private:
  bool _armed;
  bool _active;
  bool _update_started;   // Update.begin() done (deferred to the first write, see beginUpdater)
  bool _gz_mode;
  char _md5[33];          // expected MD5 of the received byte stream ("" = none)
  size_t _expected_size;
  size_t _received_size;
  size_t _next_progress_log;
  uint16_t _ack_every_chunks;
  uint16_t _chunks_since_ack;
  uint32_t _last_activity_millis;
  uint8_t *_stage;
  size_t _stage_size;
  size_t _stage_fill;
  ESP32OTAGzInflater *_gz;

  static const char *skipSpaces(const char *p);
  static bool decodeHex(const char *hex, size_t hex_len, uint8_t *out, size_t max_out, size_t *out_len);
  static bool isValidHexChar(char c);
  bool beginUpdater(const uint8_t *first_bytes, size_t first_len, char reply[]);
  bool writeChunk(size_t offset, const uint8_t *chunk, size_t chunk_len, char reply[]);
  bool writePayload(const uint8_t *data, size_t len, char reply[]);
  bool flushStage(char reply[]);
  size_t flushBlockBytes() const;
  void abortUpdate();
  bool sessionExpired() const;
  void expireIfIdle();
  void clearState();
};

#endif
