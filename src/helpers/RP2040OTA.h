#pragma once

#if defined(RP2040_PLATFORM)

#include <stddef.h>
#include <stdint.h>

class RP2040OTAController {
public:
  enum BinaryOpcode : uint8_t {
    BIN_OP_START  = 1,
    BIN_OP_BEGIN  = 2,
    BIN_OP_WRITE  = 3,
    BIN_OP_END    = 4,
    BIN_OP_STATUS = 5,
    BIN_OP_ABORT  = 6
  };

  RP2040OTAController();

  bool startSession(const char *id, char reply[]);
  bool handleCommand(const char *command, char reply[]);
  bool handleBinaryCommand(uint8_t opcode, const uint8_t *payload, size_t payload_len, char reply[]);
  bool isSleepInhibited() const;

private:
  struct StageRange {
    uint16_t start;
    uint16_t end;
  };
  static const int MAX_STAGE_RANGES = 24;

  bool _armed;
  bool _active;
  size_t _expected_size;
  size_t _received_size;  // contiguous prefix received
  size_t _next_progress_log;
  uint16_t _ack_every_chunks;
  uint16_t _chunks_since_ack;
  uint32_t _last_activity_millis;
  uint8_t *_stage;
  size_t _stage_size;
  size_t _stage_base;
  StageRange _stage_ranges[MAX_STAGE_RANGES];
  uint8_t _stage_range_count;

  static const char *skipSpaces(const char *p);
  static bool decodeHex(const char *hex, size_t hex_len, uint8_t *out, size_t max_out, size_t *out_len);
  static bool isValidHexChar(char c);
  bool writeChunk(size_t offset, const uint8_t *chunk, size_t chunk_len, char reply[]);
  bool writeToUpdater(const uint8_t *data, size_t len, char reply[]);
  bool flushStage(char reply[]);
  size_t flushBlockBytes() const;
  size_t stageExtent() const;
  size_t stagePrefix() const;
  bool insertStageRange(size_t start, size_t end);
  void appendMissList(char reply[], size_t max_len) const;
  void abortUpdate();
  bool sessionExpired() const;
  void expireIfIdle();
  void clearState();
};

#endif
