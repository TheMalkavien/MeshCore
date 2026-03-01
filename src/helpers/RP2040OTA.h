#pragma once

#if defined(RP2040_PLATFORM)

#include <stddef.h>
#include <stdint.h>

class RP2040OTAController {
public:
  RP2040OTAController();

  bool startSession(const char *id, char reply[]);
  bool handleCommand(const char *command, char reply[]);

private:
  bool _armed;
  bool _active;
  size_t _expected_size;
  size_t _received_size;
  size_t _next_progress_log;
  uint16_t _ack_every_chunks;
  uint16_t _chunks_since_ack;

  static const char *skipSpaces(const char *p);
  static bool decodeHex(const char *hex, size_t hex_len, uint8_t *out, size_t max_out, size_t *out_len);
  static bool isValidHexChar(char c);
  void clearState();
};

#endif
