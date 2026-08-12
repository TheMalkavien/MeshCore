#pragma once

#include <Arduino.h>

namespace meshcore {
namespace rxps {

constexpr uint32_t DEFAULT_RX_US = 65625UL;
constexpr uint32_t DEFAULT_SLEEP_US = 60000UL;
constexpr uint32_t MIN_PERIOD_US = 1000UL;
constexpr uint32_t MAX_PERIOD_US = 30000000UL;

// Level 1 is the safest profile (longest listen window, shortest sleep).
// Increasing the level progressively shortens RX and lengthens sleep, so
// level 10 saves the most power and has the highest packet-loss risk.
constexpr uint8_t MIN_LEVEL = 1;
constexpr uint8_t MAX_LEVEL = 10;
constexpr uint8_t BALANCED_LEVEL = 5;
constexpr uint8_t BALANCED_PREAMBLE = 16;

inline bool isValidPeriod(uint32_t us) {
  return us >= MIN_PERIOD_US && us <= MAX_PERIOD_US;
}

inline uint8_t transmittedPreambleForSF(uint8_t sf) {
  return sf <= 8 ? 32 : 16;
}

inline uint32_t ceilPositiveFloat(float value) {
  const uint32_t rounded = (uint32_t)value;
  return value > (float)rounded ? rounded + 1 : rounded;
}

// Convert a user-facing 1..10 level to SX126x RxDutyCycle timings. The sender
// preamble is explicit because interoperability depends on the preamble used
// by peers, not just the local radio's transmit setting.
inline bool calculateLevel(uint8_t level, uint8_t sf, float bw, uint8_t sender_preamble,
                           uint32_t *rx_us, uint32_t *sleep_us) {
  if (!rx_us || !sleep_us || level < MIN_LEVEL || level > MAX_LEVEL || sf < 5 || sf > 12 ||
      bw <= 0.0f || (sender_preamble != 16 && sender_preamble != 32)) {
    return false;
  }

  const float symbol_us = (1000.0f * (float)(1UL << sf)) / bw;
  const float amount = (float)(level - MIN_LEVEL) / (float)(MAX_LEVEL - MIN_LEVEL);
  const float rx_start_symbols = sender_preamble == 16 ? 12.0f : 16.0f;
  const float sleep_start_symbols = sender_preamble == 16 ? 2.0f : 15.0f;
  constexpr float rx_edge_symbols = 8.0f;
  const float sleep_edge_symbols = (float)sender_preamble + 4.25f - rx_edge_symbols;

  const float rx_symbols = rx_start_symbols + amount * (rx_edge_symbols - rx_start_symbols);
  const float sleep_symbols = sleep_start_symbols + amount * (sleep_edge_symbols - sleep_start_symbols);

  *rx_us = ceilPositiveFloat(rx_symbols * symbol_us);
  *sleep_us = (uint32_t)(sleep_symbols * symbol_us);
  return isValidPeriod(*rx_us) && isValidPeriod(*sleep_us);
}

} // namespace rxps
} // namespace meshcore
