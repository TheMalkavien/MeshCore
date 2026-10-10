#include <initializer_list>
#include "RXPowerSaving.h"
#include <assert.h>
#include <cmath>
#include <cstdio>
#include <limits>

int main() {
  unsigned accepted = 0, unavailable = 0;
  const float bandwidths[] = {7.8f, 10.4f, 15.6f, 20.8f, 31.25f, 41.7f, 62.5f, 125.f, 250.f, 500.f};
  for (uint8_t sf = 5; sf <= 12; ++sf)
    for (float bw : bandwidths)
      for (uint8_t preamble : {16, 32})
        for (uint8_t level = 1; level <= 8; ++level)
          for (uint32_t transition : {1000u, 2600u, 6000u})
            for (float capture : {6.f, 8.f}) {
              uint32_t rx = 0, sleep = 0;
              if (!calcRxPowerSavingLevel(level, sf, bw, preamble, &rx, &sleep, capture, transition)) {
                ++unavailable;
                continue;
              }
              ++accepted;
              assert(isValidRxPowerSavingPeriod(rx) && isValidRxPowerSavingPeriod(sleep));
              assert(sleep >= transition + RX_POWERSAVING_MIN_SLEEP_MARGIN_US);
              const double symbol = 1000.0 * (1u << sf) / bw;
              const double need = (preamble + (sf <= 6 ? 6.25 : 4.25) + 8.0) * symbol;
              // Validate the actual SX126x registers, independently of requested microseconds.
              const double actual_rx = (uint64_t(rx) * 8 / 125) * 15.625;
              const double actual_sleep = (uint64_t(sleep - transition) * 8 / 125) * 15.625;
              assert(2 * actual_rx + actual_sleep >= need);
              assert(preamble * symbol - sleep + 15.625 >= capture * symbol);
            }
  uint32_t rx = 0, sleep = 0;
  assert(!calcRxPowerSavingLevel(0, 8, 62.5f, 16, &rx, &sleep));
  assert(!calcRxPowerSavingLevel(11, 8, 62.5f, 16, &rx, &sleep));
  assert(!calcRxPowerSavingLevel(6, 8, 62.5f, 17, &rx, &sleep));
  assert(!calcRxPowerSavingLevel(6, 8, 62.5f, 16, nullptr, &sleep));
  assert(!calcRxPowerSavingLevel(6, 8, std::numeric_limits<float>::quiet_NaN(), 16, &rx, &sleep));
  assert(!calcRxPowerSavingLevel(6, 8, std::numeric_limits<float>::infinity(), 16, &rx, &sleep));
  assert(!calcRxPowerSavingLevel(6, 8, 0.001f, 16, &rx, &sleep));
  assert(!calcRxPowerSavingLevel(6, 8, 62.5f, 16, &rx, &sleep, -1));
  assert(calcRxPowerSavingLevel(6, 8, 62.5f, 16, &rx, &sleep));
  assert(rx == 49329 && sleep == 23757);
  RxPowerSavingArmRetryState retry;
  assert(retry.canAttempt());
  retry.recordFailure(); retry.recordFailure(); retry.recordFailure();
  assert(!retry.canAttempt());
  retry.reset(); assert(retry.canAttempt());
  retry.recordFailure(); retry.recordSuccess(); assert(retry.canAttempt());
  RxPowerSavingConfig config;
  config.enabled = 1;
  normalizeRxPowerSavingConfig(&config, 5, 500.f, 8.f, 6000);
  assert(config.enabled == 0);  // cannot arm; stale SF8 timings must not be reused
  config.level = 0; config.enabled = 1; config.rx_us = 12345; config.sleep_us = 12000;
  normalizeRxPowerSavingConfig(&config, 8, 62.5f);
  assert(config.enabled == 1 && config.rx_us == 12345 && config.sleep_us == 12000);
  std::printf("RXPS: %u guarded geometries checked, %u unavailable; invalid inputs and retries passed\n", accepted, unavailable);
}
