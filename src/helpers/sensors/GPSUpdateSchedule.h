#pragma once

#include <stdint.h>

// Small, Arduino-independent deadline helper for scheduled GNSS fixes.
// Deadlines use the usual signed-delta comparison, so they remain valid when
// millis() wraps as long as an interval stays below half the uint32_t range.
class GPSUpdateSchedule {
  bool _scheduled = false;
  uint32_t _deadline_ms = 0;

public:
  static bool timeReached(uint32_t now_ms, uint32_t target_ms) {
    return static_cast<int32_t>(now_ms - target_ms) >= 0;
  }

  void scheduleNow(uint32_t now_ms) {
    _deadline_ms = now_ms;
    _scheduled = true;
  }

  void scheduleAfter(uint32_t now_ms, uint32_t interval_sec) {
    if (interval_sec == 0) {
      cancel();
      return;
    }

    _deadline_ms = now_ms + (interval_sec * 1000UL);
    _scheduled = true;
  }

  void cancel() {
    _scheduled = false;
  }

  bool isScheduled() const {
    return _scheduled;
  }

  bool isDue(uint32_t now_ms) const {
    return _scheduled && timeReached(now_ms, _deadline_ms);
  }

  void consume() {
    _scheduled = false;
  }

  uint32_t deadline() const {
    return _deadline_ms;
  }
};
