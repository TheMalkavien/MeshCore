#pragma once

#include "../simple_sensor/SensorMesh.h"

#ifndef TRACKER_DEFAULT_INTERVAL_SECS
  #define TRACKER_DEFAULT_INTERVAL_SECS 1800
#endif

#ifndef TRACKER_DEFAULT_GPS_TIMEOUT_SECS
  #define TRACKER_DEFAULT_GPS_TIMEOUT_SECS 180
#endif

#ifndef TRACKER_GPS_TIMEOUT_MAX_SECS
  #define TRACKER_GPS_TIMEOUT_MAX_SECS 900
#endif

#ifndef TRACKER_DEFAULT_REQUIRE_LIVE_FIX
  #define TRACKER_DEFAULT_REQUIRE_LIVE_FIX 1
#endif

#ifndef TRACKER_DEFAULT_MIN_SATS
  #define TRACKER_DEFAULT_MIN_SATS 4
#endif

#ifndef TRACKER_DEFAULT_MIN_FIX_AGE_SECS
  #define TRACKER_DEFAULT_MIN_FIX_AGE_SECS 0
#endif

#ifndef TRACKER_DEFAULT_GROUP_NAME
  #define TRACKER_DEFAULT_GROUP_NAME "tracker"
#endif

#ifndef TRACKER_DEFAULT_GROUP_PSK
  #define TRACKER_DEFAULT_GROUP_PSK "izOH6cXN6mrJ5e26oRXNcg=="
#endif

#ifndef TRACKER_DEFAULT_SLEEP_ENABLED
  #define TRACKER_DEFAULT_SLEEP_ENABLED 0
#endif

#ifndef TRACKER_SLEEP_RADIO_POWEROFF
  #if defined(T1000_E)
    // T1000-E: LR1110 deep sleep can interact badly with GNSS behaviour on some units.
    // Keep radio awake by default during SYSTEM_ON sleep windows unless explicitly enabled.
    #define TRACKER_SLEEP_RADIO_POWEROFF 0
  #else
    #define TRACKER_SLEEP_RADIO_POWEROFF 1
  #endif
#endif

#ifndef TRACKER_BOOT_GRACE_SECS
  #define TRACKER_BOOT_GRACE_SECS 3
#endif

#ifndef TRACKER_SLEEP_DRAIN_RECHECK_MS
  #define TRACKER_SLEEP_DRAIN_RECHECK_MS 1500
#endif

#ifndef TRACKER_SLEEP_FLUSH_TIMEOUT_SECS
  #define TRACKER_SLEEP_FLUSH_TIMEOUT_SECS 12
#endif

#ifndef TRACKER_SERIAL_DEBUG
  #define TRACKER_SERIAL_DEBUG 1
#endif

#ifndef TRACKER_CONFIG_PATH
  #define TRACKER_CONFIG_PATH "/tracker.cfg"
#endif

#ifndef TRACKER_GROUP_TEXT_BUFFER
  #define TRACKER_GROUP_TEXT_BUFFER 176
#endif

// Group text payload must fit Mesh::createGroupDatagram() constraint:
// data_len <= MAX_PACKET_PAYLOAD - CIPHER_BLOCK_SIZE, and our data_len is 5 + text_len.
#ifndef TRACKER_GROUP_TEXT_MAX_LEN
  #if defined(MAX_PACKET_PAYLOAD) && defined(CIPHER_BLOCK_SIZE)
    #define TRACKER_GROUP_TEXT_MAX_LEN (MAX_PACKET_PAYLOAD - CIPHER_BLOCK_SIZE - 5)
  #else
    #define TRACKER_GROUP_TEXT_MAX_LEN 163
  #endif
#endif

#ifndef TRACKER_POWER_BUTTON_HOLD_MS
  #define TRACKER_POWER_BUTTON_HOLD_MS 3000
#endif

#ifndef TRACKER_POWER_BUTTON_SHORT_MIN_MS
  #define TRACKER_POWER_BUTTON_SHORT_MIN_MS 40
#endif

#ifndef TRACKER_TX_LED_PULSE_MS
  #define TRACKER_TX_LED_PULSE_MS 80
#endif

#ifndef TRACKER_BOOT_LED_PULSE_MS
  #define TRACKER_BOOT_LED_PULSE_MS 150
#endif

#ifndef TRACKER_BUTTON_LED_PULSE_MS
  #define TRACKER_BUTTON_LED_PULSE_MS 80
#endif

#ifndef TRACKER_EXT_POWER_LED_PERIOD_MS
  #define TRACKER_EXT_POWER_LED_PERIOD_MS 5000
#endif

#ifndef TRACKER_EXT_POWER_LED_ON_MS
  #define TRACKER_EXT_POWER_LED_ON_MS 120
#endif

#ifndef TRACKER_EXT_POWER_LED_GAP_MS
  #define TRACKER_EXT_POWER_LED_GAP_MS 180
#endif

#ifndef TRACKER_EXT_POWER_DEBUG_LOG_MS
  #define TRACKER_EXT_POWER_DEBUG_LOG_MS 5000
#endif

#ifndef TRACKER_BUZZER_NOTE_MS
  #define TRACKER_BUZZER_NOTE_MS 95
#endif

#ifndef TRACKER_BUZZER_NOTE_GAP_MS
  #define TRACKER_BUZZER_NOTE_GAP_MS 35
#endif

#ifndef BATT_MIN_MILLIVOLTS
  #define BATT_MIN_MILLIVOLTS 3000
#endif

#ifndef BATT_MAX_MILLIVOLTS
  #define BATT_MAX_MILLIVOLTS 4200
#endif

#if TRACKER_SERIAL_DEBUG == 1
  static inline bool trackerDebugSerialReady() {
    if (!Serial) {
      return false;
    }
    return Serial.availableForWrite() >= 16;
  }
  #define TRACKER_DBG(fmt, ...) do { \
    if (trackerDebugSerialReady()) { \
      Serial.printf("[tracker %10lu] " fmt "\r\n", millis(), ##__VA_ARGS__); \
    } \
  } while (0)
#else
  #define TRACKER_DBG(...) do { } while (0)
#endif
