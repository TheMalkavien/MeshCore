#include <Arduino.h>
#include "../simple_sensor/SensorMesh.h"
#include <base64.hpp>
#include <limits.h>
#include <math.h>
#if defined(ESP32)
  #include <esp_sleep.h>
  #include <driver/rtc_io.h>
#endif
#if defined(NRF52_PLATFORM)
  #include <nrf_soc.h>
  #include <nrf.h>
#endif
#if defined(T1000_E)
extern float t1000e_get_temperature(void);
extern uint32_t t1000e_get_light(void);
#endif

#ifdef DISPLAY_CLASS
  #include "../simple_sensor/UITask.h"
  static UITask ui_task(display);
#endif

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

#ifndef BATT_MIN_MILLIVOLTS
  #define BATT_MIN_MILLIVOLTS 3000
#endif

#ifndef BATT_MAX_MILLIVOLTS
  #define BATT_MAX_MILLIVOLTS 4200
#endif

#if TRACKER_SERIAL_DEBUG == 1
  static bool trackerDebugSerialReady() {
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

static int batteryPercentFromMilliVolts(uint16_t batteryMilliVolts) {
  const int minMilliVolts = BATT_MIN_MILLIVOLTS;
  const int maxMilliVolts = BATT_MAX_MILLIVOLTS;
  int batteryPercentage = ((batteryMilliVolts - minMilliVolts) * 100) / (maxMilliVolts - minMilliVolts);
  if (batteryPercentage < 0) batteryPercentage = 0;
  if (batteryPercentage > 100) batteryPercentage = 100;
  return batteryPercentage;
}

template <typename T>
static auto enterBoardDeepSleep(T& b, uint32_t secs, int) -> decltype(b.enterDeepSleep(secs, 0), void()) {
#if defined(PIN_USER_BTN) && (PIN_USER_BTN >= 0)
  b.enterDeepSleep(secs, PIN_USER_BTN);  // wake by timer or user button
#elif defined(BUTTON_PIN) && (BUTTON_PIN >= 0)
  b.enterDeepSleep(secs, BUTTON_PIN);    // wake by timer or user button
#else
  b.enterDeepSleep(secs, -1);            // board default wake sources
#endif
}

template <typename T>
static auto enterBoardDeepSleep(T& b, uint32_t secs, long) -> decltype(b.enterDeepSleep(secs), void()) {
  b.enterDeepSleep(secs);
}

static void enterBoardDeepSleep(mesh::MainBoard& b, uint32_t secs, ...) {
  b.sleep(secs);
}

static void enterTimerOnlyDeepSleep(uint32_t secs) {
  enterBoardDeepSleep(board, secs, 0);
}

static unsigned long tracker_tx_led_until = 0;
static bool tracker_now_requested = false;

static void trackerSetStatusLed(bool on) {
#if defined(LED_PIN) && (LED_PIN >= 0)
  #ifdef LED_STATE_ON
  digitalWrite(LED_PIN, on ? LED_STATE_ON : ((LED_STATE_ON == HIGH) ? LOW : HIGH));
  #else
  digitalWrite(LED_PIN, on ? HIGH : LOW);
  #endif
#elif defined(P_LORA_TX_LED) && (P_LORA_TX_LED >= 0)
  #ifdef P_LORA_TX_LED_ON
  digitalWrite(P_LORA_TX_LED, on ? P_LORA_TX_LED_ON : ((P_LORA_TX_LED_ON == HIGH) ? LOW : HIGH));
  #else
  digitalWrite(P_LORA_TX_LED, on ? HIGH : LOW);
  #endif
#else
  (void)on;
#endif
}

static void trackerPulseStatusLed(uint16_t duration_ms) {
  trackerSetStatusLed(true);
  tracker_tx_led_until = millis() + duration_ms;
}

static void trackerPulseTxLed() {
  trackerPulseStatusLed(TRACKER_TX_LED_PULSE_MS);
}

static void trackerPulseButtonLed() {
  trackerPulseStatusLed(TRACKER_BUTTON_LED_PULSE_MS);
}

static void trackerFlashBootLed() {
  trackerSetStatusLed(true);
  delay(TRACKER_BOOT_LED_PULSE_MS);
  trackerSetStatusLed(false);
}

static void handleTrackerTxLedPulse() {
  if (tracker_tx_led_until != 0 && (int32_t)(millis() - tracker_tx_led_until) >= 0) {
    tracker_tx_led_until = 0;
    trackerSetStatusLed(false);
  }
}

static void trackerInitPowerButtonPin() {
#if defined(PIN_USER_BTN) && (PIN_USER_BTN >= 0)
  #ifdef USER_BTN_PRESSED
    #if USER_BTN_PRESSED == HIGH
  pinMode(PIN_USER_BTN, INPUT_PULLDOWN);
    #else
  pinMode(PIN_USER_BTN, INPUT_PULLUP);
    #endif
  #else
  pinMode(PIN_USER_BTN, INPUT_PULLUP);
  #endif
#elif defined(BUTTON_PIN) && (BUTTON_PIN >= 0)
  #ifdef USER_BTN_PRESSED
    #if USER_BTN_PRESSED == HIGH
  pinMode(BUTTON_PIN, INPUT_PULLDOWN);
    #else
  pinMode(BUTTON_PIN, INPUT_PULLUP);
    #endif
  #else
  pinMode(BUTTON_PIN, INPUT_PULLUP);
  #endif
#endif
}

static void trackerForceSystemOffIfSupported() {
#if defined(NRF52_PLATFORM)
  uint8_t sd_enabled = 0;
  if (sd_softdevice_is_enabled(&sd_enabled) == NRF_SUCCESS && sd_enabled) {
    uint32_t err = sd_power_system_off();
    if (err == NRF_SUCCESS) {
      return;  // should never return from SYSTEMOFF
    }
  }
  NRF_POWER->SYSTEMOFF = POWER_SYSTEMOFF_SYSTEMOFF_Enter;
  NVIC_SystemReset();
#endif
}

static bool trackerPowerButtonPressed() {
#if defined(PIN_USER_BTN) && (PIN_USER_BTN >= 0)
  int v = digitalRead(PIN_USER_BTN);
  #ifdef USER_BTN_PRESSED
  return v == USER_BTN_PRESSED;
  #else
  return v == LOW;
  #endif
#elif defined(BUTTON_PIN) && (BUTTON_PIN >= 0)
  int v = digitalRead(BUTTON_PIN);
  #ifdef USER_BTN_PRESSED
  return v == USER_BTN_PRESSED;
  #else
  return v == LOW;
  #endif
#else
  return false;
#endif
}

static void handleTrackerPowerButton() {
#if (defined(PIN_USER_BTN) && (PIN_USER_BTN >= 0)) || (defined(BUTTON_PIN) && (BUTTON_PIN >= 0))
  static bool inited = false;
  static bool prev_pressed = false;
  static bool long_sent = false;
  static bool press_armed = false;
  static unsigned long pressed_since = 0;

  bool pressed = trackerPowerButtonPressed();
  if (!inited) {
    inited = true;
    prev_pressed = pressed;
    pressed_since = 0;
    press_armed = false;
    return;
  }

  if (pressed && !prev_pressed) {
    trackerPulseButtonLed();
    pressed_since = millis();
    long_sent = false;
    press_armed = true;
  } else if (!pressed && prev_pressed) {
    if (press_armed) {
      uint32_t press_ms = (uint32_t)(millis() - pressed_since);
      if (!long_sent &&
          press_ms >= TRACKER_POWER_BUTTON_SHORT_MIN_MS &&
          press_ms < TRACKER_POWER_BUTTON_HOLD_MS) {
        TRACKER_DBG("power button short press -> tracker now");
        tracker_now_requested = true;
      }
    }
    press_armed = false;
    long_sent = false;
    pressed_since = 0;
  }

  if (pressed && press_armed && !long_sent &&
      (uint32_t)(millis() - pressed_since) >= TRACKER_POWER_BUTTON_HOLD_MS) {
    long_sent = true;
    press_armed = false;
    TRACKER_DBG("power button long press -> power off");
    radio_driver.powerOff();
    board.powerOff();
#if defined(NRF52_PLATFORM)
    TRACKER_DBG("board.powerOff returned, forcing nRF52 SYSTEMOFF");
    trackerForceSystemOffIfSupported();
#endif
  }

  prev_pressed = pressed;
#endif
}

struct TrackerEnvExtras {
  bool has_temperature = false;
  float temperature_c = 0.0f;
  bool has_light = false;
  uint32_t light_level = 0;  // board-defined scale
};

static TrackerEnvExtras readTrackerEnvExtras() {
  TrackerEnvExtras extras;
#if defined(T1000_E)
  float temp = t1000e_get_temperature();
  if (!isnan(temp) && !isinf(temp)) {
    extras.has_temperature = true;
    extras.temperature_c = temp;
  }
  extras.has_light = true;
  extras.light_level = t1000e_get_light();
#endif
  return extras;
}

static void buildTrackerEnvExtrasJson(char* out, size_t out_len) {
  if (!out || out_len == 0) {
    return;
  }
  out[0] = 0;

  TrackerEnvExtras extras = readTrackerEnvExtras();
  size_t used = 0;

  if (extras.has_temperature) {
    int n = snprintf(out + used, out_len - used, ",\"temp\":%.1f", extras.temperature_c);
    if (n > 0 && (size_t)n < (out_len - used)) {
      used += (size_t)n;
    } else {
      out[out_len - 1] = 0;
      return;
    }
  }

  if (extras.has_light) {
    int n = snprintf(out + used, out_len - used, ",\"light\":%lu", (unsigned long)extras.light_level);
    if (n > 0 && (size_t)n < (out_len - used)) {
      used += (size_t)n;
    } else {
      out[out_len - 1] = 0;
      return;
    }
  }
}

class TrackerMesh : public SensorMesh {
public:
  TrackerMesh(mesh::MainBoard& board, mesh::Radio& radio, mesh::MillisecondClock& ms, mesh::RNG& rng, mesh::RTCClock& rtc, mesh::MeshTables& tables)
    : SensorMesh(board, radio, ms, rng, rtc, tables) {
    _next_measure_millis = millis() + TRACKER_BOOT_GRACE_SECS * 1000UL;
    configureGroupChannel(TRACKER_DEFAULT_GROUP_PSK);
    TRACKER_DBG("boot defaults: interval=%lus timeout=%lus sleep=%s fix=%s min_sats=%u min_age=%lus grace=%lus",
      (unsigned long)_interval_secs,
      (unsigned long)_gps_timeout_secs,
      _sleep_enabled ? "on" : "off",
      _require_live_fix ? "live" : "cached",
      (unsigned)_min_sats,
      (unsigned long)_min_live_fix_age_secs,
      (unsigned long)TRACKER_BOOT_GRACE_SECS);
  }

  void begin(FILESYSTEM* fs) {
    SensorMesh::begin(fs);
    _cfg_fs = fs;
    loadTrackerConfig();
#if defined(ESP32)
    if (_sleep_enabled && esp_reset_reason() == ESP_RST_DEEPSLEEP) {
      _next_measure_millis = millis() + TRACKER_SLEEP_DRAIN_RECHECK_MS;
      TRACKER_DBG("wake from deep sleep: schedule next cycle in %ums", (unsigned)TRACKER_SLEEP_DRAIN_RECHECK_MS);
    }
#endif
  }

  void queueImmediateCycle(const char* origin = "manual") {
    _sleep_waiting_for_queue_drain = false;
    _sleep_drain_started_millis = 0;
    _next_measure_millis = millis();
    TRACKER_DBG("immediate cycle queued (%s)", origin ? origin : "unknown");
  }

  void loop() {
#if defined(NRF52_PLATFORM)
    // nRF52 light sleep mode: keep running state (including GPS backup-assisted flow)
    // while idling between tracker cycles.
    if (_sleep_enabled &&
        !_tracking_in_progress &&
        !_sleep_waiting_for_queue_drain &&
        _next_measure_millis &&
        !millisHasNowPassed(_next_measure_millis)) {
      board.sleep(0);
      return;
    }
#endif

    SensorMesh::loop();

    if (_tracking_in_progress) {
      pollTrackingCycle();
    } else if (_next_measure_millis && millisHasNowPassed(_next_measure_millis)) {
      if (_sleep_waiting_for_queue_drain) {
        handleSleepQueueDrain();
        return;
      }
      TRACKER_DBG("next cycle due");
      _next_measure_millis = 0;
      startTrackingCycle();
    }
  }

protected:
  uint32_t _interval_secs = TRACKER_DEFAULT_INTERVAL_SECS;
  uint32_t _gps_timeout_secs = TRACKER_DEFAULT_GPS_TIMEOUT_SECS;
  bool _sleep_enabled = (TRACKER_DEFAULT_SLEEP_ENABLED == 1);
  bool _require_live_fix = (TRACKER_DEFAULT_REQUIRE_LIVE_FIX == 1);
  uint8_t _min_sats = TRACKER_DEFAULT_MIN_SATS;
  uint16_t _min_live_fix_age_secs = TRACKER_DEFAULT_MIN_FIX_AGE_SECS;
  unsigned long _next_measure_millis = 0;
  bool _sleep_waiting_for_queue_drain = false;
  unsigned long _sleep_drain_started_millis = 0;
  mesh::GroupChannel _group_channel;
  bool _group_ready = false;
  bool _group_psk_is_default = true;
  char _group_name[24] = TRACKER_DEFAULT_GROUP_NAME;
  char _group_psk[56] = TRACKER_DEFAULT_GROUP_PSK;

  void onSensorDataRead() override {
    // No periodic sensor logic needed; tracking is timer-driven from TrackerMesh::loop().
  }

  int querySeriesData(uint32_t, uint32_t, MinMaxAvg[], int) override {
    return 0;
  }

  bool handleCustomCommand(uint32_t sender_timestamp, char* command, char* reply) override {
    (void)sender_timestamp;

    if (strcmp(command, "tracker") == 0 || strcmp(command, "tracker status") == 0) {
      sprintf(reply,
        "tracker: interval=%lus timeout=%lus sleep=%s fix=%s min_sats=%u min_age=%us group=%s",
        (unsigned long)_interval_secs,
        (unsigned long)_gps_timeout_secs,
        _sleep_enabled ? "on" : "off",
        _require_live_fix ? "live" : "cached",
        (unsigned)_min_sats,
        (unsigned)_min_live_fix_age_secs,
        _group_name);
      return true;
    }

    if (memcmp(command, "tracker interval ", 17) == 0) {
      uint32_t secs = atoi(&command[17]);
      if (secs < 30 || secs > 86400) {
        strcpy(reply, "Error: interval range is 30-86400 seconds");
      } else {
        _interval_secs = secs;
        persistTrackerConfig();
        strcpy(reply, "OK");
      }
      return true;
    }

    if (memcmp(command, "tracker timeout ", 16) == 0) {
      uint32_t secs = atoi(&command[16]);
      if (secs < 30 || secs > TRACKER_GPS_TIMEOUT_MAX_SECS) {
        snprintf(reply, 80, "Error: timeout range is 30-%u seconds", (unsigned)TRACKER_GPS_TIMEOUT_MAX_SECS);
      } else {
        _gps_timeout_secs = secs;
        persistTrackerConfig();
        strcpy(reply, "OK");
      }
      return true;
    }

    if (memcmp(command, "tracker sleep ", 14) == 0) {
      if (memcmp(&command[14], "on", 2) == 0) {
        _sleep_enabled = true;
        _sleep_waiting_for_queue_drain = false;
        _sleep_drain_started_millis = 0;
        persistTrackerConfig();
        strcpy(reply, "OK");
      } else if (memcmp(&command[14], "off", 3) == 0) {
        _sleep_enabled = false;
        _sleep_waiting_for_queue_drain = false;
        _sleep_drain_started_millis = 0;
        persistTrackerConfig();
        strcpy(reply, "OK");
      } else {
        strcpy(reply, "Error: use tracker sleep on|off");
      }
      return true;
    }

    if (memcmp(command, "tracker group.name ", 19) == 0) {
      if (strlen(&command[19]) == 0) {
        strcpy(reply, "Error: group name cannot be empty");
      } else {
        StrHelper::strncpy(_group_name, &command[19], sizeof(_group_name));
        if (_group_psk_is_default && strcmp(_group_name, "Public") != 0) {
          _group_ready = false;
          strcpy(reply, "OK (set tracker group.psk <base64-psk>)");
        } else {
          if (_group_psk_is_default) {
            configureGroupChannel(_group_psk);
          }
          strcpy(reply, "OK");
        }
        persistTrackerConfig();
      }
      return true;
    }

    if (memcmp(command, "tracker fix ", 12) == 0) {
      if (memcmp(&command[12], "live", 4) == 0) {
        _require_live_fix = true;
        persistTrackerConfig();
        strcpy(reply, "OK");
      } else if (memcmp(&command[12], "cached", 6) == 0) {
        _require_live_fix = false;
        persistTrackerConfig();
        strcpy(reply, "OK");
      } else {
        strcpy(reply, "Error: use tracker fix live|cached");
      }
      return true;
    }

    if (memcmp(command, "tracker sats ", 13) == 0) {
      int sats = atoi(&command[13]);
      if (sats < 0 || sats > 30) {
        strcpy(reply, "Error: sats range is 0-30");
      } else {
        _min_sats = (uint8_t)sats;
        persistTrackerConfig();
        strcpy(reply, "OK");
      }
      return true;
    }

    if (memcmp(command, "tracker fix.age ", 16) == 0) {
      int secs = atoi(&command[16]);
      if (secs < 0 || secs > 3600) {
        strcpy(reply, "Error: fix.age range is 0-3600 seconds");
      } else {
        _min_live_fix_age_secs = (uint16_t)secs;
        persistTrackerConfig();
        strcpy(reply, "OK");
      }
      return true;
    }

    if (strcmp(command, "tracker gps") == 0) {
      formatGPSStatus(reply, 240);
      return true;
    }

    if (memcmp(command, "tracker group.psk ", 18) == 0) {
      char normalized_b64[56];
      if (configureGroupChannel(&command[18], normalized_b64, sizeof(normalized_b64))) {
        StrHelper::strncpy(_group_psk, normalized_b64, sizeof(_group_psk));
        _group_psk_is_default = false;
        persistTrackerConfig();
        strcpy(reply, "OK");
      } else {
        strcpy(reply, "Error: invalid group key (base64 or hex 16/32 bytes)");
      }
      return true;
    }

    if (strcmp(command, "tracker group") == 0) {
      sprintf(reply, "group=%s psk=%s (%s) hash=%02X%02X%02X%02X",
        _group_name,
        _group_psk,
        _group_psk_is_default ? "default" : "custom",
        _group_channel.hash[0],
        _group_channel.hash[1],
        _group_channel.hash[2],
        _group_channel.hash[3]);
      return true;
    }

    if (strcmp(command, "tracker send") == 0 || strcmp(command, "tracker now") == 0) {
      queueImmediateCycle("cmd");
      strcpy(reply, "queued");
      return true;
    }

    return false;
  }

private:
  struct TrackerConfigV1 {
    uint32_t magic;
    uint16_t version;
    uint16_t reserved;
    uint32_t interval_secs;
    uint32_t gps_timeout_secs;
    uint8_t sleep_enabled;
    uint8_t group_psk_is_default;
    uint8_t reserved2[2];
    char group_name[sizeof(_group_name)];
    char group_psk[sizeof(_group_psk)];
  };

  static const uint32_t TRACKER_CONFIG_MAGIC = 0x544B5231UL;  // "TKR1"
  static const uint16_t TRACKER_CONFIG_VERSION = 1;

  bool _tracking_in_progress = false;
  unsigned long _tracking_started_millis = 0;
  unsigned long _last_wait_log_millis = 0;
  FILESYSTEM* _cfg_fs = NULL;
  unsigned long _live_fix_since_millis = 0;
  bool _last_live_fix_state = false;

  struct GPSState {
    LocationProvider* location = NULL;
    bool has_provider = false;
    bool enabled = false;
    bool valid = false;
    int sats = -1;
    long timestamp = 0;
    long raw_lat = 0;
    long raw_lon = 0;
    long raw_alt = 0;
    long raw_speed = LONG_MIN;   // thousandths of knot
    long raw_course = LONG_MIN;  // thousandths of degree
  };

  GPSState getGPSState() const {
    GPSState s;
    s.location = sensors.getLocationProvider();
    s.has_provider = (s.location != NULL);
    if (s.has_provider) {
      s.enabled = s.location->isEnabled();
      s.valid = s.enabled && s.location->isValid();
      s.sats = s.location->satellitesCount();
      s.timestamp = s.location->getTimestamp();
      s.raw_lat = s.location->getLatitude();
      s.raw_lon = s.location->getLongitude();
      s.raw_alt = s.location->getAltitude();
      s.raw_speed = s.location->getSpeed();
      s.raw_course = s.location->getCourse();
    }
    return s;
  }

  bool gpsSpeedValid(long raw_speed) const {
    return raw_speed != LONG_MIN && raw_speed >= 0;
  }

  bool gpsCourseValid(long raw_course) const {
    return raw_course != LONG_MIN && raw_course >= 0;
  }

  float gpsSpeedKmh(long raw_speed) const {
    return ((float)raw_speed * 1.852f) / 1000.0f;
  }

  float gpsCourseDeg(long raw_course) const {
    return (float)raw_course / 1000.0f;
  }

  void updateLiveFixState(bool gps_live_fix) {
    if (gps_live_fix) {
      if (!_last_live_fix_state || _live_fix_since_millis == 0) {
        _live_fix_since_millis = millis();
        TRACKER_DBG("gps live fix acquired");
      }
    } else {
      if (_last_live_fix_state) {
        TRACKER_DBG("gps live fix lost");
      }
      _live_fix_since_millis = 0;
    }
    _last_live_fix_state = gps_live_fix;
  }

  uint32_t getLiveFixAgeSecs() const {
    if (_live_fix_since_millis == 0) {
      return 0;
    }
    return (uint32_t)((millis() - _live_fix_since_millis) / 1000UL);
  }

  void formatGPSStatus(char* reply, size_t reply_len) {
    GPSState gps = getGPSState();
    bool cached = hasStoredPosition();
    uint32_t age = getLiveFixAgeSecs();
    char speed_buf[16];
    char course_buf[16];
    if (gpsSpeedValid(gps.raw_speed)) {
      snprintf(speed_buf, sizeof(speed_buf), "%.1fkm/h", gpsSpeedKmh(gps.raw_speed));
    } else {
      StrHelper::strncpy(speed_buf, "n/a", sizeof(speed_buf));
    }
    if (gpsCourseValid(gps.raw_course)) {
      snprintf(course_buf, sizeof(course_buf), "%.1fdeg", gpsCourseDeg(gps.raw_course));
    } else {
      StrHelper::strncpy(course_buf, "n/a", sizeof(course_buf));
    }
    snprintf(reply, reply_len,
      "gps: provider=%d enabled=%d valid=%d sats=%d age=%lus min_sats=%u min_age=%us mode=%s cached=%d lat=%.6f lon=%.6f alt=%.1f spd=%s dir=%s raw_lat=%ld raw_lon=%ld ts=%ld",
      gps.has_provider ? 1 : 0,
      gps.enabled ? 1 : 0,
      gps.valid ? 1 : 0,
      gps.sats,
      (unsigned long)age,
      (unsigned)_min_sats,
      (unsigned)_min_live_fix_age_secs,
      _require_live_fix ? "live" : "cached",
      cached ? 1 : 0,
      sensors.node_lat,
      sensors.node_lon,
      sensors.node_altitude,
      speed_buf,
      course_buf,
      gps.raw_lat,
      gps.raw_lon,
      gps.timestamp);
  }

  void syncRTCFromGPS(const GPSState& gps) {
    if (!gps.valid) {
      return;
    }
    if (gps.raw_lat == 0 && gps.raw_lon == 0) {
      return;
    }
    if (gps.timestamp < 1577836800L) { // 2020-01-01
      return;
    }
    uint32_t rtc_now = getRTCClock()->getCurrentTime();
    long drift = (long)gps.timestamp - (long)rtc_now;
    long abs_drift = drift >= 0 ? drift : -drift;

    // Default fallback epoch used by VolatileRTCClock/ESP32RTCClock startup path.
    const uint32_t fallback_epoch = 1715770351UL; // 15 May 2024, 20:52:31 UTC
    bool rtc_is_fallback = (rtc_now >= (fallback_epoch - 86400UL) && rtc_now <= (fallback_epoch + 86400UL));
    bool rtc_is_invalid = (rtc_now < 1577836800UL); // before 2020-01-01 -> clearly not user-set real time

    // Safety: after user/manual time set, do not apply large GPS jumps.
    // Only allow large jumps when the RTC still looks like fallback/default.
    if (abs_drift <= 2) {
      return;
    }
    if (!rtc_is_fallback && !rtc_is_invalid && abs_drift > 30) {
      TRACKER_DBG("rtc gps sync skipped: large drift=%lds rtc=%lu gps=%ld",
        drift,
        (unsigned long)rtc_now,
        gps.timestamp);
      return;
    }

    if (drift > 2 || drift < -2) {
      getRTCClock()->setCurrentTime((uint32_t)gps.timestamp);
      TRACKER_DBG("rtc synced from gps: gps_ts=%ld rtc_prev=%lu drift=%lds",
        gps.timestamp,
        (unsigned long)rtc_now,
        drift);
    }
  }

  void applyGroupPolicyGuard() {
    if (_group_psk_is_default && strcmp(_group_name, "Public") != 0) {
      _group_ready = false;
    }
  }

  File openConfigForRead() {
#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
    return _cfg_fs->open(TRACKER_CONFIG_PATH, FILE_O_READ);
#elif defined(RP2040_PLATFORM)
    return _cfg_fs->open(TRACKER_CONFIG_PATH, "r");
#else
    return _cfg_fs->open(TRACKER_CONFIG_PATH, "r", false);
#endif
  }

  File openConfigForWrite() {
#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
    return _cfg_fs->open(TRACKER_CONFIG_PATH, FILE_O_WRITE);
#elif defined(RP2040_PLATFORM)
    return _cfg_fs->open(TRACKER_CONFIG_PATH, "w");
#else
    return _cfg_fs->open(TRACKER_CONFIG_PATH, "w", true);
#endif
  }

  bool persistTrackerConfig() {
    if (_cfg_fs == NULL) {
      return false;
    }

    TrackerConfigV1 cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.magic = TRACKER_CONFIG_MAGIC;
    cfg.version = TRACKER_CONFIG_VERSION;
    cfg.interval_secs = _interval_secs;
    cfg.gps_timeout_secs = _gps_timeout_secs;
    cfg.reserved = _min_live_fix_age_secs;
    cfg.sleep_enabled = _sleep_enabled ? 1 : 0;
    cfg.group_psk_is_default = _group_psk_is_default ? 1 : 0;
    cfg.reserved2[0] = _require_live_fix ? 1 : 2;   // 1=live, 2=cached
    cfg.reserved2[1] = _min_sats;
    StrHelper::strncpy(cfg.group_name, _group_name, sizeof(cfg.group_name));
    StrHelper::strncpy(cfg.group_psk, _group_psk, sizeof(cfg.group_psk));

    // Ensure we overwrite prior content instead of appending stale records.
    _cfg_fs->remove(TRACKER_CONFIG_PATH);
    File file = openConfigForWrite();
    if (!file) {
      TRACKER_DBG("config save failed: open write");
      return false;
    }

    size_t written = file.write((const uint8_t*)&cfg, sizeof(cfg));
    file.close();
    if (written != sizeof(cfg)) {
      TRACKER_DBG("config save failed: short write (%u/%u)", (unsigned)written, (unsigned)sizeof(cfg));
      return false;
    }

    TRACKER_DBG("config saved");
    return true;
  }

  bool loadTrackerConfig() {
    if (_cfg_fs == NULL) {
      return false;
    }

    File file = openConfigForRead();
    if (!file) {
      TRACKER_DBG("config load: no saved config");
      return false;
    }

    TrackerConfigV1 cfg;
    memset(&cfg, 0, sizeof(cfg));
    size_t read_len = file.read((uint8_t*)&cfg, sizeof(cfg));
    file.close();
    if (read_len != sizeof(cfg)) {
      TRACKER_DBG("config load ignored: size mismatch (%u/%u)", (unsigned)read_len, (unsigned)sizeof(cfg));
      return false;
    }

    if (cfg.magic != TRACKER_CONFIG_MAGIC || cfg.version != TRACKER_CONFIG_VERSION) {
      TRACKER_DBG("config load ignored: bad header");
      return false;
    }

    if (cfg.interval_secs >= 30 && cfg.interval_secs <= 86400) {
      _interval_secs = cfg.interval_secs;
    }
    if (cfg.gps_timeout_secs >= 30 && cfg.gps_timeout_secs <= TRACKER_GPS_TIMEOUT_MAX_SECS) {
      _gps_timeout_secs = cfg.gps_timeout_secs;
    }
    if (cfg.reserved <= 3600) {
      _min_live_fix_age_secs = cfg.reserved;
    } else {
      _min_live_fix_age_secs = TRACKER_DEFAULT_MIN_FIX_AGE_SECS;
    }
    _sleep_enabled = (cfg.sleep_enabled != 0);
    _group_psk_is_default = (cfg.group_psk_is_default != 0);
    if (cfg.reserved2[0] == 2) {
      _require_live_fix = false;
    } else if (cfg.reserved2[0] == 1) {
      _require_live_fix = true;
    } else {
      _require_live_fix = (TRACKER_DEFAULT_REQUIRE_LIVE_FIX == 1);
    }
    if (cfg.reserved2[1] <= 30) {
      _min_sats = cfg.reserved2[1];
    } else {
      _min_sats = TRACKER_DEFAULT_MIN_SATS;
    }
    StrHelper::strncpy(_group_name, cfg.group_name, sizeof(_group_name));
    StrHelper::strncpy(_group_psk, cfg.group_psk, sizeof(_group_psk));

    char normalized_b64[56];
    if (!configureGroupChannel(_group_psk, normalized_b64, sizeof(normalized_b64))) {
      _group_ready = false;
    } else {
      StrHelper::strncpy(_group_psk, normalized_b64, sizeof(_group_psk));
    }
    applyGroupPolicyGuard();

    TRACKER_DBG("config loaded: interval=%lus timeout=%lus sleep=%s fix=%s min_sats=%u group=%s",
      (unsigned long)_interval_secs,
      (unsigned long)_gps_timeout_secs,
      _sleep_enabled ? "on" : "off",
      _require_live_fix ? "live" : "cached",
      (unsigned)_min_sats,
      _group_name);
    return true;
  }

  bool hasStoredPosition() const {
    const double lat = sensors.node_lat;
    const double lon = sensors.node_lon;
    return lat >= -90.0 && lat <= 90.0
      && lon >= -180.0 && lon <= 180.0
      && !(lat == 0.0 && lon == 0.0);
  }

  static int hexNibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return -1;
  }

  static bool isHexString(const char* s, size_t n) {
    if (!s || (n != 32 && n != 64)) {
      return false;
    }
    for (size_t i = 0; i < n; i++) {
      if (hexNibble(s[i]) < 0) {
        return false;
      }
    }
    return true;
  }

  static bool hexToBytes(const char* s, size_t n, uint8_t* out) {
    if (!isHexString(s, n)) {
      return false;
    }
    for (size_t i = 0; i < n; i += 2) {
      int hi = hexNibble(s[i]);
      int lo = hexNibble(s[i + 1]);
      out[i / 2] = (uint8_t)((hi << 4) | lo);
    }
    return true;
  }

  bool decodeGroupSecret(const char* input, uint8_t* out_secret, int& out_len, char* canonical_b64, size_t canonical_b64_len) {
    if (!input || !input[0]) {
      return false;
    }

    size_t in_len = strlen(input);

    // Prefer direct hex input when it matches the canonical hex lengths.
    if (isHexString(input, in_len)) {
      if (!hexToBytes(input, in_len, out_secret)) {
        return false;
      }
      out_len = (int)(in_len / 2);
    } else {
      uint8_t decoded[64];
      int decoded_len = decode_base64((unsigned char*)input, in_len, decoded);
      if (!(decoded_len == 16 || decoded_len == 32 || decoded_len == 64)) {
        return false;
      }

      // Handle accidental "base64 of hex string" inputs from app copy/paste.
      if ((decoded_len == 32 || decoded_len == 64) && isHexString((const char*)decoded, decoded_len)) {
        if (!hexToBytes((const char*)decoded, decoded_len, out_secret)) {
          return false;
        }
        out_len = decoded_len / 2;
      } else {
        memcpy(out_secret, decoded, decoded_len);
        out_len = decoded_len;
      }
    }

    if (canonical_b64 && canonical_b64_len > 0) {
      int enc_len = encode_base64_length(out_len);
      if (enc_len <= 0 || (size_t)(enc_len + 1) > canonical_b64_len) {
        return false;
      }
      encode_base64(out_secret, out_len, (unsigned char*)canonical_b64);
      canonical_b64[enc_len] = 0;
    }

    return true;
  }

  bool configureGroupChannel(const char* key_input, char* canonical_b64 = NULL, size_t canonical_b64_len = 0) {
    memset(_group_channel.secret, 0, sizeof(_group_channel.secret));

    int len = 0;
    if (decodeGroupSecret(key_input, _group_channel.secret, len, canonical_b64, canonical_b64_len)) {
      mesh::Utils::sha256(_group_channel.hash, sizeof(_group_channel.hash), _group_channel.secret, len);
      _group_ready = true;
      TRACKER_DBG("group channel configured (secret_len=%d)", len);
      return true;
    }

    _group_ready = false;
    TRACKER_DBG("group channel config failed (decoded_len=%d)", len);
    return false;
  }

  void sendTrackerFixJson(
    float lat,
    float lon,
    float alt,
    int sats,
    bool speed_ok,
    float speed_kmh,
    bool course_ok,
    float course_deg,
    const char* fix_mode,
    uint32_t fix_secs
  ) {
    (void)fix_mode;
    char sats_txt[12];
    char speed_txt[16];
    char course_txt[16];
    char batt_txt[12];

    if (sats >= 0) {
      snprintf(sats_txt, sizeof(sats_txt), "%d", sats);
    } else {
      StrHelper::strncpy(sats_txt, "null", sizeof(sats_txt));
    }

    if (speed_ok) {
      snprintf(speed_txt, sizeof(speed_txt), "%.1f", speed_kmh);
    } else {
      StrHelper::strncpy(speed_txt, "null", sizeof(speed_txt));
    }

    if (course_ok) {
      snprintf(course_txt, sizeof(course_txt), "%.1f", course_deg);
    } else {
      StrHelper::strncpy(course_txt, "null", sizeof(course_txt));
    }

    const uint16_t batt_mv = board.getBattMilliVolts();
    int batt_pct = batt_mv > 0 ? batteryPercentFromMilliVolts(batt_mv) : -1;
    if (batt_pct >= 0) {
      snprintf(batt_txt, sizeof(batt_txt), "%d", batt_pct);
    } else {
      StrHelper::strncpy(batt_txt, "null", sizeof(batt_txt));
    }

    char extras_json[64];
    buildTrackerEnvExtrasJson(extras_json, sizeof(extras_json));

    char text[TRACKER_GROUP_TEXT_BUFFER];
    snprintf(text, sizeof(text),
      "{\"lat\":%.6f,\"lon\":%.6f,\"alt\":%.1f,\"sat\":%s,\"spd\":%s,\"dir\":%s,\"fix_s\":%lu,\"bat\":%s%s}",
      lat,
      lon,
      alt,
      sats_txt,
      speed_txt,
      course_txt,
      (unsigned long)fix_secs,
      batt_txt,
      extras_json);
    sendGroupText(text);
  }

  void sendTrackerEventJson(const char* event_name, const char* reason = NULL) {
    char batt_txt[12];
    const uint16_t batt_mv = board.getBattMilliVolts();
    int batt_pct = batt_mv > 0 ? batteryPercentFromMilliVolts(batt_mv) : -1;
    if (batt_pct >= 0) {
      snprintf(batt_txt, sizeof(batt_txt), "%d", batt_pct);
    } else {
      StrHelper::strncpy(batt_txt, "null", sizeof(batt_txt));
    }

    char extras_json[64];
    buildTrackerEnvExtrasJson(extras_json, sizeof(extras_json));

    char text[TRACKER_GROUP_TEXT_BUFFER];
    if (reason && reason[0]) {
      snprintf(text, sizeof(text),
        "{\"event\":\"%s\",\"reason\":\"%s\",\"bat\":%s%s}",
        event_name,
        reason,
        batt_txt,
        extras_json);
    } else {
      snprintf(text, sizeof(text),
        "{\"event\":\"%s\",\"bat\":%s%s}",
        event_name,
        batt_txt,
        extras_json);
    }
    sendGroupText(text);
  }

  void sendGroupText(const char* text) {
    if (!_group_ready) {
      TRACKER_DBG("skip group send: group not ready for '%s' (set tracker group.psk ...)", _group_name);
      return;
    }

    uint8_t data[5 + TRACKER_GROUP_TEXT_BUFFER];
    uint32_t timestamp = getRTCClock()->getCurrentTimeUnique();
    const char* sender_name = "tracker";
    NodePrefs* prefs = getNodePrefs();
    if (prefs && prefs->node_name[0]) {
      sender_name = prefs->node_name;
    }
    memcpy(data, &timestamp, 4);
    data[4] = 0;

    int written = snprintf((char*)&data[5], TRACKER_GROUP_TEXT_BUFFER, "%s: %s", sender_name, text);
    if (written < 0) {
      TRACKER_DBG("group text format failed");
      return;
    }
    if (written >= TRACKER_GROUP_TEXT_BUFFER) {
      TRACKER_DBG("group text truncated (%d/%u)", written, (unsigned)TRACKER_GROUP_TEXT_BUFFER);
    }
    int text_len = strlen((char*)&data[5]);
    if (text_len > TRACKER_GROUP_TEXT_MAX_LEN) {
      TRACKER_DBG("group text too long for packet (%d/%d), dropping",
        text_len, (int)TRACKER_GROUP_TEXT_MAX_LEN);
      return;
    }

    auto pkt = createGroupDatagram(PAYLOAD_TYPE_GRP_TXT, _group_channel, data, 5 + text_len);
    if (pkt) {
      TRACKER_DBG("send group text (psk hash=%02X%02X%02X%02X sender=%s ts=%lu): %s",
        _group_channel.hash[0],
        _group_channel.hash[1],
        _group_channel.hash[2],
        _group_channel.hash[3],
        sender_name,
        (unsigned long)timestamp,
        (char*)&data[5]);
      trackerPulseTxLed();
      sendFlood(pkt);
    } else {
      TRACKER_DBG("group packet alloc failed");
    }
  }

  void enterTrackerSleep(uint32_t secs) {
    TRACKER_DBG("enter deep sleep (%lus)", (unsigned long)secs);
    enterTimerOnlyDeepSleep(secs);
  }

  void scheduleNextCycle() {
    if (_sleep_enabled) {
#if defined(NRF52_PLATFORM)
      _sleep_waiting_for_queue_drain = false;
      _sleep_drain_started_millis = 0;
      _next_measure_millis = millis() + _interval_secs * 1000UL;
      TRACKER_DBG("schedule: sleep=light next in %lus", (unsigned long)_interval_secs);
#else
      uint32_t pending = _mgr->getOutboundCount(0xFFFFFFFF);
      TRACKER_DBG("schedule: sleep=on pending=%lu interval=%lus",
        (unsigned long)pending,
        (unsigned long)_interval_secs);
      if (pending == 0) {
        enterTrackerSleep(_interval_secs);
        _sleep_waiting_for_queue_drain = false;
        _sleep_drain_started_millis = 0;
      } else {
        _sleep_waiting_for_queue_drain = true;
        _sleep_drain_started_millis = millis();
        TRACKER_DBG("sleep deferred: outbound queue not empty, recheck in %ums",
          (unsigned)TRACKER_SLEEP_DRAIN_RECHECK_MS);
      }
      _next_measure_millis = millis() + TRACKER_SLEEP_DRAIN_RECHECK_MS;
      TRACKER_DBG("post-sleep wake window in %ums", (unsigned)TRACKER_SLEEP_DRAIN_RECHECK_MS);
#endif
    } else {
      _sleep_waiting_for_queue_drain = false;
      _sleep_drain_started_millis = 0;
      _next_measure_millis = millis() + _interval_secs * 1000UL;
      TRACKER_DBG("schedule: sleep=off next in %lus", (unsigned long)_interval_secs);
    }
  }

  void handleSleepQueueDrain() {
    uint32_t pending = _mgr->getOutboundCount(0xFFFFFFFF);
    uint32_t waited_ms = (_sleep_drain_started_millis == 0) ? 0 : (uint32_t)(millis() - _sleep_drain_started_millis);
    uint32_t waited_s = waited_ms / 1000UL;
    bool force_sleep = waited_ms >= (TRACKER_SLEEP_FLUSH_TIMEOUT_SECS * 1000UL);
    if (pending == 0) {
      TRACKER_DBG("sleep queue drained after %lus", (unsigned long)waited_s);
      enterTrackerSleep(_interval_secs);
      _sleep_waiting_for_queue_drain = false;
      _sleep_drain_started_millis = 0;
      _next_measure_millis = millis() + TRACKER_SLEEP_DRAIN_RECHECK_MS;
      TRACKER_DBG("post-sleep wake window in %ums", (unsigned)TRACKER_SLEEP_DRAIN_RECHECK_MS);
    } else if (force_sleep) {
      TRACKER_DBG("sleep force after %lus with pending=%lu", (unsigned long)waited_s, (unsigned long)pending);
      enterTrackerSleep(_interval_secs);
      _sleep_waiting_for_queue_drain = false;
      _sleep_drain_started_millis = 0;
      _next_measure_millis = millis() + TRACKER_SLEEP_DRAIN_RECHECK_MS;
      TRACKER_DBG("post-sleep wake window in %ums", (unsigned)TRACKER_SLEEP_DRAIN_RECHECK_MS);
    } else {
      _next_measure_millis = millis() + TRACKER_SLEEP_DRAIN_RECHECK_MS;
      TRACKER_DBG("sleep wait: pending=%lu waited=%lus retry in %ums",
        (unsigned long)pending,
        (unsigned long)waited_s,
        (unsigned)TRACKER_SLEEP_DRAIN_RECHECK_MS);
    }
  }

  void startTrackingCycle() {
    _tracking_in_progress = true;
    _tracking_started_millis = millis();
    _last_wait_log_millis = 0;
    TRACKER_DBG("tracking cycle started");
#if ENV_INCLUDE_GPS == 1
    sensors.setSettingValue("gps", "1");
    TRACKER_DBG("gps enabled request sent");
#endif
  }

  void completeTrackingCycle() {
#if ENV_INCLUDE_GPS == 1
    if (_sleep_enabled) {
      sensors.setSettingValue("gps", "0");
      TRACKER_DBG("gps disabled for sleep");
    }
#endif
    _tracking_in_progress = false;
    TRACKER_DBG("tracking cycle complete");
    scheduleNextCycle();
  }

  void pollTrackingCycle() {
#if ENV_INCLUDE_GPS == 1
    GPSState gps = getGPSState();
    syncRTCFromGPS(gps);
    bool gps_live_fix = gps.valid;
    bool live_coords_ok = !(gps.raw_lat == 0 && gps.raw_lon == 0);
    bool gps_live_with_coords = gps_live_fix && live_coords_ok;
    updateLiveFixState(gps_live_with_coords);
    uint32_t live_age_secs = getLiveFixAgeSecs();
    bool sats_ok = (_min_sats == 0) || (gps_live_with_coords && gps.sats >= (int)_min_sats);
    bool age_ok = (_min_live_fix_age_secs == 0) || (gps_live_with_coords && live_age_secs >= _min_live_fix_age_secs);
    bool gps_cached_fix = (!_require_live_fix) && hasStoredPosition();
    bool gps_live_accepted = gps_live_with_coords && (sats_ok || age_ok);
    if (gps_live_accepted || gps_cached_fix) {
      uint32_t fix_elapsed_s = (uint32_t)((millis() - _tracking_started_millis) / 1000UL);
      float lat = gps_live_accepted ? ((float)gps.raw_lat / 1000000.0f) : sensors.node_lat;
      float lon = gps_live_accepted ? ((float)gps.raw_lon / 1000000.0f) : sensors.node_lon;
      float alt = gps_live_accepted ? ((float)gps.raw_alt / 1000.0f) : sensors.node_altitude;
      int report_sats = gps_live_accepted ? gps.sats : -1;
      bool speed_ok = gps_live_accepted && gpsSpeedValid(gps.raw_speed);
      bool course_ok = gps_live_accepted && gpsCourseValid(gps.raw_course);
      float speed_kmh = speed_ok ? gpsSpeedKmh(gps.raw_speed) : 0.0f;
      float course_deg = course_ok ? gpsCourseDeg(gps.raw_course) : 0.0f;
      char speed_txt[12];
      char course_txt[12];
      if (speed_ok) {
        snprintf(speed_txt, sizeof(speed_txt), "%.1f", speed_kmh);
      } else {
        StrHelper::strncpy(speed_txt, "n/a", sizeof(speed_txt));
      }
      if (course_ok) {
        snprintf(course_txt, sizeof(course_txt), "%.1f", course_deg);
      } else {
        StrHelper::strncpy(course_txt, "n/a", sizeof(course_txt));
      }

      TRACKER_DBG("gps position ready (%s fix): lat=%.6f lon=%.6f sats=%d spd=%s km/h dir=%s deg",
        gps_live_accepted ? "live" : "cached",
        lat,
        lon,
        report_sats,
        speed_txt,
        course_txt);
      sendTrackerFixJson(
        lat,
        lon,
        alt,
        report_sats,
        speed_ok,
        speed_kmh,
        course_ok,
        course_deg,
        gps_live_accepted ? "live" : "cached",
        fix_elapsed_s);
      completeTrackingCycle();
      return;
    }

    const unsigned long timeout_millis = _gps_timeout_secs * 1000UL;
    if (_last_wait_log_millis == 0 || millisHasNowPassed(_last_wait_log_millis + 5000UL)) {
      _last_wait_log_millis = millis();
      char reason[64];
      reason[0] = 0;
      if (!gps.has_provider) {
        StrHelper::strncpy(reason, "no-provider", sizeof(reason));
      } else if (!gps.enabled) {
        StrHelper::strncpy(reason, "disabled", sizeof(reason));
      } else if (!gps_live_fix) {
        StrHelper::strncpy(reason, "no-fix", sizeof(reason));
      } else if (!live_coords_ok) {
        StrHelper::strncpy(reason, "zero-coords", sizeof(reason));
      } else if (!sats_ok) {
        StrHelper::strncpy(reason, "low-sats", sizeof(reason));
      } else if (!age_ok) {
        StrHelper::strncpy(reason, "fix-too-young", sizeof(reason));
      } else {
        StrHelper::strncpy(reason, "unknown", sizeof(reason));
      }
      TRACKER_DBG("waiting gps fix... elapsed=%lus timeout=%lus reason=%s enabled=%d valid=%d cached=%d sats=%d raw_lat=%ld raw_lon=%ld min=%u age=%lus min_age=%us mode=%s",
        (unsigned long)((millis() - _tracking_started_millis) / 1000UL),
        (unsigned long)_gps_timeout_secs,
        reason,
        gps.enabled ? 1 : 0,
        gps_live_fix ? 1 : 0,
        hasStoredPosition() ? 1 : 0,
        gps.sats,
        gps.raw_lat,
        gps.raw_lon,
        (unsigned)_min_sats,
        (unsigned long)live_age_secs,
        (unsigned)_min_live_fix_age_secs,
        _require_live_fix ? "live" : "cached");
    }
    if (millisHasNowPassed(_tracking_started_millis + timeout_millis)) {
      sendTrackerEventJson("timeout", "no_qualified_fix");
      completeTrackingCycle();
    }
#else
    sendTrackerEventJson("gps_unavailable", "build_without_gps");
    completeTrackingCycle();
#endif
  }
};

StdRNG fast_rng;
SimpleMeshTables tables;
TrackerMesh the_mesh(board, radio_driver, *new ArduinoMillis(), fast_rng, rtc_clock, tables);

void halt() {
  while (1) ;
}

static char command[160];

void setup() {
  Serial.begin(115200);
  delay(1000);
  TRACKER_DBG("setup begin");

  board.begin();
  TRACKER_DBG("board.begin done");
  trackerInitPowerButtonPin();
  TRACKER_DBG("button pin configured");
  trackerFlashBootLed();
  TRACKER_DBG("boot led pulse");

#ifdef DISPLAY_CLASS
  if (display.begin()) {
    display.startFrame();
    display.print("Please wait...");
    display.endFrame();
    TRACKER_DBG("display initialized");
  } else {
    TRACKER_DBG("display init failed");
  }
#endif

  if (!radio_init()) {
    TRACKER_DBG("radio_init failed, halting");
    halt();
  }
  TRACKER_DBG("radio_init done");

  fast_rng.begin(radio_get_rng_seed());
  TRACKER_DBG("rng seeded");

  FILESYSTEM* fs;
#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
  InternalFS.begin();
  fs = &InternalFS;
  IdentityStore store(InternalFS, "");
#elif defined(ESP32)
  SPIFFS.begin(true);
  fs = &SPIFFS;
  IdentityStore store(SPIFFS, "/identity");
#elif defined(RP2040_PLATFORM)
  LittleFS.begin();
  fs = &LittleFS;
  IdentityStore store(LittleFS, "/identity");
  store.begin();
#else
  #error "need to define filesystem"
#endif
  if (!store.load("_main", the_mesh.self_id)) {
    MESH_DEBUG_PRINTLN("Generating new keypair");
    TRACKER_DBG("identity not found, generating");
    the_mesh.self_id = radio_new_identity();
    int count = 0;
    while (count < 10 && (the_mesh.self_id.pub_key[0] == 0x00 || the_mesh.self_id.pub_key[0] == 0xFF)) {
      the_mesh.self_id = radio_new_identity(); count++;
    }
    store.save("_main", the_mesh.self_id);
    TRACKER_DBG("identity generated and saved");
  } else {
    TRACKER_DBG("identity loaded from storage");
  }

  Serial.print("Tracker ID: ");
  mesh::Utils::printHex(Serial, the_mesh.self_id.pub_key, PUB_KEY_SIZE); Serial.println();

  command[0] = 0;

  sensors.begin();
  TRACKER_DBG("sensors.begin done");


  the_mesh.begin(fs);
  TRACKER_DBG("mesh.begin done");

#ifdef DISPLAY_CLASS
  ui_task.begin(the_mesh.getNodePrefs(), FIRMWARE_BUILD_DATE, FIRMWARE_VERSION);
  TRACKER_DBG("ui_task.begin done");
#endif

#if ENABLE_ADVERT_ON_BOOT == 1
  the_mesh.sendSelfAdvertisement(16000, false);
  TRACKER_DBG("boot advertisement sent");
#endif
  TRACKER_DBG("setup complete");
}

void loop() {
  int len = strlen(command);
  bool got_line = false;
  while (Serial.available() && len < sizeof(command)-1) {
    char c = Serial.read();
    if (c == '\r' || c == '\n') {
      if (len > 0) {
        got_line = true;
      }
      continue;
    }
    if (c >= 32 && c <= 126) {
      command[len++] = c;
      command[len] = 0;
    } else {
      TRACKER_DBG("ignored non-printable serial byte: 0x%02X", (unsigned char)c);
    }
  }
  if (len == sizeof(command)-1) {  // command buffer full -> process now
    TRACKER_DBG("cmd auto-submit: buffer full");
    got_line = true;
  }
  if (got_line && len > 0) {
    char reply[240];
    TRACKER_DBG("cmd: %s", command);
    the_mesh.handleCommand(0, command, reply);
    if (reply[0]) {
      TRACKER_DBG("reply: %s", reply);
    }

    command[0] = 0;
  }

  the_mesh.loop();
  sensors.loop();
#ifdef DISPLAY_CLASS
  ui_task.loop();
#endif
  handleTrackerTxLedPulse();
  handleTrackerPowerButton();
  if (tracker_now_requested) {
    tracker_now_requested = false;
    the_mesh.queueImmediateCycle("button");
  }
  rtc_clock.tick();
}
