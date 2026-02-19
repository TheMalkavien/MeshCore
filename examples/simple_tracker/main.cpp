#include <Arduino.h>
#include "../simple_sensor/SensorMesh.h"
#include <base64.hpp>
#include <limits.h>
#if defined(ESP32)
  #include <esp_sleep.h>
  #include <driver/rtc_io.h>
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
  #define TRACKER_BOOT_GRACE_SECS 20
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

#if TRACKER_SERIAL_DEBUG == 1
  #define TRACKER_DBG(fmt, ...) Serial.printf("[tracker %10lu] " fmt "\r\n", millis(), ##__VA_ARGS__)
#else
  #define TRACKER_DBG(...) do { } while (0)
#endif

static void enterTimerOnlyDeepSleep(uint32_t secs) {
#if defined(ESP32)
  esp_sleep_disable_wakeup_source(ESP_SLEEP_WAKEUP_ALL);
  if (secs > 0) {
    esp_sleep_enable_timer_wakeup((uint64_t)secs * 1000000ULL);
  }
#if defined(P_LORA_NSS)
  if (rtc_gpio_is_valid_gpio((gpio_num_t)P_LORA_NSS)) {
    rtc_gpio_hold_en((gpio_num_t)P_LORA_NSS);
  }
#endif
  esp_deep_sleep_start();
#else
  board.sleep(secs);
#endif
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

  void loop() {
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
      if (secs < 30 || secs > 180) {
        strcpy(reply, "Error: timeout range is 30-180 seconds");
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
      _sleep_waiting_for_queue_drain = false;
      _sleep_drain_started_millis = 0;
      _next_measure_millis = millis();
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

    // Safety: after user/manual time set, do not apply large GPS jumps.
    // Only allow large jumps when the RTC still looks like fallback/default.
    if (abs_drift <= 2) {
      return;
    }
    if (!rtc_is_fallback && abs_drift > 30) {
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
    if (cfg.gps_timeout_secs >= 30 && cfg.gps_timeout_secs <= 180) {
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
    const char* fix_mode
  ) {
    char sats_txt[12];
    char speed_txt[16];
    char course_txt[16];

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

    char text[TRACKER_GROUP_TEXT_BUFFER];
    snprintf(text, sizeof(text),
      "{\"t\":\"tracker\",\"v\":1,\"lat\":%.6f,\"lon\":%.6f,\"alt\":%.1f,\"sat\":%s,\"spd\":%s,\"dir\":%s,\"fix\":\"%s\"}",
      lat,
      lon,
      alt,
      sats_txt,
      speed_txt,
      course_txt,
      (fix_mode && fix_mode[0]) ? fix_mode : "unknown");
    sendGroupText(text);
  }

  void sendTrackerEventJson(const char* event_name, const char* reason = NULL) {
    char text[TRACKER_GROUP_TEXT_BUFFER];
    if (reason && reason[0]) {
      snprintf(text, sizeof(text),
        "{\"t\":\"tracker\",\"v\":1,\"event\":\"%s\",\"reason\":\"%s\"}",
        event_name,
        reason);
    } else {
      snprintf(text, sizeof(text),
        "{\"t\":\"tracker\",\"v\":1,\"event\":\"%s\"}",
        event_name);
    }
    sendGroupText(text);
  }

  void sendGroupText(const char* text, bool include_sender = true) {
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

    int written = 0;
    if (include_sender) {
      written = snprintf((char*)&data[5], TRACKER_GROUP_TEXT_BUFFER, "%s: %s", sender_name, text);
    } else {
      written = snprintf((char*)&data[5], TRACKER_GROUP_TEXT_BUFFER, "%s", text);
    }
    if (written < 0) {
      TRACKER_DBG("group text format failed");
      return;
    }
    if (written >= TRACKER_GROUP_TEXT_BUFFER) {
      TRACKER_DBG("group text truncated (%d/%u)", written, (unsigned)TRACKER_GROUP_TEXT_BUFFER);
    }
    int text_len = strlen((char*)&data[5]);

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
        gps_live_accepted ? "live" : "cached");
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
  rtc_clock.tick();
}
