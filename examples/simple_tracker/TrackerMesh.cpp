#include "TrackerMesh.h"

#include <base64.hpp>
#include <limits.h>
#include <math.h>

#if defined(ESP32)
  #include <driver/rtc_io.h>
  #include <esp_sleep.h>
#endif

#include "tracker_peripherals.h"

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
  b.enterDeepSleep(secs, PIN_USER_BTN);
#elif defined(BUTTON_PIN) && (BUTTON_PIN >= 0)
  b.enterDeepSleep(secs, BUTTON_PIN);
#else
  b.enterDeepSleep(secs, -1);
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

template <typename T>
static auto trackerRadioTxPending(T& radio, int) -> decltype(radio.isTxPending(), bool()) {
  return radio.isTxPending();
}

template <typename T>
static bool trackerRadioTxPending(T&, long) {
  return false;
}

template <typename T>
static auto trackerRadioWakeFromSleep(T& radio, int) -> decltype(radio.wakeFromSleep(), void()) {
  radio.wakeFromSleep();
}

template <typename T>
static void trackerRadioWakeFromSleep(T&, long) {
}

TrackerMesh::TrackerMesh(mesh::MainBoard& board, mesh::Radio& radio, mesh::MillisecondClock& ms, mesh::RNG& rng, mesh::RTCClock& rtc, mesh::MeshTables& tables)
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

void TrackerMesh::begin(FILESYSTEM* fs) {
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

void TrackerMesh::queueImmediateCycle(const char* origin) {
  _sleep_waiting_for_queue_drain = false;
  _sleep_drain_started_millis = 0;
  _next_measure_millis = millis();
  TRACKER_DBG("immediate cycle queued (%s)", origin ? origin : "unknown");
}

void TrackerMesh::loop() {
  if (handleExternalPowerPolicy()) {
    SensorMesh::loop();
    return;
  }

#if defined(NRF52_PLATFORM)
  int32_t remaining_ms_i32 = _next_measure_millis ? (int32_t)(_next_measure_millis - millis()) : 0;
  bool cycle_due_or_late = (_next_measure_millis == 0) || (remaining_ms_i32 <= 0);
  const char* gps_state = sensors.getSettingByKey("gps");
  bool gps_enabled_now = (gps_state != NULL && gps_state[0] == '1');
  uint32_t pending_tx = _mgr->getOutboundCount(0xFFFFFFFF);
  bool radio_tx_active = trackerRadioTxPending(radio_driver, 0);
  if (_sleep_enabled &&
      !_tracking_in_progress &&
      !_sleep_waiting_for_queue_drain &&
      _next_measure_millis &&
      pending_tx == 0 &&
      !radio_tx_active &&
      !gps_enabled_now &&
      !trackerPeripheralsTxLedActive() &&
      !cycle_due_or_late) {
    uint32_t remaining_ms = (uint32_t)remaining_ms_i32;
    if (remaining_ms >= 1200UL && !trackerPeripheralsPowerButtonPressed()) {
      uint32_t sleep_secs = (remaining_ms + 999UL) / 1000UL;
      if (sleep_secs == 0) sleep_secs = 1;
      bool log_idle_sleep = (sleep_secs >= 2);
      if (log_idle_sleep) {
        TRACKER_DBG("idle sleep: remaining=%lums sleep=%lus", (unsigned long)remaining_ms, (unsigned long)sleep_secs);
      }
      trackerPeripheralsPrepareForIdleSleep();
      #if TRACKER_SLEEP_RADIO_POWEROFF == 1
      radio_driver.powerOff();
      #endif
      board.sleep(sleep_secs);
      #if TRACKER_SLEEP_RADIO_POWEROFF == 1
      trackerRadioWakeFromSleep(radio_driver, 0);
      #endif
      queueImmediateCycle("idle-wake");
      if (log_idle_sleep) {
        TRACKER_DBG("idle wake");
      }
      return;
    }
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

void TrackerMesh::pauseTrackingForExternalPower() {
  _sleep_waiting_for_queue_drain = false;
  _sleep_drain_started_millis = 0;
  _next_measure_millis = 0;
  _tracking_in_progress = false;
  _cycle_has_gps_snapshot = false;
  _cycle_start_gps_ts = LONG_MIN;
  _cycle_start_raw_lat = LONG_MIN;
  _cycle_start_raw_lon = LONG_MIN;
#if ENV_INCLUDE_GPS == 1
  sensors.setSettingValue("gps", "0");
#endif
}

bool TrackerMesh::handleExternalPowerPolicy() {
#if defined(NRF52_PLATFORM)
  board.setIgnoreVbusWake(_ignore_external_power);
#endif

  if (_ignore_external_power) {
    if (_external_power_paused) {
      _external_power_paused = false;
      TRACKER_DBG("external power override enabled: resume tracker GPS cycles");
      queueImmediateCycle("external-power-override");
    }
    return false;
  }

  bool external_powered = trackerPeripheralsIsExternalPowered();
  if (external_powered) {
    if (!_external_power_paused) {
      _external_power_paused = true;
      TRACKER_DBG("external power detected: pause tracker GPS cycles");
      pauseTrackingForExternalPower();
    } else {
#if ENV_INCLUDE_GPS == 1
      const char* gps_state = sensors.getSettingByKey("gps");
      if (gps_state != NULL && gps_state[0] == '1') {
        sensors.setSettingValue("gps", "0");
      }
#endif
    }
    return true;
  }

  if (_external_power_paused) {
    _external_power_paused = false;
    TRACKER_DBG("external power removed: resume tracker GPS cycles");
    queueImmediateCycle("external-power-removed");
  }

  return false;
}

void TrackerMesh::onSensorDataRead() {
}

int TrackerMesh::querySeriesData(uint32_t, uint32_t, MinMaxAvg[], int) {
  return 0;
}

TrackerMesh::GPSState TrackerMesh::getGPSState() const {
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

bool TrackerMesh::gpsSpeedValid(long raw_speed) const {
  return raw_speed != LONG_MIN && raw_speed >= 0;
}

bool TrackerMesh::gpsCourseValid(long raw_course) const {
  return raw_course != LONG_MIN && raw_course >= 0;
}

float TrackerMesh::gpsSpeedKmh(long raw_speed) const {
  return ((float)raw_speed * 1.852f) / 1000.0f;
}

float TrackerMesh::gpsCourseDeg(long raw_course) const {
  return (float)raw_course / 1000.0f;
}

void TrackerMesh::updateLiveFixState(bool gps_live_fix) {
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

uint32_t TrackerMesh::getLiveFixAgeSecs() const {
  if (_live_fix_since_millis == 0) {
    return 0;
  }
  return (uint32_t)((millis() - _live_fix_since_millis) / 1000UL);
}

void TrackerMesh::formatGPSStatus(char* reply, size_t reply_len) {
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

void TrackerMesh::syncRTCFromGPS(const GPSState& gps) {
  if (!gps.valid) {
    return;
  }
  if (gps.raw_lat == 0 && gps.raw_lon == 0) {
    return;
  }
  if (gps.timestamp < 1577836800L) {
    return;
  }
  uint32_t rtc_now = getRTCClock()->getCurrentTime();
  long drift = (long)gps.timestamp - (long)rtc_now;
  long abs_drift = drift >= 0 ? drift : -drift;

  const uint32_t fallback_epoch = 1715770351UL;
  bool rtc_is_fallback = (rtc_now >= (fallback_epoch - 86400UL) && rtc_now <= (fallback_epoch + 86400UL));
  bool rtc_is_invalid = (rtc_now < 1577836800UL);

  if (abs_drift <= 2) {
    return;
  }
  if (!rtc_is_fallback && !rtc_is_invalid && abs_drift > 3600) {
    TRACKER_DBG("rtc gps sync skipped: large drift=%lds rtc=%lu gps=%ld",
      drift,
      (unsigned long)rtc_now,
      gps.timestamp);
    return;
  }
  if (drift < -2) {
    TRACKER_DBG("rtc gps sync skipped: backward/stale gps_ts=%ld rtc=%lu drift=%lds",
      gps.timestamp,
      (unsigned long)rtc_now,
      drift);
    return;
  }

  if (drift > 2) {
    getRTCClock()->setCurrentTime((uint32_t)gps.timestamp);
    TRACKER_DBG("rtc synced from gps: gps_ts=%ld rtc_prev=%lu drift=%lds",
      gps.timestamp,
      (unsigned long)rtc_now,
      drift);
  }
}

void TrackerMesh::applyGroupPolicyGuard() {
  if (_group_psk_is_default && strcmp(_group_name, "Public") != 0) {
    _group_ready = false;
  }
}

File TrackerMesh::openConfigForRead() {
#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
  return _cfg_fs->open(TRACKER_CONFIG_PATH, FILE_O_READ);
#elif defined(RP2040_PLATFORM)
  return _cfg_fs->open(TRACKER_CONFIG_PATH, "r");
#else
  return _cfg_fs->open(TRACKER_CONFIG_PATH, "r", false);
#endif
}

File TrackerMesh::openConfigForWrite() {
#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
  return _cfg_fs->open(TRACKER_CONFIG_PATH, FILE_O_WRITE);
#elif defined(RP2040_PLATFORM)
  return _cfg_fs->open(TRACKER_CONFIG_PATH, "w");
#else
  return _cfg_fs->open(TRACKER_CONFIG_PATH, "w", true);
#endif
}

bool TrackerMesh::persistTrackerConfig() {
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
  cfg.reserved2[0] = _require_live_fix ? 1 : 2;
  cfg.reserved2[1] = _min_sats;
  StrHelper::strncpy(cfg.group_name, _group_name, sizeof(cfg.group_name));
  StrHelper::strncpy(cfg.group_psk, _group_psk, sizeof(cfg.group_psk));

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

bool TrackerMesh::loadTrackerConfig() {
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

  char normalized_b64[GROUP_PSK_SIZE];
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

bool TrackerMesh::hasStoredPosition() const {
  const double lat = sensors.node_lat;
  const double lon = sensors.node_lon;
  return lat >= -90.0 && lat <= 90.0 &&
         lon >= -180.0 && lon <= 180.0 &&
         !(lat == 0.0 && lon == 0.0);
}

int TrackerMesh::hexNibble(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
  if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
  return -1;
}

bool TrackerMesh::isHexString(const char* s, size_t n) {
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

bool TrackerMesh::hexToBytes(const char* s, size_t n, uint8_t* out) {
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

bool TrackerMesh::decodeGroupSecret(const char* input, uint8_t* out_secret, int& out_len, char* canonical_b64, size_t canonical_b64_len) {
  if (!input || !input[0]) {
    return false;
  }

  size_t in_len = strlen(input);

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

bool TrackerMesh::configureGroupChannel(const char* key_input, char* canonical_b64, size_t canonical_b64_len) {
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

void TrackerMesh::sendTrackerFixJson(float lat, float lon, float alt, int sats, bool speed_ok, float speed_kmh, bool course_ok, float course_deg, const char* fix_mode, uint32_t fix_secs) {
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
  trackerBuildEnvExtrasJson(extras_json, sizeof(extras_json));

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

void TrackerMesh::sendTrackerEventJson(const char* event_name, const char* reason) {
  char batt_txt[12];
  const uint16_t batt_mv = board.getBattMilliVolts();
  int batt_pct = batt_mv > 0 ? batteryPercentFromMilliVolts(batt_mv) : -1;
  if (batt_pct >= 0) {
    snprintf(batt_txt, sizeof(batt_txt), "%d", batt_pct);
  } else {
    StrHelper::strncpy(batt_txt, "null", sizeof(batt_txt));
  }

  char extras_json[64];
  trackerBuildEnvExtrasJson(extras_json, sizeof(extras_json));

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

void TrackerMesh::sendGroupText(const char* text) {
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
    TRACKER_DBG("group text too long for packet (%d/%d), dropping", text_len, (int)TRACKER_GROUP_TEXT_MAX_LEN);
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
    trackerPeripheralsNotifyTx();
    sendFlood(pkt);
  } else {
    TRACKER_DBG("group packet alloc failed");
  }
}

void TrackerMesh::enterTrackerSleep(uint32_t secs) {
  TRACKER_DBG("enter deep sleep (%lus)", (unsigned long)secs);
  enterTimerOnlyDeepSleep(secs);
}

void TrackerMesh::scheduleNextCycle() {
  if (_sleep_enabled) {
#if defined(NRF52_PLATFORM)
    _sleep_waiting_for_queue_drain = false;
    _sleep_drain_started_millis = 0;
    _next_measure_millis = millis() + _interval_secs * 1000UL;
    TRACKER_DBG("schedule: sleep=light next in %lus", (unsigned long)_interval_secs);
#else
    uint32_t pending = _mgr->getOutboundCount(0xFFFFFFFF);
    TRACKER_DBG("schedule: sleep=on pending=%lu interval=%lus", (unsigned long)pending, (unsigned long)_interval_secs);
    if (pending == 0) {
      enterTrackerSleep(_interval_secs);
      _sleep_waiting_for_queue_drain = false;
      _sleep_drain_started_millis = 0;
    } else {
      _sleep_waiting_for_queue_drain = true;
      _sleep_drain_started_millis = millis();
      TRACKER_DBG("sleep deferred: outbound queue not empty, recheck in %ums", (unsigned)TRACKER_SLEEP_DRAIN_RECHECK_MS);
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

void TrackerMesh::handleSleepQueueDrain() {
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

void TrackerMesh::startTrackingCycle() {
  _tracking_in_progress = true;
  _tracking_started_millis = millis();
  _last_wait_log_millis = _tracking_started_millis;
  TRACKER_DBG("tracking cycle started");
#if ENV_INCLUDE_GPS == 1
  GPSState before_enable = getGPSState();
  _cycle_has_gps_snapshot = before_enable.has_provider;
  _cycle_start_gps_ts = before_enable.timestamp;
  _cycle_start_raw_lat = before_enable.raw_lat;
  _cycle_start_raw_lon = before_enable.raw_lon;
  sensors.setSettingValue("gps", "1");
  TRACKER_DBG("gps enabled request sent");
#endif
}

void TrackerMesh::completeTrackingCycle() {
#if ENV_INCLUDE_GPS == 1
  if (_sleep_enabled) {
    sensors.setSettingValue("gps", "0");
    TRACKER_DBG("gps disabled for sleep");
  }
#endif
  _cycle_has_gps_snapshot = false;
  _cycle_start_gps_ts = LONG_MIN;
  _cycle_start_raw_lat = LONG_MIN;
  _cycle_start_raw_lon = LONG_MIN;
  _tracking_in_progress = false;
  TRACKER_DBG("tracking cycle complete");
  scheduleNextCycle();
}

void TrackerMesh::pollTrackingCycle() {
#if ENV_INCLUDE_GPS == 1
  GPSState gps = getGPSState();
  bool gps_live_fix = gps.valid;
  bool live_coords_ok = !(gps.raw_lat == 0 && gps.raw_lon == 0);
  bool gps_live_with_coords = gps_live_fix && live_coords_ok;
  bool gps_fresh_for_cycle = true;
  if (_cycle_has_gps_snapshot) {
    gps_fresh_for_cycle =
      (gps.timestamp != _cycle_start_gps_ts) ||
      (gps.raw_lat != _cycle_start_raw_lat) ||
      (gps.raw_lon != _cycle_start_raw_lon);
  }
  updateLiveFixState(gps_live_with_coords);
  uint32_t live_age_secs = getLiveFixAgeSecs();
  bool sats_ok = (_min_sats == 0) || (gps_live_with_coords && gps.sats >= (int)_min_sats);
  bool age_ok = (_min_live_fix_age_secs == 0) || (gps_live_with_coords && live_age_secs >= _min_live_fix_age_secs);
  bool gps_cached_fix = (!_require_live_fix) && hasStoredPosition();
  bool gps_live_accepted = gps_live_with_coords && gps_fresh_for_cycle && (sats_ok || age_ok);
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
    if (gps_live_accepted) {
      syncRTCFromGPS(gps);
    }
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
  if (_last_wait_log_millis == 0 || (uint32_t)(millis() - _last_wait_log_millis) >= 5000UL) {
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
    } else if (!gps_fresh_for_cycle) {
      StrHelper::strncpy(reason, "stale-cycle-fix", sizeof(reason));
    } else if (!sats_ok) {
      StrHelper::strncpy(reason, "low-sats", sizeof(reason));
    } else if (!age_ok) {
      StrHelper::strncpy(reason, "fix-too-young", sizeof(reason));
    } else {
      StrHelper::strncpy(reason, "unknown", sizeof(reason));
    }
    TRACKER_DBG("waiting gps fix... elapsed=%lus timeout=%lus reason=%s enabled=%d valid=%d fresh=%d cached=%d sats=%d raw_lat=%ld raw_lon=%ld ts=%ld start_ts=%ld min=%u age=%lus min_age=%us mode=%s",
      (unsigned long)((millis() - _tracking_started_millis) / 1000UL),
      (unsigned long)_gps_timeout_secs,
      reason,
      gps.enabled ? 1 : 0,
      gps_live_fix ? 1 : 0,
      gps_fresh_for_cycle ? 1 : 0,
      hasStoredPosition() ? 1 : 0,
      gps.sats,
      gps.raw_lat,
      gps.raw_lon,
      gps.timestamp,
      _cycle_start_gps_ts,
      (unsigned)_min_sats,
      (unsigned long)live_age_secs,
      (unsigned)_min_live_fix_age_secs,
      _require_live_fix ? "live" : "cached");
  }
  if (millisHasNowPassed(_tracking_started_millis + timeout_millis)) {
    sendTrackerEventJson("timeout", "no_qualified_fix");
    completeTrackingCycle();
    return;
  }
#else
  sendTrackerEventJson("gps_unavailable", "build_without_gps");
  completeTrackingCycle();
#endif
}
