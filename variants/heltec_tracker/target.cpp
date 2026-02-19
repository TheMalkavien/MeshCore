#include <Arduino.h>
#include "target.h"

#include <helpers/sensors/MicroNMEALocationProvider.h>
#include <time.h>
#include <math.h>
#include <limits.h>

#ifndef HWT_GPS_RECONFIGURE_ON_START
  #define HWT_GPS_RECONFIGURE_ON_START 1
#endif

#ifndef HWT_GPS_SEND_CFGSYS_ON_START
  #define HWT_GPS_SEND_CFGSYS_ON_START 0
#endif

#ifndef HWT_GPS_SEND_AID_ON_START
  #define HWT_GPS_SEND_AID_ON_START 1
#endif

#ifndef HWT_GPS_RTC_FALLBACK_EPOCH
  // Matches tracker fallback epoch in simple_tracker to avoid sending bogus AIDTIME before first real RTC sync.
  #define HWT_GPS_RTC_FALLBACK_EPOCH 1715770351UL
#endif

#ifndef HWT_GPS_BOOT_SETTLE_MS
  #define HWT_GPS_BOOT_SETTLE_MS 400
#endif

#ifndef HWT_GPS_CMD_ACK_TIMEOUT_MS
  #define HWT_GPS_CMD_ACK_TIMEOUT_MS 450
#endif

#ifndef HWT_GPS_WAIT_ACK
  #define HWT_GPS_WAIT_ACK 1
#endif

#ifndef HWT_GPS_DEBUG
  #define HWT_GPS_DEBUG 1
#endif

#if HWT_GPS_DEBUG == 1
  #define HWT_GPS_DBG(fmt, ...) Serial.printf("[hwt-gps %10lu] " fmt "\r\n", millis(), ##__VA_ARGS__)
#else
  #define HWT_GPS_DBG(...) do { } while (0)
#endif

// Keep last good position across ESP32 deep sleep so AIDPOS can be replayed on wake.
#if defined(ESP32)
RTC_DATA_ATTR static int32_t gps_aid_lat_e7 = INT32_MIN;
RTC_DATA_ATTR static int32_t gps_aid_lon_e7 = INT32_MIN;
RTC_DATA_ATTR static int32_t gps_aid_alt_cm = INT32_MIN;
#endif

static bool has_valid_geo(double lat, double lon) {
  return lat >= -90.0 && lat <= 90.0
    && lon >= -180.0 && lon <= 180.0
    && !(lat == 0.0 && lon == 0.0);
}

static void save_aid_position(double lat, double lon, double alt_m) {
#if defined(ESP32)
  if (!has_valid_geo(lat, lon)) {
    HWT_GPS_DBG("AIDPOS save skipped: invalid lat/lon lat=%.6f lon=%.6f", lat, lon);
    return;
  }
  gps_aid_lat_e7 = (int32_t)round(lat * 10000000.0);
  gps_aid_lon_e7 = (int32_t)round(lon * 10000000.0);
  gps_aid_alt_cm = (int32_t)round(alt_m * 100.0);
  HWT_GPS_DBG("AIDPOS saved: lat=%.6f lon=%.6f alt=%.1f", lat, lon, alt_m);
#else
  (void)lat;
  (void)lon;
  (void)alt_m;
  HWT_GPS_DBG("AIDPOS save skipped: non-ESP32 build");
#endif
}

static bool load_aid_position(double& lat, double& lon, double& alt_m) {
#if defined(ESP32)
  if (gps_aid_lat_e7 == INT32_MIN || gps_aid_lon_e7 == INT32_MIN || gps_aid_alt_cm == INT32_MIN) {
    HWT_GPS_DBG("AIDPOS load skipped: no retained position");
    return false;
  }
  lat = (double)gps_aid_lat_e7 / 10000000.0;
  lon = (double)gps_aid_lon_e7 / 10000000.0;
  alt_m = (double)gps_aid_alt_cm / 100.0;
  HWT_GPS_DBG("AIDPOS loaded: lat=%.6f lon=%.6f alt=%.1f", lat, lon, alt_m);
  return has_valid_geo(lat, lon);
#else
  (void)lat;
  (void)lon;
  (void)alt_m;
  HWT_GPS_DBG("AIDPOS load skipped: non-ESP32 build");
  return false;
#endif
}

static bool read_uc6580_line(char* out, size_t out_size, uint32_t timeout_ms) {
  if (out_size < 2) {
    return false;
  }
  out[0] = 0;
  size_t len = 0;
  uint32_t start = millis();
  while ((uint32_t)(millis() - start) < timeout_ms) {
    while (Serial1.available() > 0) {
      char c = (char)Serial1.read();
      if (c == '\r') {
        continue;
      }
      if (c == '\n') {
        if (len > 0) {
          out[len] = 0;
          return true;
        }
        continue;
      }
      if (len < (out_size - 1)) {
        out[len++] = c;
      }
    }
    delay(1);
  }
  if (len > 0) {
    out[len] = 0;
    return true;
  }
  return false;
}

enum Uc6580AckResult : uint8_t {
  UC6580_ACK_TIMEOUT = 0,
  UC6580_ACK_OK = 1,
  UC6580_ACK_FAIL = 2,
};

static Uc6580AckResult wait_uc6580_ack(uint32_t timeout_ms) {
  uint32_t start = millis();
  char line[128];
  while ((uint32_t)(millis() - start) < timeout_ms) {
    uint32_t elapsed = (uint32_t)(millis() - start);
    uint32_t remaining = (elapsed < timeout_ms) ? (timeout_ms - elapsed) : 0;
    if (remaining == 0) {
      break;
    }
    if (!read_uc6580_line(line, sizeof(line), remaining)) {
      break;
    }
    HWT_GPS_DBG("UC6580 => %s", line);
    if (strstr(line, "OK") != NULL) {
      return UC6580_ACK_OK;
    }
    if (strstr(line, "ERROR") != NULL || strstr(line, "FAIL") != NULL) {
      return UC6580_ACK_FAIL;
    }
  }
  return UC6580_ACK_TIMEOUT;
}

static bool send_uc6580_sentence(const char* payload, uint32_t ack_timeout_ms = HWT_GPS_CMD_ACK_TIMEOUT_MS) {
  uint8_t checksum = 0;
  for (const char* p = payload; *p; ++p) {
    checksum ^= (uint8_t)(*p);
  }
  char sentence[128];
  snprintf(sentence, sizeof(sentence), "$%s*%02X\r\n", payload, (unsigned)checksum);
  HWT_GPS_DBG("UC6580 <= %s", sentence);
  Serial1.write(sentence);
#if HWT_GPS_WAIT_ACK == 1
  Uc6580AckResult ack = wait_uc6580_ack(ack_timeout_ms);
  if (ack == UC6580_ACK_OK) {
    return true;
  }
  if (ack == UC6580_ACK_FAIL) {
    HWT_GPS_DBG("UC6580 NACK: %s", payload);
  } else {
    HWT_GPS_DBG("UC6580 ack timeout: %s", payload);
  }
  return false;
#else
  (void)ack_timeout_ms;
  return true;
#endif
}

static bool format_nmea_deg(double deg, bool is_lon, char* out, size_t out_size, char& hemi) {
  double v = fabs(deg);
  int whole = (int)v;
  double minutes = (v - (double)whole) * 60.0;
  hemi = is_lon ? (deg < 0 ? 'W' : 'E') : (deg < 0 ? 'S' : 'N');
  if (is_lon) {
    return snprintf(out, out_size, "%03d%010.7f", whole, minutes) > 0;
  }
  return snprintf(out, out_size, "%02d%010.7f", whole, minutes) > 0;
}

static void send_aid_time_if_valid() {
  uint32_t epoch = rtc_clock.getCurrentTime();
  if (epoch < 1577836800UL) { // 2020-01-01
    HWT_GPS_DBG("AIDTIME skipped: rtc epoch invalid (%lu)", (unsigned long)epoch);
    return;
  }
  if (epoch >= (HWT_GPS_RTC_FALLBACK_EPOCH - 86400UL) && epoch <= (HWT_GPS_RTC_FALLBACK_EPOCH + 86400UL)) {
    HWT_GPS_DBG("AIDTIME skipped: rtc epoch still fallback-like (%lu)", (unsigned long)epoch);
    return;
  }

  time_t t = (time_t)epoch;
  struct tm utc_tm;
#if defined(ESP32)
  if (gmtime_r(&t, &utc_tm) == NULL) {
    HWT_GPS_DBG("AIDTIME skipped: gmtime_r failed");
    return;
  }
#else
  struct tm* p = gmtime(&t);
  if (p == NULL) {
    HWT_GPS_DBG("AIDTIME skipped: gmtime failed");
    return;
  }
  utc_tm = *p;
#endif

  char payload[96];
  const unsigned ms = 0; // RTC only stores epoch seconds; send 0ms for assisted time.
  snprintf(payload, sizeof(payload),
    "AIDTIME,%04d,%02d,%02d,%02d,%02d,%02d,%u",
    utc_tm.tm_year + 1900,
    utc_tm.tm_mon + 1,
    utc_tm.tm_mday,
    utc_tm.tm_hour,
    utc_tm.tm_min,
    utc_tm.tm_sec,
    ms);
  if (!send_uc6580_sentence(payload)) {
    HWT_GPS_DBG("AIDTIME send failed");
  }
}

static void send_aid_pos_if_valid() {
  double lat = 0.0, lon = 0.0, alt_m = 0.0;
  if (!load_aid_position(lat, lon, alt_m)) {
    HWT_GPS_DBG("AIDPOS skipped: no valid retained position");
    return;
  }

  char lat_buf[24], lon_buf[24];
  char lat_hemi = 'N', lon_hemi = 'E';
  if (!format_nmea_deg(lat, false, lat_buf, sizeof(lat_buf), lat_hemi)) {
    HWT_GPS_DBG("AIDPOS skipped: lat formatting failed");
    return;
  }
  if (!format_nmea_deg(lon, true, lon_buf, sizeof(lon_buf), lon_hemi)) {
    HWT_GPS_DBG("AIDPOS skipped: lon formatting failed");
    return;
  }

  char payload[96];
  snprintf(payload, sizeof(payload),
    "AIDPOS,%s,%c,%s,%c,%.1f",
    lat_buf,
    lat_hemi,
    lon_buf,
    lon_hemi,
    alt_m);
  if (!send_uc6580_sentence(payload)) {
    HWT_GPS_DBG("AIDPOS send failed");
  }
}

static void apply_uc6580_runtime_config() {
#if HWT_GPS_RECONFIGURE_ON_START == 1
  HWT_GPS_DBG("UC6580 setup start: cfgsys=%d aid=%d", HWT_GPS_SEND_CFGSYS_ON_START, HWT_GPS_SEND_AID_ON_START);
  while (Serial1.available() > 0) {
    Serial1.read();
  }
  HWT_GPS_DBG("UC6580 settle %ums", (unsigned)HWT_GPS_BOOT_SETTLE_MS);
  delay(HWT_GPS_BOOT_SETTLE_MS);

  // Optional: set desired satellite systems (this resets UC6580 receiver state).
#if HWT_GPS_SEND_CFGSYS_ON_START == 1
  if (!send_uc6580_sentence("CFGSYS,h35155", 900)) {
    HWT_GPS_DBG("CFGSYS apply failed");
  }
  delay(750);
#endif

  // Keep only useful NMEA sentences to reduce parser load.
  if (!send_uc6580_sentence("CFGMSG,0,3,0", 900)) { HWT_GPS_DBG("CFGMSG GSV OFF failed"); }
  delay(80);
  if (!send_uc6580_sentence("CFGMSG,0,2,0")) { HWT_GPS_DBG("CFGMSG GSA OFF failed"); }
  delay(80);
  if (!send_uc6580_sentence("CFGMSG,6,0,0")) { HWT_GPS_DBG("CFGMSG TXT0 OFF failed"); }
  delay(80);
  if (!send_uc6580_sentence("CFGMSG,6,1,0")) { HWT_GPS_DBG("CFGMSG TXT1 OFF failed"); }
  delay(80);

#if HWT_GPS_SEND_AID_ON_START == 1
  send_aid_time_if_valid();
  delay(120);
  send_aid_pos_if_valid();
  delay(120);
#endif
  HWT_GPS_DBG("UC6580 setup complete");
#else
  HWT_GPS_DBG("UC6580 setup skipped: HWT_GPS_RECONFIGURE_ON_START=0");
#endif
}

HeltecV3Board board;

#if defined(P_LORA_SCLK)
  static SPIClass spi;
  RADIO_CLASS radio = new Module(P_LORA_NSS, P_LORA_DIO_1, P_LORA_RESET, P_LORA_BUSY, spi);
#else
  RADIO_CLASS radio = new Module(P_LORA_NSS, P_LORA_DIO_1, P_LORA_RESET, P_LORA_BUSY);
#endif

WRAPPER_CLASS radio_driver(radio, board);

ESP32RTCClock fallback_clock;
AutoDiscoverRTCClock rtc_clock(fallback_clock);
MicroNMEALocationProvider nmea = MicroNMEALocationProvider(Serial1);
HWTSensorManager sensors = HWTSensorManager(nmea);

#ifdef DISPLAY_CLASS
  DISPLAY_CLASS display(&board.periph_power);   // peripheral power pin is shared
  MomentaryButton user_btn(PIN_USER_BTN, 1000, true);
#endif

bool radio_init() {
  fallback_clock.begin();
  rtc_clock.begin(Wire);
  
#if defined(P_LORA_SCLK)
  return radio.std_init(&spi);
#else
  return radio.std_init();
#endif

}

uint32_t radio_get_rng_seed() {
  return radio.random(0x7FFFFFFF);
}

void radio_set_params(float freq, float bw, uint8_t sf, uint8_t cr) {
  radio.setFrequency(freq);
  radio.setSpreadingFactor(sf);
  radio.setBandwidth(bw);
  radio.setCodingRate(cr);
}

void radio_set_tx_power(int8_t dbm) {
  radio.setOutputPower(dbm);
}

mesh::LocalIdentity radio_new_identity() {
  RadioNoiseListener rng(radio);
  return mesh::LocalIdentity(&rng);  // create new random identity
}

void HWTSensorManager::start_gps() {
  if (!gps_active) {
    HWT_GPS_DBG("GPS power ON");
    board.periph_power.claim();

    gps_active = true;
    apply_uc6580_runtime_config();
  } else {
    HWT_GPS_DBG("GPS start ignored: already active");
  }
}

void HWTSensorManager::stop_gps() {
  if (gps_active) {
    HWT_GPS_DBG("GPS power OFF");
    double aid_lat = node_lat;
    double aid_lon = node_lon;
    double aid_alt = node_altitude;

    if (_location && _location->isValid()) {
      long raw_lat = _location->getLatitude();
      long raw_lon = _location->getLongitude();
      long raw_alt = _location->getAltitude();
      if (!(raw_lat == 0 && raw_lon == 0)) {
        aid_lat = ((double)raw_lat) / 1000000.0;
        aid_lon = ((double)raw_lon) / 1000000.0;
        aid_alt = ((double)raw_alt) / 1000.0;
        HWT_GPS_DBG("AIDPOS source: provider live lat=%.6f lon=%.6f alt=%.1f", aid_lat, aid_lon, aid_alt);
      } else {
        HWT_GPS_DBG("AIDPOS source: provider invalid coords, fallback to cached node_*");
      }
    } else {
      HWT_GPS_DBG("AIDPOS source: no live provider fix, fallback to cached node_*");
    }

    save_aid_position(aid_lat, aid_lon, aid_alt);
    gps_active = false;

    board.periph_power.release();
  } else {
    HWT_GPS_DBG("GPS stop ignored: already inactive");
  }
}

bool HWTSensorManager::begin() {
  // init GPS port
  Serial1.begin(115200, SERIAL_8N1, PIN_GPS_RX, PIN_GPS_TX);
  return true;
}

bool HWTSensorManager::querySensors(uint8_t requester_permissions, CayenneLPP& telemetry) {
  if (requester_permissions & TELEM_PERM_LOCATION) {   // does requester have permission?
    telemetry.addGPS(TELEM_CHANNEL_SELF, node_lat, node_lon, node_altitude);
  }
  return true;
}

void HWTSensorManager::loop() {
  static long next_gps_update = 0;

  _location->loop();

  if (millis() > next_gps_update) {
    if (gps_active && _location->isValid()) {
      node_lat = ((double)_location->getLatitude())/1000000.;
      node_lon = ((double)_location->getLongitude())/1000000.;
      node_altitude = ((double)_location->getAltitude()) / 1000.0;
      MESH_DEBUG_PRINTLN("lat %f lon %f", node_lat, node_lon);
    }
    next_gps_update = millis() + 1000;
  }
}

int HWTSensorManager::getNumSettings() const { return 1; }  // just one supported: "gps" (power switch)

const char* HWTSensorManager::getSettingName(int i) const {
  return i == 0 ? "gps" : NULL;
}
const char* HWTSensorManager::getSettingValue(int i) const {
  if (i == 0) {
    return gps_active ? "1" : "0";
  }
  return NULL;
}
bool HWTSensorManager::setSettingValue(const char* name, const char* value) {
  if (strcmp(name, "gps") == 0) {
    if (strcmp(value, "0") == 0) {
      stop_gps();
    } else {
      start_gps();
    }
    return true;
  }
  return false;  // not supported
}
