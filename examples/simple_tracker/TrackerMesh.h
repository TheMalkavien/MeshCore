#pragma once

#include <limits.h>

#include "tracker_config.h"

class TrackerMesh : public SensorMesh {
public:
  TrackerMesh(mesh::MainBoard& board, mesh::Radio& radio, mesh::MillisecondClock& ms, mesh::RNG& rng, mesh::RTCClock& rtc, mesh::MeshTables& tables);

  void begin(FILESYSTEM* fs);
  void queueImmediateCycle(const char* origin = "manual");
  void loop();

protected:
  void onSensorDataRead() override;
  int querySeriesData(uint32_t start_secs_ago, uint32_t end_secs_ago, MinMaxAvg dest[], int max_num) override;
  bool handleCustomCommand(uint32_t sender_timestamp, char* command, char* reply) override;

private:
  enum {
    GROUP_NAME_SIZE = 24,
    GROUP_PSK_SIZE = 56,
  };

  struct TrackerConfigV1 {
    uint32_t magic;
    uint16_t version;
    uint16_t reserved;
    uint32_t interval_secs;
    uint32_t gps_timeout_secs;
    uint8_t sleep_enabled;
    uint8_t group_psk_is_default;
    uint8_t reserved2[2];
    char group_name[GROUP_NAME_SIZE];
    char group_psk[GROUP_PSK_SIZE];
  };

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
    long raw_speed = LONG_MIN;
    long raw_course = LONG_MIN;
  };

  static const uint32_t TRACKER_CONFIG_MAGIC = 0x544B5231UL;
  static const uint16_t TRACKER_CONFIG_VERSION = 1;

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
  char _group_name[GROUP_NAME_SIZE] = TRACKER_DEFAULT_GROUP_NAME;
  char _group_psk[GROUP_PSK_SIZE] = TRACKER_DEFAULT_GROUP_PSK;
  bool _external_power_paused = false;
  bool _tracking_in_progress = false;
  unsigned long _tracking_started_millis = 0;
  unsigned long _last_wait_log_millis = 0;
  FILESYSTEM* _cfg_fs = NULL;
  unsigned long _live_fix_since_millis = 0;
  bool _last_live_fix_state = false;
  bool _cycle_has_gps_snapshot = false;
  long _cycle_start_gps_ts = LONG_MIN;
  long _cycle_start_raw_lat = LONG_MIN;
  long _cycle_start_raw_lon = LONG_MIN;

  void pauseTrackingForExternalPower();
  bool handleExternalPowerPolicy();
  bool handleTrackerStatusCommand(char* command, char* reply);
  bool handleTrackerConfigCommand(char* command, char* reply);
  bool handleTrackerGroupCommand(char* command, char* reply);
  bool handleTrackerActionCommand(char* command, char* reply);

  GPSState getGPSState() const;
  bool gpsSpeedValid(long raw_speed) const;
  bool gpsCourseValid(long raw_course) const;
  float gpsSpeedKmh(long raw_speed) const;
  float gpsCourseDeg(long raw_course) const;
  void updateLiveFixState(bool gps_live_fix);
  uint32_t getLiveFixAgeSecs() const;
  void formatGPSStatus(char* reply, size_t reply_len);
  void syncRTCFromGPS(const GPSState& gps);

  void applyGroupPolicyGuard();
  File openConfigForRead();
  File openConfigForWrite();
  bool persistTrackerConfig();
  bool loadTrackerConfig();
  bool hasStoredPosition() const;

  static int hexNibble(char c);
  static bool isHexString(const char* s, size_t n);
  static bool hexToBytes(const char* s, size_t n, uint8_t* out);
  bool decodeGroupSecret(const char* input, uint8_t* out_secret, int& out_len, char* canonical_b64, size_t canonical_b64_len);
  bool configureGroupChannel(const char* key_input, char* canonical_b64 = NULL, size_t canonical_b64_len = 0);

  void sendTrackerFixJson(float lat, float lon, float alt, int sats, bool speed_ok, float speed_kmh, bool course_ok, float course_deg, const char* fix_mode, uint32_t fix_secs);
  void sendTrackerEventJson(const char* event_name, const char* reason = NULL);
  void sendGroupText(const char* text);
  void enterTrackerSleep(uint32_t secs);
  void scheduleNextCycle();
  void handleSleepQueueDrain();
  void startTrackingCycle();
  void completeTrackingCycle();
  void pollTrackingCycle();
};
