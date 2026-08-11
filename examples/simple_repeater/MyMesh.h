#pragma once

#include <Arduino.h>
#include <Mesh.h>
#include <RTClib.h>
#include <target.h>

#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
  #include <InternalFileSystem.h>
#elif defined(RP2040_PLATFORM)
  #include <LittleFS.h>
#elif defined(ESP32)
  #include <SPIFFS.h>
  using File = fs::File;
#endif

#ifdef WITH_RS232_BRIDGE
#include "helpers/bridges/RS232Bridge.h"
#define WITH_BRIDGE
#endif

#ifdef WITH_ESPNOW_BRIDGE
#include "helpers/bridges/ESPNowBridge.h"
#define WITH_BRIDGE
#endif

#include <helpers/AdvertDataHelpers.h>
#include <helpers/ArduinoHelpers.h>
#include <helpers/ClientACL.h>
#include <helpers/CommonCLI.h>
#include <helpers/IdentityStore.h>
#include <helpers/SimpleMeshTables.h>
#include <helpers/StaticPoolPacketManager.h>
#include <helpers/StatsFormatHelper.h>
#include <helpers/TxtDataHelpers.h>
#include <helpers/RegionMap.h>
#include "RateLimiter.h"

#ifdef WITH_BRIDGE
extern AbstractBridge* bridge;
#endif

struct RepeaterStats {
  uint16_t batt_milli_volts;
  uint16_t curr_tx_queue_len;
  int16_t  noise_floor;
  int16_t  last_rssi;
  uint32_t n_packets_recv;
  uint32_t n_packets_sent;
  uint32_t total_air_time_secs;
  uint32_t total_up_time_secs;
  uint32_t n_sent_flood, n_sent_direct;
  uint32_t n_recv_flood, n_recv_direct;
  uint16_t err_events;                // was 'n_full_events'
  int16_t  last_snr;   // x 4
  uint16_t n_direct_dups, n_flood_dups;
  uint32_t total_rx_air_time_secs;
  uint32_t n_recv_errors;
};

#ifndef MAX_CLIENTS
  #define MAX_CLIENTS           32
#endif

struct NeighbourInfo {
  mesh::Identity id;
  uint32_t advert_timestamp;
  uint32_t heard_timestamp;
  int8_t snr; // multiplied by 4, user should divide to get float value
};

struct PendingPing {
  // --- multi-ping session state (a run of 1..count pings) ---
  bool session_active;       // a ping session is in progress
  bool reply_remote;         // results go to a remote admin client (async) vs local serial (blocking)
  mesh::Identity target;
  mesh::Identity requester;
  uint8_t requester_path_hash_size;
  char cli_prefix[10];
  uint16_t count;            // total pings requested
  uint16_t seq;              // current ping number (1-based) = pings sent so far
  uint16_t recv;             // successful responses so far
  unsigned long next_at;     // remote pacing: when to fire the next ping (0 = none scheduled)

  // --- aggregate stats over the session ---
  unsigned long rtt_sum, rtt_min, rtt_max;   // ms
  int32_t lsnr_sum;  int8_t lsnr_min, lsnr_max;  // local SNR (snr_rx), multiplied by 4
  int32_t rsnr_sum;  int8_t rsnr_min, rsnr_max;  // remote SNR (snr_tx), multiplied by 4

  // --- current in-flight ping ---
  bool active;               // a single ping is awaiting its trace response
  bool success;              // last in-flight ping succeeded
  uint32_t tag;
  unsigned long started_at;
  unsigned long expiry_at;
  unsigned long last_rtt;    // rtt (ms) of the last successful ping
  int8_t remote_snr;         // multiplied by 4
  int8_t local_snr;          // multiplied by 4
};

#define TRACE_MIN_PREFIX_SIZE        2
#define TRACE_RESPONDER_HASH_SIZE    6
#define TRACE_DISCOVERY_PROBES       3
#define TRACE_MAX_RESULTS            TRACE_DISCOVERY_PROBES
#define TRACE_TRANSIENT_PEER_INDEX  -1

#define TRACE_PHASE_IDLE        0
#define TRACE_PHASE_PROBE       1   // flood discovery probes are being sent/collected
#define TRACE_PHASE_DONE        2
#define TRACE_PHASE_REPORT      3   // remote result lines are being queued one at a time

#define TRACE_FAIL_NONE         0
#define TRACE_FAIL_NO_ROUTE     1   // no authenticated PATH response arrived during the window
#define TRACE_FAIL_POOL         2
#define TRACE_FAIL_TX           3   // local radio queue timeout or physical send failure

// One unique legacy blank-login flood probe. The encrypted payload hash identifies the queued
// packet; the PATH response itself does not reflect the request tag.
struct TraceProbeRec {
  uint8_t packet_hash[MAX_HASH_SIZE];
  uint8_t response_hash[MAX_HASH_SIZE];
  unsigned long expires_at;
  bool active;
  bool transmitted;             // expires_at is armed by logTx(), not queue time
  bool response_seen;            // rejects the same PATH again after seen-table eviction
};

// One real request/response path pair. Both path_len values use MeshCore's packed normal
// flood descriptor and the byte arrays contain relay hashes only (not either endpoint).
struct TraceResult {
  uint8_t responder[TRACE_RESPONDER_HASH_SIZE];
  uint8_t out_path_len;
  uint8_t out_path[MAX_PATH_SIZE];
  uint8_t back_path_len;
  uint8_t back_path[MAX_PATH_SIZE];
  uint8_t replies;
  int8_t last_snr;               // x4, final response link into this node
};

// A trace session discovers routes without knowing any relay in advance. Three standard blank
// login ANON_REQ floods each provoke a PATH on a compatible target. Every PATH embeds the request's
// outward path, and its own packet.path supplies the observed return.
struct PendingTrace {
  bool session_active;
  uint8_t phase;                 // TRACE_PHASE_*
  uint8_t fail;                  // TRACE_FAIL_*, when the session ends without a result
  bool reply_remote;             // results go to a remote admin client (async) vs local serial (blocking)
  bool success;
  mesh::Identity target;
  uint8_t target_secret[PUB_KEY_SIZE];
  mesh::Identity requester;
  uint8_t requester_path_hash_size;
  TransportKey requester_scope;
  bool requester_scope_valid;
  char cli_prefix[10];

  TraceResult results[TRACE_MAX_RESULTS];
  uint8_t routes_found;
  uint8_t responses_received;

  // --- three unique flood probes, paced from actual radio transmission ---
  uint8_t next_probe;
  uint8_t probes_sent;
  unsigned long next_probe_at;
  unsigned long deadline;        // when the last probe still in flight gives up
  bool has_deadline;              // deadline may legitimately equal zero at millis() wrap
  unsigned long hard_deadline;   // bounds queue congestion and the complete probe phase
  bool tx_pending;                // exactly one discovery probe is queued but not yet transmitted
  TraceProbeRec sent[TRACE_DISCOVERY_PROBES];
  uint8_t sent_count;
  uint8_t pool_misses;
  uint8_t tx_failures;
  unsigned long started_at;
  uint32_t elapsed_ms;           // frozen when probing ends; excludes paced remote reporting

  // --- paced remote report; lines are generated from results[] and retried on pool pressure ---
  uint8_t report_candidate;
  uint8_t report_part;
  uint8_t report_path_pos;
  unsigned long next_report_at;
  unsigned long report_deadline;
};

#ifndef FIRMWARE_BUILD_DATE
  #define FIRMWARE_BUILD_DATE   "9 Aug 2026"
#endif

#ifndef FIRMWARE_VERSION
  #define FIRMWARE_VERSION   "v1.17.0"
#endif

#define FIRMWARE_ROLE "repeater"

#define PACKET_LOG_FILE  "/packet_log"

class MyMesh : public mesh::Mesh, public CommonCLICallbacks {
  struct FloodRetryEntry {
    uint8_t active;
    uint8_t retries_sent;
    uint8_t priority;
    uint8_t raw_len;
    uint8_t hash[MAX_HASH_SIZE];
    uint8_t raw[MAX_TRANS_UNIT];
    unsigned long created_at;
    unsigned long next_retry_at;
    uint32_t wait_ms;
  };

  // Extensible custom prefs stored in /other_prefs, independent of NodePrefs.
  // Add new fields at the end only; bump OTHER_PREFS_VERSION on breaking changes.
  struct OtherPrefs {
    uint8_t  flood_max_retries; // max retransmit attempts per flood packet
    uint16_t flood_timeout_ms;  // minimum confirm window before first retry (ms)
  };

  FILESYSTEM* _fs;
  uint32_t last_millis;
  uint64_t uptime_millis;
  unsigned long next_local_advert, next_flood_advert;
  bool _logging;
  NodePrefs _prefs;
  ClientACL  acl;
  CommonCLI _cli;
  uint8_t reply_data[MAX_PACKET_PAYLOAD];
  uint8_t reply_path[MAX_PATH_SIZE];
  uint8_t reply_path_len;
  TransportKeyStore key_store;
  RegionMap region_map, temp_map;
  RegionEntry* load_stack[8];
  RegionEntry* recv_pkt_region;
  TransportKey default_scope;
  RateLimiter discover_limiter, anon_limiter;
  uint32_t pending_discover_tag;
  unsigned long pending_discover_until;
  bool region_load_active;
  unsigned long dirty_contacts_expiry;
#if MAX_NEIGHBOURS
  NeighbourInfo neighbours[MAX_NEIGHBOURS];
#endif
  PendingPing pending_ping;
  PendingTrace pending_trace;
  CayenneLPP telemetry;
  unsigned long set_radio_at, revert_radio_at;
  float pending_freq;
  float pending_bw;
  uint8_t pending_sf;
  uint8_t pending_cr;
  // One extra slot lets a full-key trace target act as a transient peer without changing the ACL.
  int  matching_peer_indexes[MAX_CLIENTS + 1];
  ClientInfo* active_cli_client;
  uint8_t active_cli_path_hash_size;
  FloodRetryEntry _flood_retry[8];
  uint32_t _flood_retry_tracked;
  uint32_t _flood_retry_confirmed;
  uint32_t _flood_retry_failed;
  uint32_t _flood_retry_retransmits;
  OtherPrefs _other_prefs;
#if defined(WITH_RS232_BRIDGE)
  RS232Bridge bridge;
#elif defined(WITH_ESPNOW_BRIDGE)
  ESPNowBridge bridge;
#endif

  void putNeighbour(const mesh::Identity& id, uint32_t timestamp, float snr);
  void trackFloodForward(const mesh::Packet* pkt, mesh::DispatcherAction action);
  void markFloodHeard(const mesh::Packet* pkt);
  void processFloodRetries();
  void clearFloodRetryState();
  void loadOtherPrefs();
  void saveOtherPrefs();
  void onFloodQueued(const mesh::Packet* packet, uint8_t priority, uint32_t delay_ms) override;
  bool resolvePingTarget(const char* destination, mesh::Identity& target, char* error_reply);
  bool startPingSession(const mesh::Identity& target, uint16_t count, bool reply_remote, const char* cli_prefix, char* error_reply);
  bool firePing(char* error_reply);
  void accumulatePingStats();
  void completePingAndAdvance();
  void sendRemotePingLine(bool summary);
  void endPingSession();
  void runLocalPingSession(char* reply);
  void formatPingLine(char* out) const;
  void formatPingSummary(char* out) const;
  bool sendRemoteCliLine(const mesh::Identity& requester, uint8_t path_hash_size, const char* cli_prefix,
                         const char* text, uint32_t delay_millis, const TransportKey* scope = NULL);
  void beginTraceSession(bool reply_remote, const char* cli_prefix);
  bool resolveTraceTarget(const char* destination, mesh::Identity& target, char* error_reply);
  bool startTraceSession(const char* destination, bool reply_remote, const char* cli_prefix, char* error_reply);
  mesh::Packet* createTraceDiscoveryProbe(uint32_t tag);
  bool isPendingTraceProbe(const mesh::Packet* packet, bool unsent_only) const;
  void recordTraceResponse(mesh::Packet* packet, const mesh::Identity& responder,
                           const uint8_t* out_path, uint8_t out_path_len);
  void driveTrace();
  void sendNextTraceProbe();
  void noteTraceTx(mesh::Packet* packet, bool success);
  void cancelPendingTraceTx();
  void finishTrace();
  bool emitTraceProgress(const char* line, bool remote_too);
  void formatTraceRouteHeader(char* out, const TraceResult& result, uint8_t route_no) const;
  uint8_t formatTracePathChunk(char* out, size_t out_sz, const TraceResult& result,
                               bool back, uint8_t start) const;
  void formatTraceSummary(char* out) const;
  void emitTraceLine(const char* line, char* reply);
  bool sendNextTraceReportLine();
  void reportTrace(char* reply);
  void endTraceSession();
  void runLocalTraceSession(char* reply);
  uint8_t handleLoginReq(const mesh::Identity& sender, const uint8_t* secret, uint32_t sender_timestamp, const uint8_t* data, bool is_flood);
  uint8_t handleAnonRegionsReq(const mesh::Identity& sender, uint32_t sender_timestamp, const uint8_t* data);
  uint8_t handleAnonOwnerReq(const mesh::Identity& sender, uint32_t sender_timestamp, const uint8_t* data);
  uint8_t handleAnonClockReq(const mesh::Identity& sender, uint32_t sender_timestamp, const uint8_t* data);
  int handleRequest(ClientInfo* sender, uint32_t sender_timestamp, uint8_t* payload, size_t payload_len);
  mesh::Packet* createSelfAdvert();

  File openAppend(const char* fname);
  bool isLooped(const mesh::Packet* packet, const uint8_t max_counters[]);

protected:
  float getAirtimeBudgetFactor() const override {
    return _prefs.airtime_factor;
  }

  mesh::DispatcherAction onRecvPacket(mesh::Packet* pkt) override;
  uint32_t nextAppWake(uint32_t now) const override;
  bool allowPacketForward(const mesh::Packet* packet) override;
  const char* getLogDateTime() override;
  void logRxRaw(float snr, float rssi, const uint8_t raw[], int len) override;

  void logRx(mesh::Packet* pkt, int len, float score) override;
  void logTx(mesh::Packet* pkt, int len) override;
  void logTxFail(mesh::Packet* pkt, int len) override;
  int calcRxDelay(float score, uint32_t air_time) const override;

  uint32_t getRetransmitDelay(const mesh::Packet* packet) override;
  uint32_t getDirectRetransmitDelay(const mesh::Packet* packet) override;

  int getInterferenceThreshold() const override {
    return _prefs.interference_threshold;
  }
  bool getCADEnabled() const override {
    return _prefs.cad_enabled;
  }
  int getAGCResetInterval() const override;   // defined in .cpp: suspended during OTA
  uint32_t getCADFailRetryDelay() const override;
  uint32_t getCADFailMaxDuration() const override;
  uint8_t getExtraAckTransmitCount() const override {
    return _prefs.multi_acks;
  }

#if ENV_INCLUDE_GPS == 1
  void applyGpsPrefs() {
    sensors.setSettingValue("gps", _prefs.gps_enabled?"1":"0");
  }
#endif

  void onAnonDataRecv(mesh::Packet* packet, const uint8_t* secret, const mesh::Identity& sender, uint8_t* data, size_t len) override;
  int searchPeersByHash(const uint8_t* hash) override;
  void getPeerSharedSecret(uint8_t* dest_secret, int peer_idx) override;
  void onTraceRecv(mesh::Packet* packet, uint32_t tag, uint32_t auth_code, uint8_t flags,
                   const uint8_t* path_snrs, const uint8_t* path_hashes, uint8_t path_len) override;
  void onAdvertRecv(mesh::Packet* packet, const mesh::Identity& id, uint32_t timestamp, const uint8_t* app_data, size_t app_data_len);
  void onPeerDataRecv(mesh::Packet* packet, uint8_t type, int sender_idx, const uint8_t* secret, uint8_t* data, size_t len) override;
  bool onPeerPathRecv(mesh::Packet* packet, int sender_idx, const uint8_t* secret, uint8_t* path, uint8_t path_len, uint8_t extra_type, uint8_t* extra, uint8_t extra_len) override;
  void onControlDataRecv(mesh::Packet* packet) override;

  void sendFloodReply(mesh::Packet* packet, unsigned long delay_millis, uint8_t path_hash_size);

public:
  MyMesh(mesh::MainBoard& board, mesh::Radio& radio, mesh::MillisecondClock& ms, mesh::RNG& rng, mesh::RTCClock& rtc, mesh::MeshTables& tables);

  void begin(FILESYSTEM* fs);
  void sendNodeDiscoverReq();
  const char* getFirmwareVer() override { return FIRMWARE_VERSION; }
  const char* getBuildDate() override { return FIRMWARE_BUILD_DATE; }
  const char* getRole() override { return FIRMWARE_ROLE; }
  const char* getNodeName() { return _prefs.node_name; }
  NodePrefs* getNodePrefs() {
    return &_prefs;
  }

  void savePrefs() override {
    _cli.savePrefs(_fs);
  }

  void sendFloodScoped(const TransportKey& scope, mesh::Packet* pkt, uint32_t delay_millis, uint8_t path_hash_size);

  // CommonCLICallbacks
  void applyTempRadioParams(float freq, float bw, uint8_t sf, uint8_t cr, int timeout_mins) override;
  bool formatFileSystem() override;
  void sendSelfAdvertisement(int delay_millis, bool flood) override;
  void updateAdvertTimer() override;
  void updateFloodAdvertTimer() override;

  void setLoggingOn(bool enable) override { _logging = enable; }

  void eraseLogFile() override {
    _fs->remove(PACKET_LOG_FILE);
  }

  void dumpLogFile() override;
  void setTxPower(int8_t power_dbm) override;
  void formatNeighborsReply(char *reply) override;
  void removeNeighbor(const uint8_t* pubkey, int key_len) override;
  void formatStatsReply(char *reply) override;
  void formatRadioStatsReply(char *reply) override;
  void formatPacketStatsReply(char *reply) override;
  void startRegionsLoad() override;
  bool saveRegions() override;
  void onDefaultRegionChanged(const RegionEntry* r) override;
  void formatFloodStatsReply(char *reply) override;

  mesh::LocalIdentity& getSelfId() override { return self_id; }

  void saveIdentity(const mesh::LocalIdentity& new_id) override;
  void clearStats() override;

  void handleCommand(uint32_t sender_timestamp, char* command, char* reply);
  void loop();

#if defined(WITH_BRIDGE)
  void setBridgeState(bool enable) override {
    if (enable == bridge.isRunning()) return;
    if (enable)
    {
      bridge.begin();
    }
    else 
    {
      bridge.end();
    }
  }

  void restartBridge() override {
    if (!bridge.isRunning()) return;
    bridge.end();
    bridge.begin();
  }
#endif

  // To check if there is pending work
  bool hasPendingWork() const;

  bool setRxBoostedGain(bool enable) override;

  #if defined(USE_LR2021)
  virtual bool configSideDetectors(const uint8_t sideDetSFs[], uint8_t num, float bw) override;
  #endif

};
