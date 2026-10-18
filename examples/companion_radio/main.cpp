#include <Arduino.h>   // needed for PlatformIO
#include <Mesh.h>
#include "MyMesh.h"
#include <limits.h>

#ifndef TRACKER_HYBRID
  #define TRACKER_HYBRID 0
#endif

#if TRACKER_HYBRID == 1
  #ifndef TRACKER_DEFAULT_INTERVAL_SECS
    #define TRACKER_DEFAULT_INTERVAL_SECS 1800
  #endif
  #ifndef TRACKER_DEFAULT_GPS_TIMEOUT_SECS
    #define TRACKER_DEFAULT_GPS_TIMEOUT_SECS 180
  #endif
  #ifndef TRACKER_BOOT_GRACE_SECS
    #define TRACKER_BOOT_GRACE_SECS 20
  #endif
  #ifndef TRACKER_GROUP_NAME
    #define TRACKER_GROUP_NAME "tracker"
  #endif
  #ifndef TRACKER_GROUP_PSK
    #define TRACKER_GROUP_PSK ""
  #endif
  #ifndef TRACKER_HYBRID_SLEEP_ENABLED
    #define TRACKER_HYBRID_SLEEP_ENABLED 0
  #endif
#endif

// Believe it or not, this std C function is busted on some platforms!
static uint32_t _atoi(const char* sp) {
  uint32_t n = 0;
  while (*sp && *sp >= '0' && *sp <= '9') {
    n *= 10;
    n += (*sp++ - '0');
  }
  return n;
}

// interface manager
#include <helpers/MultiSerialInterface.h>
MultiSerialInterface interface_manager;

// include bluetooth interface
#if defined(BLE_PIN_CODE)
  #ifdef ESP32
    // include esp32 bluetooth interface
    #include <helpers/esp32/SerialBLEInterface.h>
    SerialBLEInterface bluetooth_interface;
  #elif defined(NRF52_PLATFORM)
    // include nrf52 bluetooth interface
    #include <helpers/nrf52/SerialBLEInterface.h>
    SerialBLEInterface bluetooth_interface;
  #elif defined(RP2040_PLATFORM)
    // include rp2040 (Pico W / CYW43) bluetooth interface
    #include <helpers/rp2040/SerialBLEInterface.h>
    SerialBLEInterface bluetooth_interface;
  #else
    #error "SerialBLEInterface is not defined for this platform"
  #endif
#endif

// include wifi interface
#ifdef ENABLE_WIFI_INTERFACE
  #ifndef WIFI_SSID
    #define WIFI_SSID ""
  #endif
  #ifndef WIFI_PWD
    #define WIFI_PWD ""
  #endif
  #ifndef TCP_PORT
    #define TCP_PORT 5000
  #endif
  #ifndef WIFI_RETRY_INTERVAL
    #if defined(RP2040_PLATFORM)
      #define WIFI_RETRY_INTERVAL 30000   // each attempt blocks loop(), so retry less often
    #else
      #define WIFI_RETRY_INTERVAL 10000   // millis between reconnect attempts
    #endif
  #endif
  #ifndef WIFI_RETRY_TIMEOUT
    #define WIFI_RETRY_TIMEOUT 5000     // RP2040: cap on how long one join may block loop()
  #endif
  #if defined(ESP32) || defined(RP2040_PLATFORM)
    #include <helpers/wifi/SerialWifiInterface.h>
    SerialWifiInterface wifi_interface;
  #else
    #error "SerialWifiInterface is not defined for this platform"
  #endif
#endif

// include usb interface
#if defined(ENABLE_USB_INTERFACE)
  #include <helpers/ArduinoSerialInterface.h>
  ArduinoSerialInterface usb_serial_interface;
#endif

// include ethernet interface
#if defined(ETHERNET_ENABLED)
  #include <helpers/ethernet/EthernetInterface.h>
  ETHERNET_CLASS ethernet_interface;
#endif

// include hardware serial interface
#if defined(SERIAL_RX)
  #include <helpers/ArduinoSerialInterface.h>
  ArduinoSerialInterface hardware_serial_interface;
  HardwareSerial companion_serial(1);
#endif

// platform file system
#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
  #include <InternalFileSystem.h>
  #if defined(QSPIFLASH)
    #include <CustomLFS_QSPIFlash.h>
    DataStore store(InternalFS, QSPIFlash, rtc_clock);
  #else
    #if defined(EXTRAFS)
      #include <CustomLFS.h>
      CustomLFS ExtraFS(0xD4000, 0x19000, 128);
      DataStore store(InternalFS, ExtraFS, rtc_clock);
    #else
      DataStore store(InternalFS, rtc_clock);
    #endif
  #endif
#elif defined(RP2040_PLATFORM)
  #include <LittleFS.h>
  DataStore store(LittleFS, rtc_clock);
#elif defined(ESP32)
  #include <SPIFFS.h>
  DataStore store(SPIFFS, rtc_clock);
#endif

/* GLOBAL OBJECTS */
#ifdef DISPLAY_CLASS
  #include "UITask.h"
  UITask ui_task(&board, &interface_manager);
#endif

StdRNG fast_rng;
SimpleMeshTables tables;
MyMesh the_mesh(radio_driver, fast_rng, rtc_clock, tables, store);

#if TRACKER_HYBRID == 1
class TrackerHybridTask {
  bool _tracking_in_progress = false;
  unsigned long _next_measure_millis = 0;
  unsigned long _tracking_started_millis = 0;
  char _group_name[32] = TRACKER_GROUP_NAME;
  char _group_psk[56] = TRACKER_GROUP_PSK;

  bool hasStoredPosition() const {
    const double lat = sensors.node_lat;
    const double lon = sensors.node_lon;
    return lat >= -90.0 && lat <= 90.0
      && lon >= -180.0 && lon <= 180.0
      && !(lat == 0.0 && lon == 0.0);
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

  void persistGroupName() {
    const uint8_t key[] = {'t', 'r', 'k', 'g', 'r', 'p'};
    uint8_t len = (uint8_t)strnlen(_group_name, sizeof(_group_name) - 1);
    store.putBlobByKey(key, sizeof(key), (const uint8_t*)_group_name, len);
  }

  void persistGroupPSK() {
    const uint8_t key[] = {'t', 'r', 'k', 'g', 'p', 's', 'k'};
    uint8_t len = (uint8_t)strnlen(_group_psk, sizeof(_group_psk) - 1);
    store.putBlobByKey(key, sizeof(key), (const uint8_t*)_group_psk, len);
  }

  void loadPersistedGroupName() {
    const uint8_t key[] = {'t', 'r', 'k', 'g', 'r', 'p'};
    uint8_t temp[sizeof(_group_name)] = {0};
    uint8_t len = store.getBlobByKey(key, sizeof(key), temp);
    if (len == 0) {
      return;
    }
    int copy_len = (len < sizeof(_group_name)) ? len : (sizeof(_group_name) - 1);
    temp[copy_len] = 0;
    setGroupName((const char*)temp);
  }

  void loadPersistedGroupPSK() {
    const uint8_t key[] = {'t', 'r', 'k', 'g', 'p', 's', 'k'};
    uint8_t temp[sizeof(_group_psk)] = {0};
    uint8_t len = store.getBlobByKey(key, sizeof(key), temp);
    if (len == 0) {
      return;
    }
    int copy_len = (len < sizeof(_group_psk)) ? len : (sizeof(_group_psk) - 1);
    temp[copy_len] = 0;
    setGroupPSK((const char*)temp);
  }

  uint32_t getIntervalSecs() const {
    NodePrefs* prefs = the_mesh.getNodePrefs();
    if (prefs && prefs->gps_interval >= 30 && prefs->gps_interval <= 86400) {
      return prefs->gps_interval;
    }
    return TRACKER_DEFAULT_INTERVAL_SECS;
  }

  bool isTrackerEnabled() const {
    NodePrefs* prefs = the_mesh.getNodePrefs();
    return !prefs || prefs->gps_enabled != 0;
  }

  void scheduleWhenDisabled() {
    _next_measure_millis = millis() + 5000UL;
  }

  void scheduleNextCycle() {
    uint32_t interval_secs = getIntervalSecs();
#if TRACKER_HYBRID_SLEEP_ENABLED == 1
    board.sleep(interval_secs);
    _next_measure_millis = millis() + 1500UL;
#else
    _next_measure_millis = millis() + interval_secs * 1000UL;
#endif
  }

  bool resolveTrackerChannel(ChannelDetails& channel) const {
#ifdef MAX_GROUP_CHANNELS
    for (int i = 0; i < MAX_GROUP_CHANNELS; i++) {
      ChannelDetails candidate;
      if (the_mesh.getChannel(i, candidate) && candidate.name[0] && strcmp(candidate.name, _group_name) == 0) {
        channel = candidate;
        return true;
      }
    }
#endif
    return false;
  }

  bool ensureTrackerChannel(ChannelDetails& channel) {
    if (resolveTrackerChannel(channel)) {
      return true;
    }
    if (_group_psk[0] == 0) {
      return false;
    }

    ChannelDetails* added = the_mesh.addChannel(_group_name, _group_psk);
    if (added) {
      the_mesh.persistChannels();
      channel = *added;
      return true;
    }

    return resolveTrackerChannel(channel);
  }

  void sendTrackerText(const char* text) {
    ChannelDetails channel;
    if (!ensureTrackerChannel(channel)) {
      return;
    }

    const char* sender_name = "tracker";
    NodePrefs* prefs = the_mesh.getNodePrefs();
    if (prefs && prefs->node_name[0]) {
      sender_name = prefs->node_name;
    }

    uint32_t timestamp = the_mesh.getRTCClock()->getCurrentTimeUnique();
    the_mesh.sendGroupMessage(timestamp, channel.channel, sender_name, text, strlen(text));
  }

  void startTrackingCycle() {
    if (!isTrackerEnabled()) {
      scheduleWhenDisabled();
      return;
    }

    _tracking_in_progress = true;
    _tracking_started_millis = millis();
#if ENV_INCLUDE_GPS == 1
    sensors.setSettingValue("gps", "1");
#endif
  }

  void completeTrackingCycle() {
    _tracking_in_progress = false;
    scheduleNextCycle();
  }

  void pollTrackingCycle() {
#if ENV_INCLUDE_GPS == 1
    LocationProvider* location = sensors.getLocationProvider();
    bool gps_enabled = (location && location->isEnabled());
    bool gps_live_fix = (gps_enabled && location->isValid());
    bool gps_cached_fix = hasStoredPosition();
    if (gps_live_fix || gps_cached_fix) {
      int sats = (gps_live_fix && location) ? location->satellitesCount() : -1;
      bool speed_ok = (gps_live_fix && location && gpsSpeedValid(location->getSpeed()));
      bool course_ok = (gps_live_fix && location && gpsCourseValid(location->getCourse()));
      char speed_txt[12];
      char course_txt[12];
      if (speed_ok) {
        snprintf(speed_txt, sizeof(speed_txt), "%.1f", gpsSpeedKmh(location->getSpeed()));
      } else {
        StrHelper::strncpy(speed_txt, "n/a", sizeof(speed_txt));
      }
      if (course_ok) {
        snprintf(course_txt, sizeof(course_txt), "%.1f", gpsCourseDeg(location->getCourse()));
      } else {
        StrHelper::strncpy(course_txt, "n/a", sizeof(course_txt));
      }

      char text[160];
      snprintf(text, sizeof(text), "GPS lat=%.6f lon=%.6f alt=%.1fm sats=%d spd=%skm/h dir=%sdeg fix=%s",
        sensors.node_lat,
        sensors.node_lon,
        sensors.node_altitude,
        sats,
        speed_txt,
        course_txt,
        gps_live_fix ? "live" : "cached");
      sendTrackerText(text);
      completeTrackingCycle();
      return;
    }

    const unsigned long timeout_millis = TRACKER_DEFAULT_GPS_TIMEOUT_SECS * 1000UL;
    if (the_mesh.millisHasNowPassed(_tracking_started_millis + timeout_millis)) {
      sendTrackerText("GPS timeout: no fix");
      completeTrackingCycle();
    }
#else
    sendTrackerText("GPS unavailable on this build");
    completeTrackingCycle();
#endif
  }

public:
  bool setGroupName(const char* name) {
    if (!name || !name[0]) {
      return false;
    }
    snprintf(_group_name, sizeof(_group_name), "%s", name);
    persistGroupName();
    return true;
  }

  bool setGroupPSK(const char* psk) {
    if (!psk || !psk[0]) {
      return false;
    }

    snprintf(_group_psk, sizeof(_group_psk), "%s", psk);
    persistGroupPSK();
    return true;
  }

  const char* getGroupName() const {
    return _group_name;
  }

  const char* getGroupPSK() const {
    return _group_psk;
  }

  void begin() {
    loadPersistedGroupPSK();
    loadPersistedGroupName();
    _next_measure_millis = millis() + TRACKER_BOOT_GRACE_SECS * 1000UL;
  }

  void loop() {
    if (_tracking_in_progress) {
      pollTrackingCycle();
      return;
    }

    if (_next_measure_millis && the_mesh.millisHasNowPassed(_next_measure_millis)) {
      _next_measure_millis = 0;
      startTrackingCycle();
    }
  }
};

static TrackerHybridTask tracker_hybrid_task;

bool trackerHybridSetGroupName(const char* name) {
  return tracker_hybrid_task.setGroupName(name);
}

const char* trackerHybridGetGroupName() {
  return tracker_hybrid_task.getGroupName();
}

bool trackerHybridSetGroupPSK(const char* psk) {
  return tracker_hybrid_task.setGroupPSK(psk);
}

const char* trackerHybridGetGroupPSK() {
  return tracker_hybrid_task.getGroupPSK();
}
#endif

/* END GLOBAL OBJECTS */

void halt() {
  while (1) ;
}

/* WIFI RECONNECT TRACKERS */
#ifdef ENABLE_WIFI_INTERFACE
  bool wifi_needs_reconnect = false;
  unsigned long last_wifi_reconnect_attempt = 0;
  char wifi_ssid[33] = WIFI_SSID;   // replaced by stored prefs at boot, if set
  char wifi_pwd[64] = WIFI_PWD;
  bool wifi_was_connected = false;
  bool wifi_enabled = false;   // set at boot from prefs; false also when the effective SSID is blank
#endif

void setup() {
  Serial.begin(115200);
  board.begin();

#ifdef HAS_EXTERNAL_WATCHDOG
  external_watchdog.begin();
#endif

#ifdef DISPLAY_CLASS
  DisplayDriver* disp = NULL;
  if (display.begin()) {
    disp = &display;
    disp->startFrame();
  #ifdef ST7789
    disp->setTextSize(2);
  #endif
    disp->drawTextCentered(disp->width() / 2, 28, "Loading...");
    disp->endFrame();
  }
#endif

  if (!radio_init()) { halt(); }

  fast_rng.begin(radio_driver.getRngSeed());

#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
  InternalFS.begin();
  #if defined(QSPIFLASH)
    if (!QSPIFlash.begin()) {
      // debug output might not be available at this point, might be too early. maybe should fall back to InternalFS here?
      MESH_DEBUG_PRINTLN("CustomLFS_QSPIFlash: failed to initialize");
    } else {
      MESH_DEBUG_PRINTLN("CustomLFS_QSPIFlash: initialized successfully");
    }
  #else
  #if defined(EXTRAFS)
      ExtraFS.begin();
  #endif
  #endif
  store.begin();
  the_mesh.begin();
#elif defined(RP2040_PLATFORM)
  LittleFS.begin();
  store.begin();
  the_mesh.begin();
#elif defined(ESP32)
  SPIFFS.begin(true);
  store.begin();
  the_mesh.begin();
#else
  #error "need to define filesystem"
#endif

#ifdef BLE_PIN_CODE // 123456 by default
  if (the_mesh.getNodePrefs()->ble_pin == 0) {
#ifdef DISPLAY_CLASS
    if (disp != NULL && BLE_PIN_CODE == 123456) {
      StdRNG rng;
      the_mesh.setBLEPin(rng.nextInt(100000, 999999)); // random pin each session
    } else {
      the_mesh.setBLEPin(BLE_PIN_CODE); // otherwise static pin
    }
#else
    the_mesh.setBLEPin(BLE_PIN_CODE); // otherwise static pin
#endif
  } else {
    the_mesh.setBLEPin(the_mesh.getNodePrefs()->ble_pin);
  }
#else
  the_mesh.setBLEPin(0);
#endif

// add bluetooth interface
#if defined(BLE_PIN_CODE)
  bluetooth_interface.begin(BLE_NAME_PREFIX, the_mesh.getNodePrefs()->node_name, the_mesh.getBLEPin());
  interface_manager.addInterface(InterfaceType::Bluetooth, &bluetooth_interface);
#endif

// add wifi interface
#ifdef ENABLE_WIFI_INTERFACE
  // use wifi ssid and password from prefs if ssid is not empty, otherwise use the build flag defaults
  if (the_mesh.getNodePrefs()->wifi_ssid[0]) {
    strcpy(wifi_ssid, the_mesh.getNodePrefs()->wifi_ssid);
    strcpy(wifi_pwd, the_mesh.getNodePrefs()->wifi_pwd);
  }
  // only start wifi if enabled and ssid is not empty
  wifi_enabled = the_mesh.getNodePrefs()->wifi_enabled;
  if (wifi_enabled && wifi_ssid[0]) {
#if defined(ESP32)
    board.setInhibitSleep(true);   // prevent sleep when WiFi is active
    WiFi.setAutoReconnect(true);

    WiFi.onEvent([](WiFiEvent_t event, WiFiEventInfo_t info){
        if (event == ARDUINO_EVENT_WIFI_STA_DISCONNECTED) {
            WIFI_DEBUG_PRINTLN("WiFi disconnected (reason=%s). Flagging for reconnect...",
              WiFi.disconnectReasonName((wifi_err_reason_t)info.wifi_sta_disconnected.reason));
            wifi_needs_reconnect = true;
        } else if (event == ARDUINO_EVENT_WIFI_STA_GOT_IP) {
            WIFI_DEBUG_PRINTLN("WiFi connected successfully!");
            wifi_needs_reconnect = false;
        }
    });
#endif

    WIFI_DEBUG_PRINTLN("connecting to %s", wifi_ssid);

#if defined(RP2040_PLATFORM)
    // the join itself blocks inside the core (CYW43::begin busy-waits for the
    // association), so every attempt stalls the mesh loop. beginNoBlock() only skips the
    // extra DHCP wait. Give the first connect a full window, then bound the retries below.
    WiFi.beginNoBlock(wifi_ssid, wifi_pwd);
    last_wifi_reconnect_attempt = millis();   // let DHCP finish before the poll can retry
#else
    WiFi.begin(wifi_ssid, wifi_pwd);
#endif
    wifi_interface.begin(TCP_PORT);
    interface_manager.addInterface(InterfaceType::WiFi, &wifi_interface);
  } else {
    WIFI_DEBUG_PRINTLN("wifi disabled");
  }
#endif

// add usb interface
#if defined(ENABLE_USB_INTERFACE)
  usb_serial_interface.begin(Serial);
  interface_manager.addInterface(InterfaceType::USB, &usb_serial_interface);
#endif

// add ethernet interface
#if defined(ETHERNET_ENABLED)
  ethernet_interface.begin();
  interface_manager.addInterface(InterfaceType::Ethernet, &ethernet_interface);
#endif

// add hardware serial interface
#if defined(SERIAL_RX)
  companion_serial.setPins(SERIAL_RX, SERIAL_TX);
  companion_serial.begin(115200);
  hardware_serial_interface.begin(companion_serial);
  interface_manager.addInterface(InterfaceType::HardwareSerial, &hardware_serial_interface);
#endif

  the_mesh.startInterface(interface_manager);
  sensors.begin();

#if ENV_INCLUDE_GPS == 1
  the_mesh.applyGpsPrefs();
#endif

#ifdef DISPLAY_CLASS
  ui_task.begin(disp, &sensors, the_mesh.getNodePrefs());  // still want to pass this in as dependency, as prefs might be moved
  the_mesh.setListener(&ui_task);
#endif

  board.onBootComplete();
}

void loop() {
  the_mesh.loop();
  interface_manager.loop();
  sensors.loop();
#if TRACKER_HYBRID == 1
  tracker_hybrid_task.loop();
#endif
#ifdef DISPLAY_CLASS
  ui_task.loop();
#endif
  rtc_clock.tick();
  board.loop();
#ifdef HAS_EXTERNAL_WATCHDOG
  external_watchdog.loop();
#endif

  if (!the_mesh.hasPendingWork()) {
#if defined(NRF52_PLATFORM)
    board.sleep(0); // nrf ignores seconds param, sleeps whenever possible
#endif
  }

#ifdef ENABLE_WIFI_INTERFACE
  if (wifi_enabled) {
    // RP2040 has no WiFi event callbacks, so poll the link state instead
  #if defined(RP2040_PLATFORM)
      wifi_needs_reconnect = (WiFi.status() != WL_CONNECTED);
      if (wifi_was_connected == wifi_needs_reconnect) {   // link state changed
        wifi_was_connected = !wifi_needs_reconnect;
        if (wifi_was_connected) {
          WIFI_DEBUG_PRINTLN("connected, listening on %s:%d", WiFi.localIP().toString().c_str(), TCP_PORT);
        } else {
          WIFI_DEBUG_PRINTLN("link lost");
        }
      }
  #endif

    // Safely attempt to reconnect if flagged. On RP2040 each attempt blocks the mesh loop
    // for up to WIFI_RETRY_TIMEOUT, so retry less often and cap how long a join may stall.
    if (wifi_needs_reconnect && (millis() - last_wifi_reconnect_attempt > WIFI_RETRY_INTERVAL)) {
      WIFI_DEBUG_PRINTLN("Attempting manual WiFi reconnect to %s (status %d)...", wifi_ssid, WiFi.status());
    #if defined(RP2040_PLATFORM)
        WiFi.setTimeout(WIFI_RETRY_TIMEOUT);
        WiFi.beginNoBlock(wifi_ssid, wifi_pwd);   // no reconnect() on this platform
    #else
        WiFi.disconnect();
        WiFi.reconnect();
    #endif
      last_wifi_reconnect_attempt = millis();
    }
  }
#endif
}
