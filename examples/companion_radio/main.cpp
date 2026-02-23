#include <Arduino.h>   // needed for PlatformIO
#include <Mesh.h>
#include "MyMesh.h"

#if defined(ESP32)
  #include <freertos/FreeRTOS.h>
  #include <freertos/task.h>
  #if defined(ESP_PLATFORM)
    #include <esp_sleep.h>
    #include <driver/rtc_io.h>
    #if defined(CONFIG_PM_ENABLE)
      #include <esp_pm.h>
    #endif
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

#ifdef ESP32
  #ifdef WIFI_SSID
    #include <helpers/esp32/SerialWifiInterface.h>
    SerialWifiInterface serial_interface;
    #ifndef TCP_PORT
      #define TCP_PORT 5000
    #endif
  #elif defined(BLE_PIN_CODE)
    #include <helpers/esp32/SerialBLEInterface.h>
    SerialBLEInterface serial_interface;
  #elif defined(SERIAL_RX)
    #include <helpers/ArduinoSerialInterface.h>
    ArduinoSerialInterface serial_interface;
    HardwareSerial companion_serial(1);
  #else
    #include <helpers/ArduinoSerialInterface.h>
    ArduinoSerialInterface serial_interface;
  #endif
#elif defined(RP2040_PLATFORM)
  //#ifdef WIFI_SSID
  //  #include <helpers/rp2040/SerialWifiInterface.h>
  //  SerialWifiInterface serial_interface;
  //  #ifndef TCP_PORT
  //    #define TCP_PORT 5000
  //  #endif
  // #elif defined(BLE_PIN_CODE)
  //   #include <helpers/rp2040/SerialBLEInterface.h>
  //   SerialBLEInterface serial_interface;
  #if defined(SERIAL_RX)
    #include <helpers/ArduinoSerialInterface.h>
    ArduinoSerialInterface serial_interface;
    HardwareSerial companion_serial(1);
  #else
    #include <helpers/ArduinoSerialInterface.h>
    ArduinoSerialInterface serial_interface;
  #endif
#elif defined(NRF52_PLATFORM)
  #ifdef BLE_PIN_CODE
    #include <helpers/nrf52/SerialBLEInterface.h>
    SerialBLEInterface serial_interface;
  #else
    #include <helpers/ArduinoSerialInterface.h>
    ArduinoSerialInterface serial_interface;
  #endif
#elif defined(STM32_PLATFORM)
  #include <helpers/ArduinoSerialInterface.h>
  ArduinoSerialInterface serial_interface;
#else
  #error "need to define a serial interface"
#endif

/* GLOBAL OBJECTS */
#ifdef DISPLAY_CLASS
  #include "UITask.h"
  UITask ui_task(&board, &serial_interface);
#endif

StdRNG fast_rng;
SimpleMeshTables tables;
MyMesh the_mesh(radio_driver, fast_rng, rtc_clock, tables, store
   #ifdef DISPLAY_CLASS
      , &ui_task
   #endif
);

/* END GLOBAL OBJECTS */

void halt() {
  while (1) ;
}

#if defined(ESP32) && defined(ESP_PLATFORM) && defined(CONFIG_PM_ENABLE)
static void configureESP32PowerManagement() {
  // Keep CPU scaling dynamic but capped at 80 MHz for low-power operation.
#if defined(CONFIG_IDF_TARGET_ESP32S3)
  esp_pm_config_esp32s3_t pm_config = {};
#elif defined(CONFIG_IDF_TARGET_ESP32S2)
  esp_pm_config_esp32s2_t pm_config = {};
#elif defined(CONFIG_IDF_TARGET_ESP32C3)
  esp_pm_config_esp32c3_t pm_config = {};
#elif defined(CONFIG_IDF_TARGET_ESP32)
  esp_pm_config_esp32_t pm_config = {};
#else
  return;
#endif
  pm_config.max_freq_mhz = 80;
  pm_config.min_freq_mhz = 40;
  pm_config.light_sleep_enable = true;
  (void)esp_pm_configure(&pm_config);
}
#endif

#if defined(ESP32) && defined(ESP_PLATFORM)
static bool tryManualLightSleep(uint32_t sleep_ms) {
#if defined(P_LORA_DIO_1)
  if (sleep_ms == 0) return false;
  if (!rtc_gpio_is_valid_gpio((gpio_num_t)P_LORA_DIO_1)) return false;

  // Keep LoRa DIO wake capability while sleeping.
  (void)esp_sleep_pd_config(ESP_PD_DOMAIN_RTC_PERIPH, ESP_PD_OPTION_ON);
  (void)esp_sleep_enable_ext1_wakeup((1ULL << P_LORA_DIO_1), ESP_EXT1_WAKEUP_ANY_HIGH);
  (void)esp_sleep_enable_timer_wakeup((uint64_t)sleep_ms * 1000ULL);
  return esp_light_sleep_start() == ESP_OK;
#else
  (void)sleep_ms;
  return false;
#endif
}
#endif

#ifndef MANUAL_LIGHT_SLEEP_IDLE_MS
  #if defined(BLE_PIN_CODE)
    #define MANUAL_LIGHT_SLEEP_IDLE_MS 0
  #else
    #define MANUAL_LIGHT_SLEEP_IDLE_MS 5
  #endif
#endif

#ifndef MANUAL_LIGHT_SLEEP_CONNECTED_MS
  #define MANUAL_LIGHT_SLEEP_CONNECTED_MS 0
#endif

#ifndef LOOP_BUSY_DELAY_MS
  #define LOOP_BUSY_DELAY_MS 3
#endif

#ifndef LOOP_IDLE_DELAY_MS
  #define LOOP_IDLE_DELAY_MS 15
#endif

void setup() {
  Serial.begin(115200);

  board.begin();

#if defined(ESP32) && defined(ESP_PLATFORM) && defined(CONFIG_PM_ENABLE)
  configureESP32PowerManagement();
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

  fast_rng.begin(radio_get_rng_seed());

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
  the_mesh.begin(
    #ifdef DISPLAY_CLASS
        disp != NULL
    #else
        false
    #endif
  );

#ifdef BLE_PIN_CODE
  serial_interface.begin(BLE_NAME_PREFIX, the_mesh.getNodePrefs()->node_name, the_mesh.getBLEPin());
#else
  serial_interface.begin(Serial);
#endif
  the_mesh.startInterface(serial_interface);
#elif defined(RP2040_PLATFORM)
  LittleFS.begin();
  store.begin();
  the_mesh.begin(
    #ifdef DISPLAY_CLASS
        disp != NULL
    #else
        false
    #endif
  );

  //#ifdef WIFI_SSID
  //  WiFi.begin(WIFI_SSID, WIFI_PWD);
  //  serial_interface.begin(TCP_PORT);
  // #elif defined(BLE_PIN_CODE)
  //   char dev_name[32+16];
  //   sprintf(dev_name, "%s%s", BLE_NAME_PREFIX, the_mesh.getNodeName());
  //   serial_interface.begin(dev_name, the_mesh.getBLEPin());
  #if defined(SERIAL_RX)
    companion_serial.setPins(SERIAL_RX, SERIAL_TX);
    companion_serial.begin(115200);
    serial_interface.begin(companion_serial);
  #else
    serial_interface.begin(Serial);
  #endif
    the_mesh.startInterface(serial_interface);
#elif defined(ESP32)
  SPIFFS.begin(true);
  store.begin();
  the_mesh.begin(
    #ifdef DISPLAY_CLASS
        disp != NULL
    #else
        false
    #endif
  );

#ifdef WIFI_SSID
  board.setInhibitSleep(true);   // prevent sleep when WiFi is active
  WiFi.begin(WIFI_SSID, WIFI_PWD);
  serial_interface.begin(TCP_PORT);
#elif defined(BLE_PIN_CODE)
  serial_interface.begin(BLE_NAME_PREFIX, the_mesh.getNodePrefs()->node_name, the_mesh.getBLEPin());
#elif defined(SERIAL_RX)
  companion_serial.setPins(SERIAL_RX, SERIAL_TX);
  companion_serial.begin(115200);
  serial_interface.begin(companion_serial);
#else
  serial_interface.begin(Serial);
#endif
  the_mesh.startInterface(serial_interface);
#else
  #error "need to define filesystem"
#endif

  sensors.begin();

#ifdef DISPLAY_CLASS
  ui_task.begin(disp, &sensors, the_mesh.getNodePrefs());  // still want to pass this in as dependency, as prefs might be moved
#endif
}

void loop() {
  the_mesh.loop();
  sensors.loop();
#ifdef DISPLAY_CLASS
  ui_task.loop();
#endif
  rtc_clock.tick();

#if defined(ESP32)
  // Manual low-power policy: short light-sleep windows when idle.
  const bool link_connected = serial_interface.isConnected();
  const bool busy = the_mesh.hasPendingWork() || serial_interface.isWriteBusy();
  #if defined(BLE_PIN_CODE)
    const bool allow_manual_sleep = false; // BLE advertising/stack can become unstable with aggressive manual light sleep
  #else
    const bool allow_manual_sleep = true;
  #endif
  if (busy) {
    const TickType_t busy_ticks = pdMS_TO_TICKS(LOOP_BUSY_DELAY_MS);
    vTaskDelay((busy_ticks > 0) ? busy_ticks : 1);
  } else {
  #if defined(ESP_PLATFORM) && !defined(WIFI_SSID)
    const uint32_t sleep_ms = allow_manual_sleep ? (link_connected ? MANUAL_LIGHT_SLEEP_CONNECTED_MS : MANUAL_LIGHT_SLEEP_IDLE_MS) : 0;
    if (sleep_ms > 0 && !tryManualLightSleep(sleep_ms)) {
      const TickType_t idle_ticks = pdMS_TO_TICKS(LOOP_IDLE_DELAY_MS);
      vTaskDelay((idle_ticks > 0) ? idle_ticks : 1);
    } else if (sleep_ms == 0) {
      const TickType_t idle_ticks = pdMS_TO_TICKS(LOOP_IDLE_DELAY_MS);
      vTaskDelay((idle_ticks > 0) ? idle_ticks : 1);
    }
  #else
    const TickType_t idle_ticks = pdMS_TO_TICKS(LOOP_IDLE_DELAY_MS);
    vTaskDelay((idle_ticks > 0) ? idle_ticks : 1);
  #endif
  }
#endif
}
