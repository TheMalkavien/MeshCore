#include <Arduino.h>   // needed for PlatformIO
#include <Mesh.h>
#include "MyMesh.h"

#if defined(ESP32)
  #include <freertos/FreeRTOS.h>
  #include <freertos/task.h>
  #if defined(ESP_PLATFORM)
    #include <esp_sleep.h>
    #include <driver/gpio.h>
    #include <driver/rtc_io.h>
    #if defined(CONFIG_IDF_TARGET_ESP32S3)
      #include <soc/soc.h>
      #include <soc/gpio_struct.h>
      #include <soc/usb_serial_jtag_reg.h>
      #include <soc/rtc_cntl_reg.h>
    #endif
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
  #else
    #error "SerialBLEInterface is not defined for this platform"
  #endif
#endif

// include wifi interface
#ifdef WIFI_SSID
  #ifndef TCP_PORT
    #define TCP_PORT 5000
  #endif
  #ifdef ESP32
    // include esp32 wifi interface
    #include <helpers/esp32/SerialWifiInterface.h>
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
MyMesh the_mesh(radio_driver, fast_rng, rtc_clock, tables, store
   #ifdef DISPLAY_CLASS
      , &ui_task
   #endif
);

/* END GLOBAL OBJECTS */

void halt() {
  while (1) ;
}

/* WIFI RECONNECT TRACKERS */
#if defined(ESP32) && defined(WIFI_SSID)
  bool wifi_needs_reconnect = false;
  unsigned long last_wifi_reconnect_attempt = 0;
#endif

#if defined(ESP32) && defined(ESP_PLATFORM) && defined(CONFIG_PM_ENABLE)
#ifndef PM_MAX_FREQ_MHZ
  #define PM_MAX_FREQ_MHZ 80
#endif

#ifndef PM_MIN_FREQ_MHZ
  #define PM_MIN_FREQ_MHZ 40
#endif

static void configureESP32PowerManagement() {
  // Keep CPU scaling dynamic but allow env-specific caps for low-power tuning.
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
  pm_config.max_freq_mhz = PM_MAX_FREQ_MHZ;
  pm_config.min_freq_mhz = PM_MIN_FREQ_MHZ;
  pm_config.light_sleep_enable = true;
  (void)esp_pm_configure(&pm_config);
}
#endif

#if defined(ESP32) && defined(ESP_PLATFORM) && defined(CONFIG_IDF_TARGET_ESP32S3)
static void disableESP32S3USBSerialJTAG() {
#if defined(DISABLE_SERIAL_CONSOLE)
  // Hard-disconnect USB Serial/JTAG from D+/D- so host COM port disappears at runtime.
  CLEAR_PERI_REG_MASK(USB_SERIAL_JTAG_CONF0_REG, USB_SERIAL_JTAG_DP_PULLUP);
  CLEAR_PERI_REG_MASK(USB_SERIAL_JTAG_CONF0_REG, USB_SERIAL_JTAG_USB_PAD_ENABLE);
  SET_PERI_REG_MASK(RTC_CNTL_USB_CONF_REG, RTC_CNTL_USB_PAD_ENABLE_OVERRIDE);
  CLEAR_PERI_REG_MASK(RTC_CNTL_USB_CONF_REG, RTC_CNTL_USB_PAD_ENABLE);
#endif
}
#endif

#ifndef LOOP_BUSY_DELAY_MS
  #define LOOP_BUSY_DELAY_MS 6
#endif

#ifndef LOOP_IDLE_DELAY_MS
  #define LOOP_IDLE_DELAY_MS 30
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

#if defined(EVENT_DRIVEN_LOOP)
  // Idle caps: the loop blocks up to this long, but a real event (LoRa DIO1 IRQ,
  // BLE RX) resumes it immediately. With CONFIG_PM + tickless idle the SoC auto
  // light-sleeps during the wait, so longer caps = fewer wakeups = less power.
  #ifndef EVENT_LOOP_CONNECTED_MAX_MS
    #define EVENT_LOOP_CONNECTED_MAX_MS 30   // phone connected: stay snappy
  #endif
  #ifndef EVENT_LOOP_IDLE_MAX_MS
    #define EVENT_LOOP_IDLE_MAX_MS 500       // disconnected: LoRa IRQ still wakes us
  #endif

// Handle to the Arduino loop task, so ISRs / BLE callbacks can resume it.
static TaskHandle_t g_main_loop_task = NULL;

extern "C" bool meshcore_radio_irq_pending(void);

// Button edges are also events. Keep a short polling grace period after each
// edge so MomentaryButton can finish its multi-click window without falling
// back to the long disconnected idle cap.
#if defined(PIN_USER_BTN)
static volatile uint32_t g_button_irq_count = 0;
#endif

#if defined(ESP_PLATFORM) && defined(CONFIG_IDF_TARGET_ESP32S3)
// gpio_wakeup_enable() deliberately changes the regular GPIO interrupt to a
// level interrupt.  A level interrupt is required to wake from light sleep,
// but SX1262 DIO1 remains high until the IRQ is serviced, so leaving it armed
// would continuously re-enter the ISR.  The ISR disables the GPIO interrupt
// with a single IRAM-safe register write.  The loop normally re-arms it after
// the line is inactive, while an unlatched active level is delivered once.
#if defined(P_LORA_DIO_1)
static volatile bool g_lora_gpio_wake_armed = false;
static volatile bool g_lora_active_level_latched = false;
#endif
#if defined(PIN_USER_BTN)
static volatile bool g_button_gpio_wake_armed = false;
static volatile bool g_button_active_level_latched = false;
#endif

static inline void IRAM_ATTR disableGPIOInterruptFromISR(gpio_num_t pin) {
  // gpio_intr_disable() is not IRAM_ATTR in IDF 4.4.  This hook may run while
  // the flash cache is disabled, so use the equivalent S3 GPIO register write.
  GPIO.pin[(uint32_t)pin].int_ena = 0;
}

static bool armLevelWakeSafely(gpio_num_t pin, int active_level,
                               gpio_int_type_t wake_type,
                               gpio_int_type_t normal_type,
                               volatile bool* armed,
                               volatile bool* active_level_latched) {
  if (*armed) return true;

  // Switch atomically enough that an edge between the level check and enable
  // is still observed: once wake_type is installed, an already-active level
  // immediately raises the GPIO interrupt.
  (void)gpio_intr_disable(pin);
  (void)gpio_wakeup_disable(pin);
  (void)gpio_set_intr_type(pin, normal_type);

  const bool active = gpio_get_level(pin) == active_level;
  if (active && *active_level_latched) {
#if defined(P_LORA_DIO_1)
    if (pin != (gpio_num_t)P_LORA_DIO_1 || meshcore_radio_irq_pending()) {
      // The original event is still pending. Keep the level interrupt masked
      // until RadioLib consumes it, otherwise it would retrigger forever.
      return false;
    }
    // RadioLib consumed the original event, but DIO1 is high again: another
    // packet completed while the GPIO interrupt was masked. Re-arm the level
    // interrupt so that second event is delivered immediately.
    *active_level_latched = false;
#else
    return false;
#endif
  }
  if (!active) *active_level_latched = false;

  // Normally this arms an inactive line. If an unlatched level became active
  // inside the switch window, enabling the level interrupt below immediately
  // runs its ISR exactly once, so the edge cannot be lost.
  *armed = true;
  if (gpio_wakeup_enable(pin, wake_type) != ESP_OK) {
    *armed = false;
    (void)gpio_set_intr_type(pin, normal_type);
  }
  (void)gpio_intr_enable(pin);
  return *armed;
}

static void armEventDrivenWakeSources(void) {
#if defined(P_LORA_DIO_1)
  (void)armLevelWakeSafely((gpio_num_t)P_LORA_DIO_1, HIGH,
                           GPIO_INTR_HIGH_LEVEL, GPIO_INTR_POSEDGE,
                           &g_lora_gpio_wake_armed,
                           &g_lora_active_level_latched);
#endif
#if defined(PIN_USER_BTN)
  (void)armLevelWakeSafely((gpio_num_t)PIN_USER_BTN, LOW,
                           GPIO_INTR_LOW_LEVEL, GPIO_INTR_ANYEDGE,
                           &g_button_gpio_wake_armed,
                           &g_button_active_level_latched);
#endif
}
#endif

// Resume the loop from thread context (called by the BLE RX callback; overrides
// the weak no-op in SerialBLEInterface.cpp).
extern "C" void meshcore_wake_main_loop(void) {
  if (g_main_loop_task) xTaskNotifyGive(g_main_loop_task);
}

static void IRAM_ATTR notifyMainLoopFromISR(void) {
  if (g_main_loop_task) {
    BaseType_t hpw = pdFALSE;
    vTaskNotifyGiveFromISR(g_main_loop_task, &hpw);
    if (hpw) portYIELD_FROM_ISR(hpw);
  }
}

// Resume the loop from ISR context on a LoRa DIO1 IRQ (RX/TX complete). Overrides
// the weak hook in RadioLibWrappers.cpp so a packet wakes us with ~0 latency.
// IRAM_ATTR is mandatory: setFlag() runs from IRAM, and this may fire while the
// flash cache is disabled (e.g. during a contacts save).
extern "C" void IRAM_ATTR meshcore_on_lora_dio1_irq(void) {
#if defined(ESP_PLATFORM) && defined(CONFIG_IDF_TARGET_ESP32S3) && defined(P_LORA_DIO_1)
  g_lora_active_level_latched = true;
  if (g_lora_gpio_wake_armed) {
    g_lora_gpio_wake_armed = false;
    disableGPIOInterruptFromISR((gpio_num_t)P_LORA_DIO_1);
  }
#endif
  notifyMainLoopFromISR();
}

#if defined(PIN_USER_BTN)
static void IRAM_ATTR onUserButtonEdge(void) {
#if defined(ESP_PLATFORM) && defined(CONFIG_IDF_TARGET_ESP32S3)
  g_button_active_level_latched = true;
  if (g_button_gpio_wake_armed) {
    g_button_gpio_wake_armed = false;
    disableGPIOInterruptFromISR((gpio_num_t)PIN_USER_BTN);
  }
#endif
  ++g_button_irq_count;
  notifyMainLoopFromISR();
}
#endif

static void configureEventDrivenWakeSources(void) {
#if defined(ESP_PLATFORM)
  // CONFIG_PM_SLP_DISABLE_GPIO isolates GPIOs during automatic light sleep.
  // gpio_wakeup_enable() exempts event pins from that isolation and makes them
  // real automatic-light-sleep wake sources.  The ISR/re-arm handshake above
  // safely handles the level interrupt required by the ESP32-S3 wake hardware.
#if defined(P_LORA_DIO_1)
  gpio_sleep_sel_dis((gpio_num_t)P_LORA_DIO_1);
#endif
#if defined(PIN_USER_BTN)
  pinMode(PIN_USER_BTN, INPUT_PULLUP);
  gpio_sleep_sel_dis((gpio_num_t)PIN_USER_BTN);
  attachInterrupt(digitalPinToInterrupt(PIN_USER_BTN), onUserButtonEdge, CHANGE);
#endif
#if defined(CONFIG_IDF_TARGET_ESP32S3)
  if (esp_sleep_enable_gpio_wakeup() == ESP_OK) {
    armEventDrivenWakeSources();
  }
#endif
#endif
}
#endif

void setup() {
#if defined(ESP32) && defined(ESP_PLATFORM) && defined(CONFIG_IDF_TARGET_ESP32S3)
  disableESP32S3USBSerialJTAG();
#endif

#ifndef DISABLE_SERIAL_CONSOLE
  Serial.begin(115200);
#endif

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
  the_mesh.begin(
    #ifdef DISPLAY_CLASS
        disp != NULL
    #else
        false
    #endif
  );
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
#else
  #error "need to define filesystem"
#endif

// add bluetooth interface
#if defined(BLE_PIN_CODE)
  bluetooth_interface.begin(BLE_NAME_PREFIX, the_mesh.getNodePrefs()->node_name, the_mesh.getBLEPin());
  interface_manager.addInterface(InterfaceType::Bluetooth, &bluetooth_interface);
#endif

// add wifi interface
#ifdef WIFI_SSID
  board.setInhibitSleep(true);   // prevent sleep when WiFi is active
  WiFi.setAutoReconnect(true);

  WiFi.onEvent([](WiFiEvent_t event, WiFiEventInfo_t info){
      if (event == ARDUINO_EVENT_WIFI_STA_DISCONNECTED) {
          WIFI_DEBUG_PRINTLN("WiFi disconnected. Flagging for reconnect...");
          wifi_needs_reconnect = true;
      } else if (event == ARDUINO_EVENT_WIFI_STA_GOT_IP) {
          WIFI_DEBUG_PRINTLN("WiFi connected successfully!");
          wifi_needs_reconnect = false;
      }
  });

  WiFi.begin(WIFI_SSID, WIFI_PWD);
  wifi_interface.begin(TCP_PORT);
  interface_manager.addInterface(InterfaceType::WiFi, &wifi_interface);
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
#endif

#if defined(ESP32) && defined(ESP_PLATFORM) && defined(CONFIG_PM_ENABLE)
  configureESP32PowerManagement();
#endif

#if defined(EVENT_DRIVEN_LOOP)
  // setup() runs in the Arduino loop task, so this is the task loop() blocks on.
  g_main_loop_task = xTaskGetCurrentTaskHandle();
  configureEventDrivenWakeSources();
#endif

  board.onBootComplete();
}


void loop() {
  the_mesh.loop();
  interface_manager.loop();
  sensors.loop();
#ifdef DISPLAY_CLASS
  ui_task.loop();
#endif
  rtc_clock.tick();
#ifdef HAS_EXTERNAL_WATCHDOG
  external_watchdog.loop();
#endif

  if (!the_mesh.hasPendingWork()) {
#if defined(NRF52_PLATFORM)
    board.sleep(0); // nrf ignores seconds param, sleeps whenever possible
#endif
  }

#if defined(ESP32) && defined(WIFI_SSID)
  // Safely attempt to reconnect every 10 seconds if flagged
  if (wifi_needs_reconnect && (millis() - last_wifi_reconnect_attempt > 10000)) {
    WIFI_DEBUG_PRINTLN("Attempting manual WiFi reconnect...");
    WiFi.disconnect();
    WiFi.reconnect();
    last_wifi_reconnect_attempt = millis();
  }
#endif

#if defined(EVENT_DRIVEN_LOOP)
  // Event-driven idle: block until a real event (LoRa DIO1 IRQ, BLE RX) or a
  // capped deadline. With CONFIG_PM + tickless idle the SoC auto light-sleeps
  // during the wait; a genuine event resumes us immediately -> low power without
  // losing reactivity.
  const bool busy = the_mesh.hasPendingWork() || interface_manager.isWriteBusy() ||
                    radio_driver.isRxPowerSavingMaintenanceActive();
  bool gps_streaming = false;
  #if ENV_INCLUDE_GPS
    #if GPS_SLEEP_BETWEEN_UPDATES
      gps_streaming = sensors.isGPSFixWindowActive();  // NMEA only flows during a fix window
    #else
      gps_streaming = sensors.isGPSActive();           // NMEA flows whenever GPS is on
    #endif
  #endif
  uint32_t wait_ms;
  if (busy || gps_streaming) {
    wait_ms = LOOP_BUSY_DELAY_MS;                       // service pending work / drain GPS UART promptly
  } else if (interface_manager.isConnected()) {
    wait_ms = EVENT_LOOP_CONNECTED_MAX_MS;
  } else {
    wait_ms = EVENT_LOOP_IDLE_MAX_MS;
  }
#if defined(PIN_USER_BTN)
  static uint32_t seen_button_irq_count = 0;
  static uint32_t button_fast_poll_until = 0;
  const uint32_t button_irq_count = g_button_irq_count;
  if (button_irq_count != seen_button_irq_count) {
    seen_button_irq_count = button_irq_count;
    button_fast_poll_until = millis() + 350U;
  }
  const bool button_fast_poll =
      digitalRead(PIN_USER_BTN) == LOW ||
      (button_fast_poll_until != 0 &&
       (int32_t)(millis() - button_fast_poll_until) < 0);
  if (button_fast_poll && wait_ms > EVENT_LOOP_CONNECTED_MAX_MS) {
    wait_ms = EVENT_LOOP_CONNECTED_MAX_MS;
  }
#endif
#if defined(ESP_PLATFORM) && defined(CONFIG_IDF_TARGET_ESP32S3)
  // RadioLib has consumed a completed IRQ by now and the button may have been
  // released. Re-arm only inactive lines immediately before blocking so an
  // event during tickless automatic light sleep wakes this task via its ISR.
  armEventDrivenWakeSources();
#endif
  const TickType_t ticks = pdMS_TO_TICKS(wait_ms);
  ulTaskNotifyTake(pdTRUE, ticks ? ticks : 1);           // cleared/short-circuited by any wake notification
#elif defined(ESP32)
  // Preserve the legacy pacing for ESP32 companion profiles that do not opt in
  // to the event-driven low-power loop.
  const bool link_connected = interface_manager.isConnected();
  const bool busy = the_mesh.hasPendingWork() || interface_manager.isWriteBusy() ||
                    radio_driver.isRxPowerSavingMaintenanceActive();
#if defined(BLE_PIN_CODE)
  const bool allow_manual_sleep = false;
#else
  const bool allow_manual_sleep = true;
#endif
  if (busy) {
    const TickType_t busy_ticks = pdMS_TO_TICKS(LOOP_BUSY_DELAY_MS);
    vTaskDelay((busy_ticks > 0) ? busy_ticks : 1);
  } else {
#if defined(ESP_PLATFORM) && !defined(WIFI_SSID)
    const uint32_t sleep_ms = allow_manual_sleep
        ? (link_connected ? MANUAL_LIGHT_SLEEP_CONNECTED_MS : MANUAL_LIGHT_SLEEP_IDLE_MS)
        : 0;
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
