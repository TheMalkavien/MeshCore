#pragma once

#include <MeshCore.h>
#include <Arduino.h>

#if defined(ESP_PLATFORM)

#include <rom/rtc.h>
#include <sys/time.h>
#include <Wire.h>
#include <esp_sleep.h>
#include "driver/rtc_io.h"
#if defined(ESP32_ENABLE_DYNAMIC_PM) && (ESP32_ENABLE_DYNAMIC_PM == 1)
  #include <esp_pm.h>
#endif

class ESP32Board : public mesh::MainBoard {
protected:
  uint8_t startup_reason;
  bool inhibit_sleep = false;

private:
  uint64_t getLightSleepWakeupMicros(uint32_t secs) const {
    if (secs > 0) {
      return ((uint64_t)secs) * 1000000ULL;
    }
#if defined(ESP32_IDLE_LIGHT_SLEEP_MILLIS) && (ESP32_IDLE_LIGHT_SLEEP_MILLIS > 0)
    return ((uint64_t)ESP32_IDLE_LIGHT_SLEEP_MILLIS) * 1000ULL;
#else
    return 0;
#endif
  }

  void configureDynamicPowerManagement() {
#if defined(ESP32_ENABLE_DYNAMIC_PM) && (ESP32_ENABLE_DYNAMIC_PM == 1)
    int max_freq_mhz = getCpuFrequencyMhz();
#ifdef ESP32_PM_MAX_FREQ
    max_freq_mhz = ESP32_PM_MAX_FREQ;
#endif
    if (max_freq_mhz < 10) {
      max_freq_mhz = 10;
    }

    int min_freq_mhz = 10;
#ifdef ESP32_PM_MIN_FREQ
    min_freq_mhz = ESP32_PM_MIN_FREQ;
#endif
    if (min_freq_mhz < 10) {
      min_freq_mhz = 10;
    }
    if (min_freq_mhz > max_freq_mhz) {
      min_freq_mhz = max_freq_mhz;
    }

    bool light_sleep_enable = true;
#ifdef ESP32_PM_LIGHT_SLEEP
    light_sleep_enable = (ESP32_PM_LIGHT_SLEEP != 0);
#endif

#if defined(CONFIG_IDF_TARGET_ESP32S3)
    esp_pm_config_esp32s3_t pm_config = {
      .max_freq_mhz = max_freq_mhz,
      .min_freq_mhz = min_freq_mhz,
      .light_sleep_enable = light_sleep_enable
    };
    (void) esp_pm_configure(&pm_config);
#elif defined(CONFIG_IDF_TARGET_ESP32S2)
    esp_pm_config_esp32s2_t pm_config = {
      .max_freq_mhz = max_freq_mhz,
      .min_freq_mhz = min_freq_mhz,
      .light_sleep_enable = light_sleep_enable
    };
    (void) esp_pm_configure(&pm_config);
#elif defined(CONFIG_IDF_TARGET_ESP32C3)
    esp_pm_config_esp32c3_t pm_config = {
      .max_freq_mhz = max_freq_mhz,
      .min_freq_mhz = min_freq_mhz,
      .light_sleep_enable = light_sleep_enable
    };
    (void) esp_pm_configure(&pm_config);
#elif defined(CONFIG_IDF_TARGET_ESP32H2)
    esp_pm_config_esp32h2_t pm_config = {
      .max_freq_mhz = max_freq_mhz,
      .min_freq_mhz = min_freq_mhz,
      .light_sleep_enable = light_sleep_enable
    };
    (void) esp_pm_configure(&pm_config);
#elif defined(CONFIG_IDF_TARGET_ESP32)
    esp_pm_config_esp32_t pm_config = {
      .max_freq_mhz = max_freq_mhz,
      .min_freq_mhz = min_freq_mhz,
      .light_sleep_enable = light_sleep_enable
    };
    (void) esp_pm_configure(&pm_config);
#endif
#endif
  }

public:
  void begin() {
    // for future use, sub-classes SHOULD call this from their begin()
    startup_reason = BD_STARTUP_NORMAL;

  #ifdef ESP32_CPU_FREQ
    setCpuFrequencyMhz(ESP32_CPU_FREQ);
  #endif
    configureDynamicPowerManagement();

  #ifdef PIN_VBAT_READ
    // battery read support
    pinMode(PIN_VBAT_READ, INPUT);
    adcAttachPin(PIN_VBAT_READ);
  #endif

  #ifdef P_LORA_TX_LED
    pinMode(P_LORA_TX_LED, OUTPUT);
    digitalWrite(P_LORA_TX_LED, LOW);
  #endif

  #if defined(PIN_BOARD_SDA) && defined(PIN_BOARD_SCL)
   #if PIN_BOARD_SDA >= 0 && PIN_BOARD_SCL >= 0
    Wire.begin(PIN_BOARD_SDA, PIN_BOARD_SCL);
   #endif
  #else
    Wire.begin();
  #endif
  }

  // Temperature from ESP32 MCU
  float getMCUTemperature() override {
    uint32_t raw = 0;

    // To get and average the temperature so it is more accurate, especially in low temperature
    for (int i = 0; i < 4; i++) {
      raw += temperatureRead();
    }

    return raw / 4;
  }

  void enterLightSleep(uint32_t secs) {
#if defined(ESP_PLATFORM)
    bool has_wakeup = false;
    bool allow_timer_only = false;
#if defined(ESP32_ENABLE_TIMER_ONLY_LIGHT_SLEEP) && (ESP32_ENABLE_TIMER_ONLY_LIGHT_SLEEP == 1)
    allow_timer_only = true;
#endif
    esp_sleep_disable_wakeup_source(ESP_SLEEP_WAKEUP_ALL);

#if defined(CONFIG_IDF_TARGET_ESP32S3) && defined(P_LORA_DIO_1)
    if (rtc_gpio_is_valid_gpio((gpio_num_t)P_LORA_DIO_1)) {
      esp_sleep_pd_config(ESP_PD_DOMAIN_RTC_PERIPH, ESP_PD_OPTION_ON);
      esp_sleep_enable_ext1_wakeup((1ULL << P_LORA_DIO_1), ESP_EXT1_WAKEUP_ANY_HIGH);
      has_wakeup = true;
    }
#endif

    uint64_t wakeup_micros = getLightSleepWakeupMicros(secs);
    if (wakeup_micros > 0 && (has_wakeup || allow_timer_only)) {
      esp_sleep_enable_timer_wakeup(wakeup_micros);
      has_wakeup = true;
    }

    if (has_wakeup) {
      esp_light_sleep_start();
    }
#endif
  }

  void sleep(uint32_t secs) override {
    if (!inhibit_sleep) {
      enterLightSleep(secs);
    }
  }

  uint8_t getStartupReason() const override { return startup_reason; }

#if defined(P_LORA_TX_LED)
  void onBeforeTransmit() override {
    digitalWrite(P_LORA_TX_LED, HIGH);   // turn TX LED on
  }
  void onAfterTransmit() override {
    digitalWrite(P_LORA_TX_LED, LOW);   // turn TX LED off
  }
#elif defined(P_LORA_TX_NEOPIXEL_LED)
  #define NEOPIXEL_BRIGHTNESS    64  // white brightness (max 255)

  void onBeforeTransmit() override {
    neopixelWrite(P_LORA_TX_NEOPIXEL_LED, NEOPIXEL_BRIGHTNESS, NEOPIXEL_BRIGHTNESS, NEOPIXEL_BRIGHTNESS);   // turn TX neopixel on (White)
  }
  void onAfterTransmit() override {
    neopixelWrite(P_LORA_TX_NEOPIXEL_LED, 0, 0, 0);   // turn TX neopixel off
  }
#endif

  uint16_t getBattMilliVolts() override {
  #ifdef PIN_VBAT_READ
    analogReadResolution(12);

    uint32_t raw = 0;
    for (int i = 0; i < 4; i++) {
      raw += analogReadMilliVolts(PIN_VBAT_READ);
    }
    raw = raw / 4;

    return (2 * raw);
  #else
    return 0;  // not supported
  #endif
  }

  const char* getManufacturerName() const override {
    return "Generic ESP32";
  }

  void reboot() override {
    esp_restart();
  }

  bool startOTAUpdate(const char* id, char reply[]) override;

  void setInhibitSleep(bool inhibit) {
    inhibit_sleep = inhibit;
  }
};

class ESP32RTCClock : public mesh::RTCClock {
public:
  ESP32RTCClock() { }
  void begin() {
    esp_reset_reason_t reason = esp_reset_reason();
    if (reason == ESP_RST_POWERON) {
      // start with some date/time in the recent past
      struct timeval tv;
      tv.tv_sec = 1715770351;  // 15 May 2024, 8:50pm
      tv.tv_usec = 0;
      settimeofday(&tv, NULL);
    }
  }
  uint32_t getCurrentTime() override {
    time_t _now;
    time(&_now);
    return _now;
  }
  void setCurrentTime(uint32_t time) override { 
    struct timeval tv;
    tv.tv_sec = time;
    tv.tv_usec = 0;
    settimeofday(&tv, NULL);
  }
};

#endif
