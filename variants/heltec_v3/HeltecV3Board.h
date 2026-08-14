#pragma once

#include <Arduino.h>
#include <helpers/RefCountedDigitalPin.h>
#include <helpers/ESP32Board.h>
#if defined(HELTEC_V3_LOW_POWER_PROFILE) && defined(CONFIG_PM_SLP_DISABLE_GPIO) && CONFIG_PM_SLP_DISABLE_GPIO
  #include <driver/gpio.h>
#endif

// built-ins
#ifndef PIN_VBAT_READ              // set in platformio.ini for boards like Heltec Wireless Paper (20)
  #define  PIN_VBAT_READ    1
#endif
#ifndef PIN_ADC_CTRL              // set in platformio.ini for Heltec Wireless Tracker (2)
  #define  PIN_ADC_CTRL    37
#endif
#ifndef ADC_MULTIPLIER            //default ADC multiplier
  #define ADC_MULTIPLIER 5.42
#endif
#define  PIN_ADC_CTRL_ACTIVE    LOW
#define  PIN_ADC_CTRL_INACTIVE  HIGH

class HeltecV3Board : public ESP32Board {
private:
  bool adc_active_state;

#if defined(HELTEC_V3_LOW_POWER_PROFILE) && defined(CONFIG_PM_SLP_DISABLE_GPIO) && CONFIG_PM_SLP_DISABLE_GPIO
  static inline void configureSleepOutputPin(int pin) {
    if (pin < 0) return;
    const gpio_num_t gpio = (gpio_num_t)pin;
    // Keep the current output level while the IDF isolates ordinary GPIOs in
    // automatic light sleep.
    (void)gpio_sleep_set_direction(gpio, GPIO_MODE_OUTPUT);
    (void)gpio_sleep_set_pull_mode(gpio, GPIO_FLOATING);
    (void)gpio_sleep_sel_en(gpio);
  }
#endif

  static inline void biasInputPin(int pin, uint8_t mode) {
    if (pin < 0) return;
    pinMode((uint8_t)pin, mode);
  }

  void configureLowPowerPins() {
#if defined(PIN_USER_BTN)
    // Heltec V3 user button is active-low on GPIO0.
    biasInputPin(PIN_USER_BTN, INPUT_PULLUP);
#endif

#if defined(HELTEC_V3_LOW_POWER_PROFILE) && defined(CONFIG_PM_SLP_DISABLE_GPIO) && CONFIG_PM_SLP_DISABLE_GPIO
    // Preserve the radio deselect, battery-divider and disabled TX LED states
    // while the ESP32-S3 enters automatic light sleep.
    pinMode(P_LORA_NSS, OUTPUT);
    digitalWrite(P_LORA_NSS, HIGH);
    configureSleepOutputPin(P_LORA_NSS);
    configureSleepOutputPin(PIN_ADC_CTRL);
#ifdef P_LORA_TX_LED
    configureSleepOutputPin(P_LORA_TX_LED);
#endif
#endif

    // Optional user-defined unused GPIO list (bias to GND).
#ifdef LOW_POWER_UNUSED_GPIO_1
    biasInputPin(LOW_POWER_UNUSED_GPIO_1, INPUT_PULLDOWN);
#endif
#ifdef LOW_POWER_UNUSED_GPIO_2
    biasInputPin(LOW_POWER_UNUSED_GPIO_2, INPUT_PULLDOWN);
#endif
#ifdef LOW_POWER_UNUSED_GPIO_3
    biasInputPin(LOW_POWER_UNUSED_GPIO_3, INPUT_PULLDOWN);
#endif
#ifdef LOW_POWER_UNUSED_GPIO_4
    biasInputPin(LOW_POWER_UNUSED_GPIO_4, INPUT_PULLDOWN);
#endif
#ifdef LOW_POWER_UNUSED_GPIO_5
    biasInputPin(LOW_POWER_UNUSED_GPIO_5, INPUT_PULLDOWN);
#endif
#ifdef LOW_POWER_UNUSED_GPIO_6
    biasInputPin(LOW_POWER_UNUSED_GPIO_6, INPUT_PULLDOWN);
#endif
#ifdef LOW_POWER_UNUSED_GPIO_7
    biasInputPin(LOW_POWER_UNUSED_GPIO_7, INPUT_PULLDOWN);
#endif
#ifdef LOW_POWER_UNUSED_GPIO_8
    biasInputPin(LOW_POWER_UNUSED_GPIO_8, INPUT_PULLDOWN);
#endif

    // Optional user-defined GPIOs that should be biased to VCC.
#ifdef LOW_POWER_UNUSED_PULLUP_GPIO_1
    biasInputPin(LOW_POWER_UNUSED_PULLUP_GPIO_1, INPUT_PULLUP);
#endif
#ifdef LOW_POWER_UNUSED_PULLUP_GPIO_2
    biasInputPin(LOW_POWER_UNUSED_PULLUP_GPIO_2, INPUT_PULLUP);
#endif
#ifdef LOW_POWER_UNUSED_PULLUP_GPIO_3
    biasInputPin(LOW_POWER_UNUSED_PULLUP_GPIO_3, INPUT_PULLUP);
#endif
#ifdef LOW_POWER_UNUSED_PULLUP_GPIO_4
    biasInputPin(LOW_POWER_UNUSED_PULLUP_GPIO_4, INPUT_PULLUP);
#endif
  }

public:
  RefCountedDigitalPin periph_power;

  HeltecV3Board() : periph_power(PIN_VEXT_EN
#ifdef PIN_VEXT_EN_ACTIVE
      , PIN_VEXT_EN_ACTIVE
#endif
  ) { }

  void begin() {
    ESP32Board::begin();

    // Auto-detect correct ADC_CTRL pin polarity (different for boards >3.2)
    pinMode(PIN_ADC_CTRL, INPUT);
    adc_active_state = !digitalRead(PIN_ADC_CTRL);

    pinMode(PIN_ADC_CTRL, OUTPUT);
    digitalWrite(PIN_ADC_CTRL, !adc_active_state); // Initially inactive

    periph_power.begin();
    configureLowPowerPins();

    esp_reset_reason_t reason = esp_reset_reason();
    if (reason == ESP_RST_DEEPSLEEP) {
      long wakeup_source = esp_sleep_get_ext1_wakeup_status();
      if (wakeup_source & (1 << P_LORA_DIO_1)) {  // received a LoRa packet (while in deep sleep)
        startup_reason = BD_STARTUP_RX_PACKET;
      }

      rtc_gpio_hold_dis((gpio_num_t)P_LORA_NSS);
      rtc_gpio_deinit((gpio_num_t)P_LORA_DIO_1);
    }
  }

  uint16_t getBattMilliVolts() override {
    analogReadResolution(10);
    digitalWrite(PIN_ADC_CTRL, adc_active_state);

    uint32_t raw = 0;
    for (int i = 0; i < 8; i++) {
      raw += analogRead(PIN_VBAT_READ);
    }
    raw = raw / 8;

    digitalWrite(PIN_ADC_CTRL, !adc_active_state);

    return (ADC_MULTIPLIER * (3.3 / 1024.0) * raw) * 1000;
  }

#if defined(P_LORA_TX_LED)
  void onBeforeTransmit() override {
#if !defined(DISABLE_TX_LED) || (DISABLE_TX_LED == 0)
    ESP32Board::onBeforeTransmit();
#endif
  }

  void onAfterTransmit() override {
#if !defined(DISABLE_TX_LED) || (DISABLE_TX_LED == 0)
    ESP32Board::onAfterTransmit();
#endif
  }
#endif

  const char* getManufacturerName() const override {
    return "Heltec V3";
  }
};
