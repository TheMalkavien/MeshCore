#pragma once

#include <Arduino.h>
#if defined(ESP_PLATFORM) && defined(CONFIG_PM_SLP_DISABLE_GPIO)
  #include <driver/gpio.h>
#endif

class RefCountedDigitalPin {
  uint8_t _pin;
  int8_t _claims = 0;
  uint8_t _active = 0;
public:
  RefCountedDigitalPin(uint8_t pin,uint8_t active=HIGH): _pin(pin), _active(active) { }

  void begin() {
    pinMode(_pin, OUTPUT);
    digitalWrite(_pin, !_active);  // initial state
#if defined(ESP_PLATFORM) && defined(CONFIG_PM_SLP_DISABLE_GPIO)
    // Keep this GPIO under normal control in auto light sleep.
    // Required for power-gate pins (e.g. VEXT for OLED/GPS rails).
    gpio_sleep_sel_dis((gpio_num_t)_pin);
#endif
  }

  void claim() {
    _claims++;
    if (_claims > 0) {
      digitalWrite(_pin, _active);
    }
  }
  void release() {
    _claims--;
    if (_claims == 0) {
      digitalWrite(_pin, !_active);
    }
  }
};
