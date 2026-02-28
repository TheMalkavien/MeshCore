#include "HeltecV4Board.h"

static inline void biasInputPin(int pin, uint8_t mode) {
  if (pin < 0) return;
  pinMode((uint8_t)pin, mode);
}

void HeltecV4Board::configureLowPowerPins() {
#if defined(PIN_USER_BTN)
  // Heltec V4 user button is active-low on GPIO0.
  biasInputPin(PIN_USER_BTN, INPUT_PULLUP);
#endif

  // Optional user-defined unused GPIO list (bias to GND).
  // Example build_flags:
  //   -D LOW_POWER_UNUSED_GPIO_1=5
  //   -D LOW_POWER_UNUSED_GPIO_2=6
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

void HeltecV4Board::begin() {
  ESP32Board::begin();

  pinMode(PIN_ADC_CTRL, OUTPUT);
  digitalWrite(PIN_ADC_CTRL, LOW); // Initially inactive

  // Set up digital GPIO registers before releasing RTC hold. The hold latches
  // the pad state including function select, so register writes accumulate
  // without affecting the pad. On hold release, all changes apply atomically.
  pinMode(P_LORA_PA_POWER, OUTPUT);
  digitalWrite(P_LORA_PA_POWER, HIGH);
  rtc_gpio_hold_dis((gpio_num_t)P_LORA_PA_POWER);

  pinMode(P_LORA_PA_EN, OUTPUT);
  digitalWrite(P_LORA_PA_EN, HIGH);
  rtc_gpio_hold_dis((gpio_num_t)P_LORA_PA_EN);

  pinMode(P_LORA_PA_TX_EN, OUTPUT);
  digitalWrite(P_LORA_PA_TX_EN, LOW);

  esp_reset_reason_t reason = esp_reset_reason();
  if (reason != ESP_RST_DEEPSLEEP) {
    delay(1);  // GC1109 startup time after cold power-on
  }

  periph_power.begin();
  configureLowPowerPins();

  if (reason == ESP_RST_DEEPSLEEP) {
    long wakeup_source = esp_sleep_get_ext1_wakeup_status();
    if (wakeup_source & (1 << P_LORA_DIO_1)) {  // received a LoRa packet while sleeping
      startup_reason = BD_STARTUP_RX_PACKET;
    }

    rtc_gpio_hold_dis((gpio_num_t)P_LORA_NSS);
    rtc_gpio_deinit((gpio_num_t)P_LORA_DIO_1);
  }
}

void HeltecV4Board::onBeforeTransmit(void) {
  digitalWrite(P_LORA_TX_LED, HIGH);   // turn TX LED on
  digitalWrite(P_LORA_PA_TX_EN, HIGH);
}

void HeltecV4Board::onAfterTransmit(void) {
  digitalWrite(P_LORA_TX_LED, LOW);   // turn TX LED off
  digitalWrite(P_LORA_PA_TX_EN, LOW);
}

void HeltecV4Board::enterDeepSleep(uint32_t secs, int pin_wake_btn) {
  esp_sleep_pd_config(ESP_PD_DOMAIN_RTC_PERIPH, ESP_PD_OPTION_ON);

  // Make sure the DIO1 and NSS GPIOs are held on required levels during deep sleep.
  rtc_gpio_set_direction((gpio_num_t)P_LORA_DIO_1, RTC_GPIO_MODE_INPUT_ONLY);
  rtc_gpio_pulldown_en((gpio_num_t)P_LORA_DIO_1);
  rtc_gpio_hold_en((gpio_num_t)P_LORA_NSS);

  // Hold GC1109 FEM pins during sleep to keep the RX path stable for wakeups.
  rtc_gpio_hold_en((gpio_num_t)P_LORA_PA_POWER);
  rtc_gpio_hold_en((gpio_num_t)P_LORA_PA_EN);

  if (pin_wake_btn < 0) {
    esp_sleep_enable_ext1_wakeup((1L << P_LORA_DIO_1), ESP_EXT1_WAKEUP_ANY_HIGH);
  } else {
    esp_sleep_enable_ext1_wakeup((1L << P_LORA_DIO_1) | (1L << pin_wake_btn), ESP_EXT1_WAKEUP_ANY_HIGH);
  }

  if (secs > 0) {
    esp_sleep_enable_timer_wakeup(secs * 1000000);
  }

  esp_deep_sleep_start();
}

void HeltecV4Board::powerOff() {
  enterDeepSleep(0);
}

uint16_t HeltecV4Board::getBattMilliVolts() {
  analogReadResolution(10);
  digitalWrite(PIN_ADC_CTRL, HIGH);
  delay(10);

  uint32_t raw = 0;
  for (int i = 0; i < 8; i++) {
    raw += analogRead(PIN_VBAT_READ);
  }
  raw = raw / 8;

  digitalWrite(PIN_ADC_CTRL, LOW);

  return (5.42 * (3.3 / 1024.0) * raw) * 1000;
}

const char* HeltecV4Board::getManufacturerName() const {
#ifdef HELTEC_LORA_V4_TFT
  return "Heltec V4 TFT";
#else
  return "Heltec V4 OLED";
#endif
}
