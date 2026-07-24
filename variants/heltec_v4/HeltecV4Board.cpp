#include "HeltecV4Board.h"

static inline void biasInputPin(int pin, uint8_t mode) {
  if (pin < 0) return;
  pinMode((uint8_t)pin, mode);
}

// Compile-time guard: a pin listed in LOW_POWER_UNUSED_GPIO_* must never overlap
// a radio/FEM/GPS/OLED/ADC/button pin, otherwise configureLowPowerPins() would
// re-bias it as a floating input after the peripheral was set up (this is exactly
// how GPIO5 == P_LORA_KCT8103L_PA_CTX silently broke RX/TX on the V4.3 FEM).
static constexpr bool isReservedLowPowerPin(int pin) {
  return false
#ifdef P_LORA_NSS
    || pin == P_LORA_NSS
#endif
#ifdef P_LORA_SCLK
    || pin == P_LORA_SCLK
#endif
#ifdef P_LORA_MISO
    || pin == P_LORA_MISO
#endif
#ifdef P_LORA_MOSI
    || pin == P_LORA_MOSI
#endif
#ifdef P_LORA_DIO_1
    || pin == P_LORA_DIO_1
#endif
#ifdef P_LORA_BUSY
    || pin == P_LORA_BUSY
#endif
#ifdef P_LORA_RESET
    || pin == P_LORA_RESET
#endif
#ifdef P_LORA_TX_LED
    || pin == P_LORA_TX_LED
#endif
#ifdef P_LORA_PA_POWER
    || pin == P_LORA_PA_POWER
#endif
#ifdef P_LORA_GC1109_PA_EN
    || pin == P_LORA_GC1109_PA_EN
#endif
#ifdef P_LORA_GC1109_PA_TX_EN
    || pin == P_LORA_GC1109_PA_TX_EN
#endif
#ifdef P_LORA_KCT8103L_PA_CSD
    || pin == P_LORA_KCT8103L_PA_CSD
#endif
#ifdef P_LORA_KCT8103L_PA_CTX
    || pin == P_LORA_KCT8103L_PA_CTX
#endif
#ifdef PIN_USER_BTN
    || pin == PIN_USER_BTN
#endif
#ifdef PIN_VEXT_EN
    || pin == PIN_VEXT_EN
#endif
#ifdef PIN_ADC_CTRL
    || pin == PIN_ADC_CTRL
#endif
#ifdef PIN_VBAT_READ
    || pin == PIN_VBAT_READ
#endif
#ifdef PIN_GPS_EN
    || pin == PIN_GPS_EN
#endif
#ifdef PIN_GPS_RX
    || pin == PIN_GPS_RX
#endif
#ifdef PIN_GPS_TX
    || pin == PIN_GPS_TX
#endif
#ifdef PIN_GPS_RESET
    || pin == PIN_GPS_RESET
#endif
#ifdef PIN_GPS_SLEEP
    || pin == PIN_GPS_SLEEP
#endif
#ifdef PIN_BOARD_SDA
    || pin == PIN_BOARD_SDA
#endif
#ifdef PIN_BOARD_SCL
    || pin == PIN_BOARD_SCL
#endif
#ifdef PIN_OLED_RESET
    || pin == PIN_OLED_RESET
#endif
    ;
}

void HeltecV4Board::configureLowPowerPins() {
#ifdef LOW_POWER_UNUSED_GPIO_1
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_GPIO_1), "LOW_POWER_UNUSED_GPIO_1 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif
#ifdef LOW_POWER_UNUSED_GPIO_2
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_GPIO_2), "LOW_POWER_UNUSED_GPIO_2 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif
#ifdef LOW_POWER_UNUSED_GPIO_3
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_GPIO_3), "LOW_POWER_UNUSED_GPIO_3 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif
#ifdef LOW_POWER_UNUSED_GPIO_4
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_GPIO_4), "LOW_POWER_UNUSED_GPIO_4 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif
#ifdef LOW_POWER_UNUSED_GPIO_5
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_GPIO_5), "LOW_POWER_UNUSED_GPIO_5 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif
#ifdef LOW_POWER_UNUSED_GPIO_6
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_GPIO_6), "LOW_POWER_UNUSED_GPIO_6 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif
#ifdef LOW_POWER_UNUSED_GPIO_7
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_GPIO_7), "LOW_POWER_UNUSED_GPIO_7 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif
#ifdef LOW_POWER_UNUSED_GPIO_8
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_GPIO_8), "LOW_POWER_UNUSED_GPIO_8 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif
#ifdef LOW_POWER_UNUSED_PULLUP_GPIO_1
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_PULLUP_GPIO_1), "LOW_POWER_UNUSED_PULLUP_GPIO_1 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif
#ifdef LOW_POWER_UNUSED_PULLUP_GPIO_2
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_PULLUP_GPIO_2), "LOW_POWER_UNUSED_PULLUP_GPIO_2 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif
#ifdef LOW_POWER_UNUSED_PULLUP_GPIO_3
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_PULLUP_GPIO_3), "LOW_POWER_UNUSED_PULLUP_GPIO_3 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif
#ifdef LOW_POWER_UNUSED_PULLUP_GPIO_4
  static_assert(!isReservedLowPowerPin(LOW_POWER_UNUSED_PULLUP_GPIO_4), "LOW_POWER_UNUSED_PULLUP_GPIO_4 overlaps a reserved radio/FEM/GPS/OLED pin");
#endif

#if defined(PIN_USER_BTN)
  // Heltec V4 user button is active-low on GPIO0.
  biasInputPin(PIN_USER_BTN, INPUT_PULLUP);
#endif

  // Optional user-defined unused GPIO list (bias to GND). Keep this list
  // conservative: FEM, display and radio pins must never be listed here.
  // Example build_flags:
  //   -D LOW_POWER_UNUSED_GPIO_1=6
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

  // Keep the revision-aware FEM abstraction: it handles both the V4.2
  // GC1109 and the V4.3 KCT8103L pin mappings.
  loRaFEMControl.init();

  periph_power.begin();
  configureLowPowerPins();

  esp_reset_reason_t reason = esp_reset_reason();
  if (reason == ESP_RST_DEEPSLEEP) {
    long wakeup_source = esp_sleep_get_ext1_wakeup_status();
    if (wakeup_source & (1ULL << P_LORA_DIO_1)) {
      startup_reason = BD_STARTUP_RX_PACKET;
    }

    rtc_gpio_hold_dis((gpio_num_t)P_LORA_NSS);
    rtc_gpio_deinit((gpio_num_t)P_LORA_DIO_1);
  }
}

void HeltecV4Board::onBeforeTransmit(void) {
  digitalWrite(P_LORA_TX_LED, HIGH); // turn TX LED on
  loRaFEMControl.setTxModeEnable();
}

void HeltecV4Board::onAfterTransmit(void) {
  digitalWrite(P_LORA_TX_LED, LOW); // turn TX LED off
  loRaFEMControl.setRxModeEnable();
}

void HeltecV4Board::powerOff() {
  // powerOff is hibernation, not network standby: shut the FEM down before
  // delegating to the common ESP32 deep-sleep implementation. Keep the
  // active-low user button as the sole wake source.
  loRaFEMControl.setSleepModeEnable();
  ESP32Board::enterDeepSleep(0, PIN_USER_BTN, LOW);
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

  return (adc_mult * (3.3 / 1024.0) * raw) * 1000;
}

const char* HeltecV4Board::getManufacturerName() const {
#ifdef HELTEC_LORA_V4_TFT
  return loRaFEMControl.getFEMType() == KCT8103L_PA ? "Heltec V4.3 TFT" : "Heltec V4 TFT";
#else
  return loRaFEMControl.getFEMType() == KCT8103L_PA ? "Heltec V4.3 OLED" : "Heltec V4 OLED";
#endif
}

bool HeltecV4Board::setLoRaFemLnaEnabled(bool enable) {
  if (!loRaFEMControl.isLnaCanControl()) {
    return false;
  }

  loRaFEMControl.setLNAEnable(enable);
  loRaFEMControl.setRxModeEnable();
  return true;
}

bool HeltecV4Board::canControlLoRaFemLna() const {
  return loRaFEMControl.isLnaCanControl();
}

bool HeltecV4Board::isLoRaFemLnaEnabled() const {
  return loRaFEMControl.isLNAEnabled();
}
