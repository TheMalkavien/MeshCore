#include "tracker_peripherals.h"

#include <math.h>

#if defined(NRF52_PLATFORM)
  #include <nrf.h>
  #include <nrf_soc.h>
#endif

#if defined(T1000_E)
extern float t1000e_get_temperature(void);
extern uint32_t t1000e_get_light(void);
#endif

static unsigned long tracker_tx_led_until = 0;
static bool tracker_now_requested = false;
static bool tracker_external_power_led_mode = false;
static unsigned long tracker_external_power_led_epoch = 0;
static unsigned long tracker_external_power_last_dbg_ms = 0;

static void trackerSetStatusLed(bool on) {
#if defined(LED_PIN) && (LED_PIN >= 0)
  #ifdef LED_STATE_ON
  digitalWrite(LED_PIN, on ? LED_STATE_ON : ((LED_STATE_ON == HIGH) ? LOW : HIGH));
  #else
  digitalWrite(LED_PIN, on ? HIGH : LOW);
  #endif
#elif defined(P_LORA_TX_LED) && (P_LORA_TX_LED >= 0)
  #ifdef P_LORA_TX_LED_ON
  digitalWrite(P_LORA_TX_LED, on ? P_LORA_TX_LED_ON : ((P_LORA_TX_LED_ON == HIGH) ? LOW : HIGH));
  #else
  digitalWrite(P_LORA_TX_LED, on ? HIGH : LOW);
  #endif
#else
  (void)on;
#endif
}

static void trackerPulseStatusLed(uint16_t duration_ms) {
  if (tracker_external_power_led_mode) {
    return;
  }
  trackerSetStatusLed(true);
  tracker_tx_led_until = millis() + duration_ms;
}

static void trackerPulseTxLed() {
  trackerPulseStatusLed(TRACKER_TX_LED_PULSE_MS);
}

static void trackerPulseButtonLed() {
  trackerPulseStatusLed(TRACKER_BUTTON_LED_PULSE_MS);
}

static void trackerFlashBootLed() {
  trackerSetStatusLed(true);
  delay(TRACKER_BOOT_LED_PULSE_MS);
  trackerSetStatusLed(false);
}

static void handleTrackerTxLedPulse() {
  if (tracker_external_power_led_mode) {
    return;
  }
  if (tracker_tx_led_until != 0 && (int32_t)(millis() - tracker_tx_led_until) >= 0) {
    tracker_tx_led_until = 0;
    trackerSetStatusLed(false);
  }
}

static void trackerPlayExternalPowerPlugBuzzerSequence();
static void trackerPlayExternalPowerUnplugBuzzerSequence();

#if defined(T1000_E) && defined(NRF52_PLATFORM)
static bool trackerReadT1000ExternalPowerRaw(uint32_t* usb_status_out, bool* via_softdevice_out) {
  uint32_t usb_status = 0;
  bool via_softdevice = false;

  uint8_t sd_enabled = 0;
  bool sd_ok = (sd_softdevice_is_enabled(&sd_enabled) == NRF_SUCCESS);
  if (sd_ok && sd_enabled) {
    if (sd_power_usbregstatus_get(&usb_status) == NRF_SUCCESS) {
      via_softdevice = true;
    } else {
      usb_status = NRF_POWER->USBREGSTATUS;
    }
  } else {
    usb_status = NRF_POWER->USBREGSTATUS;
  }

  if (usb_status_out) {
    *usb_status_out = usb_status;
  }
  if (via_softdevice_out) {
    *via_softdevice_out = via_softdevice;
  }
  return (usb_status & POWER_USBREGSTATUS_VBUSDETECT_Msk) != 0;
}
#endif

bool trackerPeripheralsIsExternalPowered() {
#if defined(T1000_E) && defined(NRF52_PLATFORM)
  return trackerReadT1000ExternalPowerRaw(NULL, NULL);
#else
  return board.isExternalPowered();
#endif
}

static void trackerDebugExternalPowerState(const char* tag, bool force_log = false) {
#if TRACKER_SERIAL_DEBUG == 1
  if (!force_log) {
    if (TRACKER_EXT_POWER_DEBUG_LOG_MS == 0) {
      return;
    }
    if (tracker_external_power_last_dbg_ms != 0 &&
        (uint32_t)(millis() - tracker_external_power_last_dbg_ms) < (uint32_t)TRACKER_EXT_POWER_DEBUG_LOG_MS) {
      return;
    }
  }
  tracker_external_power_last_dbg_ms = millis();

  #if defined(T1000_E) && defined(NRF52_PLATFORM)
  uint32_t usb_status = 0;
  bool via_softdevice = false;
  bool powered = trackerReadT1000ExternalPowerRaw(&usb_status, &via_softdevice);
  TRACKER_DBG("ext power [%s]: powered=%d usbreg=0x%08lX source=%s",
    tag ? tag : "?",
    powered ? 1 : 0,
    (unsigned long)usb_status,
    via_softdevice ? "softdevice" : "register");
  #else
  bool powered = board.isExternalPowered();
  TRACKER_DBG("ext power [%s]: powered=%d source=board.isExternalPowered()",
    tag ? tag : "?",
    powered ? 1 : 0);
  #endif
#else
  (void)tag;
  (void)force_log;
#endif
}

static void trackerHandleExternalPowerLed(bool external_powered) {
  if (external_powered) {
    if (!tracker_external_power_led_mode) {
      tracker_external_power_led_mode = true;
      tracker_external_power_led_epoch = millis();
      tracker_tx_led_until = 0;
      TRACKER_DBG("external power: LED double-blink enabled");
      trackerPlayExternalPowerPlugBuzzerSequence();
      trackerDebugExternalPowerState("plug", true);
    }

    const uint32_t elapsed = (uint32_t)(millis() - tracker_external_power_led_epoch);
    const uint32_t phase = elapsed % (uint32_t)TRACKER_EXT_POWER_LED_PERIOD_MS;
    const uint32_t second_on_start = (uint32_t)TRACKER_EXT_POWER_LED_ON_MS + (uint32_t)TRACKER_EXT_POWER_LED_GAP_MS;
    const uint32_t second_on_end = second_on_start + (uint32_t)TRACKER_EXT_POWER_LED_ON_MS;
    bool led_on = (phase < (uint32_t)TRACKER_EXT_POWER_LED_ON_MS) ||
                  (phase >= second_on_start && phase < second_on_end);
    trackerSetStatusLed(led_on);
  } else if (tracker_external_power_led_mode) {
    tracker_external_power_led_mode = false;
    tracker_external_power_led_epoch = 0;
    trackerSetStatusLed(false);
    TRACKER_DBG("external power: LED double-blink disabled");
    trackerPlayExternalPowerUnplugBuzzerSequence();
    trackerDebugExternalPowerState("unplug", true);
  }
}

static void trackerInitBuzzerPins() {
#if defined(PIN_BUZZER_EN)
  pinMode(PIN_BUZZER_EN, OUTPUT);
  digitalWrite(PIN_BUZZER_EN, LOW);
  TRACKER_DBG("buzzer enable pin configured: PIN_BUZZER_EN=%d", PIN_BUZZER_EN);
#endif
#if defined(PIN_BUZZER) && (PIN_BUZZER >= 0)
  pinMode(PIN_BUZZER, OUTPUT);
  digitalWrite(PIN_BUZZER, LOW);
  TRACKER_DBG("buzzer pin configured: PIN_BUZZER=%d", PIN_BUZZER);
#else
  TRACKER_DBG("buzzer disabled: PIN_BUZZER not defined in this build");
#endif
}

static void trackerEnableBuzzer(bool on) {
#if defined(PIN_BUZZER_EN)
  digitalWrite(PIN_BUZZER_EN, on ? HIGH : LOW);
#else
  (void)on;
#endif
}

static void trackerPlayToneBlocking(uint16_t freq_hz, uint16_t duration_ms) {
#if defined(PIN_BUZZER) && (PIN_BUZZER >= 0)
  if (duration_ms == 0) {
    return;
  }
  if (freq_hz == 0) {
    delay(duration_ms);
    return;
  }

  uint32_t period_us = 1000000UL / (uint32_t)freq_hz;
  if (period_us < 2) {
    period_us = 2;
  }
  uint32_t half_period_us = period_us / 2UL;
  uint32_t cycles = ((uint32_t)duration_ms * 1000UL) / period_us;
  if (cycles == 0) {
    cycles = 1;
  }

  for (uint32_t i = 0; i < cycles; i++) {
    digitalWrite(PIN_BUZZER, HIGH);
    delayMicroseconds((unsigned int)half_period_us);
    digitalWrite(PIN_BUZZER, LOW);
    delayMicroseconds((unsigned int)half_period_us);
  }
#else
  (void)freq_hz;
  (void)duration_ms;
#endif
}

static void trackerPlayBuzzerSequence(const uint16_t* notes_hz, size_t note_count) {
#if defined(PIN_BUZZER) && (PIN_BUZZER >= 0)
  if (!notes_hz || note_count == 0) {
    return;
  }

  TRACKER_DBG("buzzer sequence: notes=%u", (unsigned)note_count);
  trackerEnableBuzzer(true);
  for (size_t i = 0; i < note_count; i++) {
    trackerPlayToneBlocking(notes_hz[i], TRACKER_BUZZER_NOTE_MS);
    if (i + 1 < note_count) {
      delay(TRACKER_BUZZER_NOTE_GAP_MS);
    }
  }
  digitalWrite(PIN_BUZZER, LOW);
  trackerEnableBuzzer(false);
#else
  TRACKER_DBG("buzzer sequence skipped: no PIN_BUZZER");
  (void)notes_hz;
  (void)note_count;
#endif
}

static void trackerPlayBootBuzzerSequence() {
  static const uint16_t notes_hz[4] = {523, 659, 784, 988};
  trackerPlayBuzzerSequence(notes_hz, sizeof(notes_hz) / sizeof(notes_hz[0]));
}

static void trackerPlayShutdownBuzzerSequence() {
  static const uint16_t notes_hz[4] = {988, 784, 659, 523};
  trackerPlayBuzzerSequence(notes_hz, sizeof(notes_hz) / sizeof(notes_hz[0]));
}

static void trackerPlayExternalPowerPlugBuzzerSequence() {
  static const uint16_t notes_hz[2] = {659, 988};
  trackerPlayBuzzerSequence(notes_hz, sizeof(notes_hz) / sizeof(notes_hz[0]));
}

static void trackerPlayExternalPowerUnplugBuzzerSequence() {
  static const uint16_t notes_hz[2] = {988, 659};
  trackerPlayBuzzerSequence(notes_hz, sizeof(notes_hz) / sizeof(notes_hz[0]));
}

static void trackerInitPowerButtonPin() {
#if defined(PIN_USER_BTN) && (PIN_USER_BTN >= 0)
  #ifdef USER_BTN_PRESSED
    #if USER_BTN_PRESSED == HIGH
  pinMode(PIN_USER_BTN, INPUT_PULLDOWN);
    #else
  pinMode(PIN_USER_BTN, INPUT_PULLUP);
    #endif
  #else
  pinMode(PIN_USER_BTN, INPUT_PULLUP);
  #endif
#elif defined(BUTTON_PIN) && (BUTTON_PIN >= 0)
  #ifdef USER_BTN_PRESSED
    #if USER_BTN_PRESSED == HIGH
  pinMode(BUTTON_PIN, INPUT_PULLDOWN);
    #else
  pinMode(BUTTON_PIN, INPUT_PULLUP);
    #endif
  #else
  pinMode(BUTTON_PIN, INPUT_PULLUP);
  #endif
#endif
}

static void trackerForceSystemOffIfSupported() {
#if defined(NRF52_PLATFORM)
  uint8_t sd_enabled = 0;
  if (sd_softdevice_is_enabled(&sd_enabled) == NRF_SUCCESS && sd_enabled) {
    uint32_t err = sd_power_system_off();
    if (err == NRF_SUCCESS) {
      return;
    }
  }
  NRF_POWER->SYSTEMOFF = POWER_SYSTEMOFF_SYSTEMOFF_Enter;
  NVIC_SystemReset();
#endif
}

bool trackerPeripheralsPowerButtonPressed() {
#if defined(PIN_USER_BTN) && (PIN_USER_BTN >= 0)
  int v = digitalRead(PIN_USER_BTN);
  #ifdef USER_BTN_PRESSED
  return v == USER_BTN_PRESSED;
  #else
  return v == LOW;
  #endif
#elif defined(BUTTON_PIN) && (BUTTON_PIN >= 0)
  int v = digitalRead(BUTTON_PIN);
  #ifdef USER_BTN_PRESSED
  return v == USER_BTN_PRESSED;
  #else
  return v == LOW;
  #endif
#else
  return false;
#endif
}

static void handleTrackerPowerButton() {
#if (defined(PIN_USER_BTN) && (PIN_USER_BTN >= 0)) || (defined(BUTTON_PIN) && (BUTTON_PIN >= 0))
  static bool inited = false;
  static bool prev_pressed = false;
  static bool long_sent = false;
  static bool press_armed = false;
  static unsigned long pressed_since = 0;

  bool pressed = trackerPeripheralsPowerButtonPressed();
  if (!inited) {
    inited = true;
    prev_pressed = pressed;
    pressed_since = 0;
    press_armed = false;
    return;
  }

  if (pressed && !prev_pressed) {
    trackerPulseButtonLed();
    pressed_since = millis();
    long_sent = false;
    press_armed = true;
  } else if (!pressed && prev_pressed) {
    if (press_armed) {
      uint32_t press_ms = (uint32_t)(millis() - pressed_since);
      if (!long_sent &&
          press_ms >= TRACKER_POWER_BUTTON_SHORT_MIN_MS &&
          press_ms < TRACKER_POWER_BUTTON_HOLD_MS) {
        TRACKER_DBG("power button short press -> tracker now");
        tracker_now_requested = true;
      }
    }
    press_armed = false;
    long_sent = false;
    pressed_since = 0;
  }

  if (pressed && press_armed && !long_sent &&
      (uint32_t)(millis() - pressed_since) >= TRACKER_POWER_BUTTON_HOLD_MS) {
    long_sent = true;
    press_armed = false;
    TRACKER_DBG("power button long press -> shutdown melody");
    trackerPlayShutdownBuzzerSequence();
    TRACKER_DBG("power button long press -> power off");
    radio_driver.powerOff();
    board.powerOff();
#if defined(NRF52_PLATFORM)
    TRACKER_DBG("board.powerOff returned, forcing nRF52 SYSTEMOFF");
    trackerForceSystemOffIfSupported();
#endif
  }

  prev_pressed = pressed;
#endif
}

void trackerPeripheralsBegin() {
  trackerInitPowerButtonPin();
  TRACKER_DBG("button pin configured");
  trackerFlashBootLed();
  TRACKER_DBG("boot led pulse");
  trackerInitBuzzerPins();
  trackerPlayBootBuzzerSequence();
  TRACKER_DBG("boot buzzer sequence");
  trackerDebugExternalPowerState("setup", true);
}

void trackerPeripheralsLoop() {
  trackerDebugExternalPowerState("loop");
  trackerHandleExternalPowerLed(trackerPeripheralsIsExternalPowered());
  handleTrackerTxLedPulse();
  handleTrackerPowerButton();
}

void trackerPeripheralsNotifyTx() {
  trackerPulseTxLed();
}

bool trackerPeripheralsConsumeImmediateCycleRequest() {
  if (!tracker_now_requested) {
    return false;
  }
  tracker_now_requested = false;
  return true;
}

bool trackerPeripheralsTxLedActive() {
  return (tracker_tx_led_until != 0) && ((int32_t)(millis() - tracker_tx_led_until) < 0);
}

void trackerPeripheralsPrepareForIdleSleep() {
  tracker_tx_led_until = 0;
  trackerSetStatusLed(false);
}

TrackerEnvExtras trackerReadEnvExtras() {
  TrackerEnvExtras extras;
#if defined(T1000_E)
  float temp = t1000e_get_temperature();
  if (!isnan(temp) && !isinf(temp)) {
    extras.has_temperature = true;
    extras.temperature_c = temp;
  }
  extras.has_light = true;
  extras.light_level = t1000e_get_light();
#endif
  return extras;
}

void trackerBuildEnvExtrasJson(char* out, size_t out_len) {
  if (!out || out_len == 0) {
    return;
  }
  out[0] = 0;

  TrackerEnvExtras extras = trackerReadEnvExtras();
  size_t used = 0;

  if (extras.has_temperature) {
    int n = snprintf(out + used, out_len - used, ",\"temp\":%.1f", extras.temperature_c);
    if (n > 0 && (size_t)n < (out_len - used)) {
      used += (size_t)n;
    } else {
      out[out_len - 1] = 0;
      return;
    }
  }

  if (extras.has_light) {
    int n = snprintf(out + used, out_len - used, ",\"light\":%lu", (unsigned long)extras.light_level);
    if (n > 0 && (size_t)n < (out_len - used)) {
      used += (size_t)n;
    } else {
      out[out_len - 1] = 0;
    }
  }
}
