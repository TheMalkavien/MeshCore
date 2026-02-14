#include "WaveshareBoard.h"

#include <Arduino.h>
#include <Wire.h>

#if defined(ARDUINO_ARCH_RP2040)
  #include <hardware/clocks.h>
  #include <hardware/sync.h>
#endif

#if defined(ARDUINO_ARCH_RP2040) && defined(P_LORA_DIO_1)
static volatile bool g_lora_wake_irq = false;

extern "C" bool meshcore_radio_irq_pending(void);
extern "C" bool meshcore_radio_prepare_for_sleep(void);
extern "C" bool meshcore_radio_hw_irq_pending(void) __attribute__((weak));
extern "C" bool meshcore_radio_hw_irq_pending(void) {
  return false;
}
extern "C" void meshcore_on_lora_dio1_irq(void) {
  g_lora_wake_irq = true;
}
#endif

void WaveshareBoard::begin() {
  // for future use, sub-classes SHOULD call this from their begin()
  startup_reason = BD_STARTUP_NORMAL;
  _sleep_until_millis = 0;

#if defined(ARDUINO_ARCH_RP2040) && defined(RP2040_CPU_FREQ_MHZ)
  set_sys_clock_khz(RP2040_CPU_FREQ_MHZ * 1000, true);
#endif

#ifdef P_LORA_TX_LED
  pinMode(P_LORA_TX_LED, OUTPUT);
#endif

#ifdef PIN_VBAT_READ
  pinMode(PIN_VBAT_READ, INPUT);
#endif

#ifdef P_LORA_DIO_1
  pinMode(P_LORA_DIO_1, INPUT);
  #if defined(ARDUINO_ARCH_RP2040)
    // RadioLib installs its own DIO1 interrupt callback. Wake flag is set
    // via meshcore_on_lora_dio1_irq() from RadioLibWrappers.cpp.
    g_lora_wake_irq = false;
  #endif
#endif

#if defined(PIN_BOARD_SDA) && defined(PIN_BOARD_SCL)
  Wire.setSDA(PIN_BOARD_SDA);
  Wire.setSCL(PIN_BOARD_SCL);
#endif

  Wire.begin();

  delay(10); // give sx1262 some time to power up

  #ifdef MLK_RP2040_LOWPOWER
    rp2040_lowpower();
  #endif
  #ifdef MLK_ESP32_FLASHER_PIN
    pinMode(MLK_ESP32_FLASHER_PIN, OUTPUT);
    digitalWrite(MLK_ESP32_FLASHER_PIN, LOW); // ensure ESP32Flasher is not woken up by default
  #endif
}


void WaveshareBoard::sleep(uint32_t secs) {
#if defined(ARDUINO_ARCH_RP2040)
  if (secs == 0) return;

  // Sleep window with immediate wake on LoRa DIO1 assertion.
  _sleep_until_millis = millis() + ((uint32_t)secs * 1000);
  #ifdef P_LORA_DIO_1
    if (meshcore_radio_prepare_for_sleep()) {
      return;
    }

    // Avoid missing an already-pending edge between flag clear and first WFI.
    noInterrupts();
    g_lora_wake_irq = false;
    bool dio1_high = (digitalRead(P_LORA_DIO_1) == HIGH);
    interrupts();
    if (dio1_high) {
      return;
    }
  #endif

  while ((int32_t)(millis() - _sleep_until_millis) < 0) {
    #ifdef P_LORA_DIO_1
      if (g_lora_wake_irq || digitalRead(P_LORA_DIO_1) == HIGH || meshcore_radio_irq_pending() || meshcore_radio_hw_irq_pending()) {
        break;  // packet IRQ latched/pending -> return immediately to process it
      }
    #endif
    __wfi();
  }

#else
  delay(secs * 1000);
#endif
}

bool WaveshareBoard::startOTAUpdate(const char *id, char reply[]) {
  #ifdef MLK_ESP32_FLASHER_PIN
    pinMode(MLK_ESP32_FLASHER_PIN, OUTPUT);
    digitalWrite(MLK_ESP32_FLASHER_PIN, HIGH); // wake up esp32flasher
    delay(500);
    digitalWrite(MLK_ESP32_FLASHER_PIN, LOW); // stay low to avoid another wake up
    sprintf(reply, "Waking UP ESP32Flasher to flash the firmware.");
  return true;
  #else
    sprintf(reply, "OTA update not supported on this board");
    return false;
  #endif
}
