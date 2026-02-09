#include "WaveshareBoard.h"

#include <Arduino.h>
#include <Wire.h>

void WaveshareBoard::begin() {
  // for future use, sub-classes SHOULD call this from their begin()
  startup_reason = BD_STARTUP_NORMAL;

#ifdef P_LORA_TX_LED
  pinMode(P_LORA_TX_LED, OUTPUT);
#endif

#ifdef PIN_VBAT_READ
  pinMode(PIN_VBAT_READ, INPUT);
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
