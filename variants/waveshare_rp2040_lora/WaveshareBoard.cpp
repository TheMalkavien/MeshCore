#include "WaveshareBoard.h"

#include <Arduino.h>
#include <Wire.h>

void WaveshareBoard::begin() {
  // for future use, sub-classes SHOULD call this from their begin()
  startup_reason = BD_STARTUP_NORMAL;

#if defined(ARDUINO_ARCH_RP2040)
  rp2040_restore_active_profile();
#endif

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
}

bool WaveshareBoard::startOTAUpdate(const char *id, char reply[]) {
#if defined(ARDUINO_ARCH_RP2040)
  rp2040_enter_ota_profile();
#endif
  return ota.startSession(id, reply);
}

bool WaveshareBoard::handleOTACommand(const char *command, char reply[]) {
#if defined(ARDUINO_ARCH_RP2040)
  rp2040_enter_ota_profile();
#endif
  bool ok = ota.handleCommand(command, reply);
#if defined(ARDUINO_ARCH_RP2040)
  if (!ota.isSleepInhibited()) {
    rp2040_restore_active_profile();
  }
#endif
  return ok;
}

bool WaveshareBoard::handleOTABinaryCommand(uint8_t opcode, const uint8_t *payload, size_t payload_len, char reply[]) {
#if defined(ARDUINO_ARCH_RP2040)
  rp2040_enter_ota_profile();
#endif
  bool ok = ota.handleBinaryCommand(opcode, payload, payload_len, reply);
#if defined(ARDUINO_ARCH_RP2040)
  if (!ota.isSleepInhibited()) {
    rp2040_restore_active_profile();
  }
#endif
  return ok;
}
