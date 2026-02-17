#include "WaveshareBoard.h"

#include <Arduino.h>
#include <SPI.h>
#include <Wire.h>

#if defined(ARDUINO_ARCH_RP2040)
  #include <hardware/clocks.h>
  #include <hardware/sync.h>
#if !defined(NO_USB) && !defined(USE_TINYUSB)
  #include <USB.h>
#endif
#endif

#if defined(ARDUINO_ARCH_RP2040)
#if !defined(NO_USB) && !defined(USE_TINYUSB)
static bool g_usb_connected = true;

extern "C" bool meshcore_board_usb_on_demand(void) {
#ifdef MLK_RP2040_LOWPOWER
  // USB needs the active profile (clock + voltage) to enumerate reliably.
  rp2040_restore_active_profile();
#endif
  if (!g_usb_connected) {
    // Keep USB stack alive and only re-attach to host.
    USB.connect();
    delay(20);
    g_usb_connected = true;
  }
  return true;
}

extern "C" bool meshcore_board_usb_off_demand(void) {
  if (g_usb_connected) {
    Serial.flush();
    USB.disconnect();
    g_usb_connected = false;
  }
#ifdef MLK_RP2040_LOWPOWER
  // Once USB is down, keep runtime in lowest clock/voltage profile.
  rp2040_enter_sleep_profile();
#endif
  return true;
}

extern "C" bool meshcore_board_usb_is_connected(void) {
  return g_usb_connected;
}
#else
extern "C" bool meshcore_board_usb_on_demand(void) { return false; }
extern "C" bool meshcore_board_usb_off_demand(void) { return false; }
extern "C" bool meshcore_board_usb_is_connected(void) { return false; }
#endif
#endif

void WaveshareBoard::begin() {
  // for future use, sub-classes SHOULD call this from their begin()
  startup_reason = BD_STARTUP_NORMAL;

#if defined(ARDUINO_ARCH_RP2040) && defined(RP2040_CPU_FREQ_MHZ)
  set_sys_clock_khz(RP2040_CPU_FREQ_MHZ * 1000, true);
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

  #ifdef MLK_ESP32_FLASHER_PIN
    pinMode(MLK_ESP32_FLASHER_PIN, OUTPUT);
    digitalWrite(MLK_ESP32_FLASHER_PIN, LOW); // ensure ESP32Flasher is not woken up by default
  #endif
}


void WaveshareBoard::sleep(uint32_t secs) {
  // if MLK_RP2040_LOWPOWER is defined, we will disable usb insted of sleeping, as RP2040 doesn't have a real sleep mode, and disabling USB can save power significantly.
  #if defined(ARDUINO_ARCH_RP2040) && defined(MLK_RP2040_LOWPOWER)
    Serial.println("Disabling USB for low power sleep. Use commd usb on to re-enable.");
    meshcore_board_usb_off_demand();
  #endif
  return;
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
