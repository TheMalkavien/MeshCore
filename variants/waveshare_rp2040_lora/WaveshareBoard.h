#pragma once

#include <Arduino.h>
#include <MeshCore.h>
#include <helpers/RP2040OTA.h>

#if defined(ARDUINO_ARCH_RP2040)
  #include <hardware/clocks.h>
#endif

// LoRa radio module pins for Waveshare RP2040-LoRa-HF/LF
// https://files.waveshare.com/wiki/RP2040-LoRa/Rp2040-lora-sch.pdf

/*
 * This board has no built-in way to read battery voltage.
 * Nevertheless it's very easy to make it work, you only require two 1% resistors.
 *
 *    BAT+ -----+
 *              |
 *       VSYS --+ -/\/\/\/\- --+
 *                   200k      |
 *                             +-- GPIO28
 *                             |
 *        GND --+ -/\/\/\/\- --+
 *              |    100k
 *    BAT- -----+
 */
#ifndef PIN_VBAT_READ
  #define PIN_VBAT_READ          28
#endif
#define BATTERY_SAMPLES          8
#define ADC_MULTIPLIER           (3.0f * 3.3f * 1000)

class WaveshareBoard : public mesh::MainBoard {
protected:
  uint8_t startup_reason;
  RP2040OTAController ota;

public:
  void begin();
  uint8_t getStartupReason() const override { return startup_reason; }

#ifdef P_LORA_TX_LED
  void onBeforeTransmit() override { digitalWrite(P_LORA_TX_LED, HIGH); }
  void onAfterTransmit() override { digitalWrite(P_LORA_TX_LED, LOW); }
#endif

  uint16_t getBattMilliVolts() override {
#if defined(PIN_VBAT_READ) && defined(ADC_MULTIPLIER)
    analogReadResolution(12);

    uint32_t raw = 0;
    for (int i = 0; i < BATTERY_SAMPLES; i++) {
      raw += analogRead(PIN_VBAT_READ);
    }
    raw = raw / BATTERY_SAMPLES;

    return (ADC_MULTIPLIER * raw) / 4096;
#else
    return 0;
#endif
  }

  const char *getManufacturerName() const override { return "Waveshare RP2040-LoRa"; }

  void reboot() override { rp2040.reboot(); }

  bool startOTAUpdate(const char *id, char reply[]) override;
  bool handleOTACommand(const char *command, char reply[]) override;
  bool handleOTABinaryCommand(uint8_t opcode, const uint8_t *payload, size_t payload_len, char reply[]) override;
};

#if defined(ARDUINO_ARCH_RP2040)
#ifndef RP2040_ACTIVE_CLOCK_MHZ
  #ifdef RP2040_CPU_FREQ_MHZ
    #define RP2040_ACTIVE_CLOCK_MHZ RP2040_CPU_FREQ_MHZ
  #else
    #define RP2040_ACTIVE_CLOCK_MHZ 125
  #endif
#endif

#ifndef RP2040_OTA_CLOCK_MHZ
  #define RP2040_OTA_CLOCK_MHZ 125
#endif

inline void rp2040_apply_clock_profile(uint32_t clock_mhz) {
  set_sys_clock_khz(clock_mhz * KHZ, false);
  clock_configure(clk_peri,
                  0,
                  CLOCKS_CLK_PERI_CTRL_AUXSRC_VALUE_CLK_SYS,
                  clock_mhz * MHZ,
                  clock_mhz * MHZ);
  clock_configure(clk_adc, 0,
                  CLOCKS_CLK_ADC_CTRL_AUXSRC_VALUE_CLKSRC_PLL_SYS,
                  clock_mhz * MHZ, clock_mhz * MHZ);
  clock_configure(clk_rtc, 0, CLOCKS_CLK_RTC_CTRL_AUXSRC_VALUE_XOSC_CLKSRC, 12 * MHZ, 46875);
}

inline void rp2040_restore_active_profile() {
  rp2040_apply_clock_profile(RP2040_ACTIVE_CLOCK_MHZ);
}

inline void rp2040_enter_ota_profile() {
  rp2040_apply_clock_profile(RP2040_OTA_CLOCK_MHZ);
}
#endif
