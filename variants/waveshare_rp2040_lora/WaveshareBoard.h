#pragma once

#include <Arduino.h>
#include <MeshCore.h>

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
#define PIN_VBAT_READ            26
#define BATTERY_SAMPLES          8
#define ADC_MULTIPLIER           (3.0f * 3.3f * 1000)

class WaveshareBoard : public mesh::MainBoard {
protected:
  uint8_t startup_reason;
  float adc_mult = ADC_MULTIPLIER;
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

    return (adc_mult * raw * 1000) / 4096;
#else
    return 0;
#endif
  }
  bool setAdcMultiplier(float multiplier) override {
#if defined(PIN_VBAT_READ) && defined(ADC_MULTIPLIER)
     if (multiplier == 0.0f) {
      adc_mult = ADC_MULTIPLIER;}
    else {
      adc_mult = multiplier;
    }
    return true;
#else
    return false;
#endif
  }
  float getAdcMultiplier() const override {
#if defined(PIN_VBAT_READ) && defined(ADC_MULTIPLIER)
    if (adc_mult == 0.0f) {
      return ADC_MULTIPLIER;
    } else {
      return adc_mult;
    }
#else
    return 0.0f;
#endif
  }
  const char *getManufacturerName() const override { return "Waveshare RP2040-LoRa"; }

  void reboot() override { rp2040.reboot(); }

  bool startOTAUpdate(const char *id, char reply[]) override;
};

#ifdef MLK_RP2040_LOWPOWER
#include <hardware/vreg.h>
#define CLOCK_MHZ 48

inline void rp2040_lowpower() {
  vreg_set_voltage(VREG_VOLTAGE_0_90);
      /* Set the system frequency to 18 MHz. */
  set_sys_clock_khz(CLOCK_MHZ * KHZ, false);
  /* The previous line automatically detached clk_peri from clk_sys, and
      attached it to pll_usb. We need to attach clk_peri back to system PLL to keep SPI
      working at this low speed.
      For details see https://github.com/jgromes/RadioLib/discussions/938
  */
  clock_configure(clk_peri,
                  0,                                                // No glitchless mux
                  CLOCKS_CLK_PERI_CTRL_AUXSRC_VALUE_CLK_SYS, // System PLL on AUX mux
                  CLOCK_MHZ * MHZ,                                         // Input frequency
                  CLOCK_MHZ * MHZ                                          // Output (must be same as no divider)
  );
  /* Run also ADC on lower clk_sys. */
  clock_configure(clk_adc, 0, CLOCKS_CLK_ADC_CTRL_AUXSRC_VALUE_CLKSRC_PLL_SYS, 48 * MHZ, 48 * MHZ);
  /* Run RTC from XOSC since USB clock is off */
  clock_configure(clk_rtc, 0, CLOCKS_CLK_RTC_CTRL_AUXSRC_VALUE_XOSC_CLKSRC, 12 * MHZ, 46875);
}
#endif