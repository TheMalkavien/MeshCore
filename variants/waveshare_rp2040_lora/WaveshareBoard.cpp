#include "WaveshareBoard.h"

#include <Arduino.h>
#include <Wire.h>
#include <hardware/vreg.h>
#define CLOCK_MHZ 48

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


digitalWrite(22, LOW); // ensure ESP32Flasher is not woken up by default
  Wire.begin();

  delay(10); // give sx1262 some time to power up
}

bool WaveshareBoard::startOTAUpdate(const char *id, char reply[]) {
    pinMode(22, OUTPUT);
    digitalWrite(22, HIGH); // wake up esp32flasher
    delay(500);
    digitalWrite(22, LOW); // stay low to avoid another wake up
    sprintf(reply, "Waking UP ESP32Flasher to flash the firmware.");
  return true;
}
