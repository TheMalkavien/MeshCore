#include "SSD1306Display.h"
#if defined(ESP_PLATFORM) && defined(CONFIG_PM_SLP_DISABLE_GPIO)
  #include <driver/gpio.h>
#endif

#if defined(ESP_PLATFORM) && defined(CONFIG_PM_SLP_DISABLE_GPIO)
static inline void keepPinActiveDuringSleep(int pin) {
  if (pin >= 0) {
    gpio_sleep_sel_dis((gpio_num_t)pin);
  }
}
#endif

// Arduino's Wire.begin() enables the internal pull-ups on SDA/SCL. Those sit on
// the always-on 3V3 rail, so once the panel rail is cut they keep trickling
// current into the dead OLED through its ESD clamp diodes. Releasing them while
// the panel is off removes that path; the bus is idle anyway, since the OLED is
// the only device on it. Boards that share this bus with a device on a
// different rail must leave OLED_RELEASE_BUS_WHEN_OFF at 0.
#ifndef OLED_RELEASE_BUS_WHEN_OFF
  #define OLED_RELEASE_BUS_WHEN_OFF 0
#endif

#ifndef OLED_POWER_ON_DELAY_MS
  #define OLED_POWER_ON_DELAY_MS 0
#endif

#if defined(ESP_PLATFORM) && OLED_RELEASE_BUS_WHEN_OFF && \
    defined(PIN_BOARD_SDA) && defined(PIN_BOARD_SCL)
static void setI2CBusIdlePullups(bool enabled) {
  const gpio_num_t pins[] = {(gpio_num_t)PIN_BOARD_SDA, (gpio_num_t)PIN_BOARD_SCL};
  for (gpio_num_t pin : pins) {
    (void)gpio_set_pull_mode(pin, enabled ? GPIO_PULLUP_ONLY : GPIO_FLOATING);
#if defined(CONFIG_PM_SLP_DISABLE_GPIO)
    // While the panel is powered the pads must stay under normal control across
    // automatic light sleep; once released, let the sleep isolation take over.
    if (enabled) {
      gpio_sleep_sel_dis(pin);
    } else {
      gpio_sleep_sel_en(pin);
    }
#endif
  }
}
  #define I2C_BUS_IDLE_PULLUPS(en) setI2CBusIdlePullups(en)
#else
  #define I2C_BUS_IDLE_PULLUPS(en) ((void)0)
#endif

bool SSD1306Display::i2c_probe(TwoWire& wire, uint8_t addr) {
  wire.beginTransmission(addr);
  uint8_t error = wire.endTransmission();
  return (error == 0);
}

// Color scheme
ColorVal UIColor::window_bkg = SSD1306_BLACK;
ColorVal UIColor::title_bkg = SSD1306_BLACK;
ColorVal UIColor::title_txt = SSD1306_WHITE;
ColorVal UIColor::primary_txt = SSD1306_WHITE;
ColorVal UIColor::secondary_txt = SSD1306_WHITE;
ColorVal UIColor::warning_txt = SSD1306_WHITE;
ColorVal UIColor::popup_bkg = SSD1306_BLACK;
ColorVal UIColor::popup_txt = SSD1306_WHITE;
ColorVal UIColor::corp_blue = SSD1306_WHITE;

bool SSD1306Display::begin() {
  if (!_isOn) {
    if (_peripher_power) {
      _peripher_power->claim();
#if OLED_POWER_ON_DELAY_MS > 0
      delay(OLED_POWER_ON_DELAY_MS);
#endif
    }
    _isOn = true;
  }
#if defined(ESP_PLATFORM) && defined(CONFIG_PM_SLP_DISABLE_GPIO)
  // Keep OLED control pins valid while ESP32 auto light-sleep is active.
  // Without this, RESET can float and blank the panel after boot.
  #if PIN_OLED_RESET >= 0
    keepPinActiveDuringSleep(PIN_OLED_RESET);
  #endif
  #if defined(PIN_BOARD_SDA) && (PIN_BOARD_SDA >= 0)
    keepPinActiveDuringSleep(PIN_BOARD_SDA);
  #endif
  #if defined(PIN_BOARD_SCL) && (PIN_BOARD_SCL >= 0)
    keepPinActiveDuringSleep(PIN_BOARD_SCL);
  #endif
#endif
  #ifdef DISPLAY_ROTATION
  display.setRotation(DISPLAY_ROTATION);
  #endif
  const bool ok = display.begin(SSD1306_SWITCHCAPVCC, DISPLAY_ADDRESS, true, false) && i2c_probe(Wire, DISPLAY_ADDRESS);
#ifdef OLED_CONTRAST
  if (ok) {
    // Lower panel current while keeping readable UI.
    display.ssd1306_command(SSD1306_SETCONTRAST);
    display.ssd1306_command((uint8_t)OLED_CONTRAST);
  }
#endif
  return ok;
}

void SSD1306Display::turnOn() {
  if (!_isOn) {
    if (_peripher_power) {
      _peripher_power->claim();
#if OLED_POWER_ON_DELAY_MS > 0
      delay(OLED_POWER_ON_DELAY_MS);
#endif
      I2C_BUS_IDLE_PULLUPS(true);  // restore the bus before the first transaction
    }
    _isOn = true;  // set before begin() to prevent double claim
    if (_peripher_power) begin();  // re-init display after power was cut
  }
#if defined(ESP_PLATFORM) && defined(CONFIG_PM_SLP_DISABLE_GPIO)
  #if PIN_OLED_RESET >= 0
    keepPinActiveDuringSleep(PIN_OLED_RESET);
  #endif
#endif
  display.ssd1306_command(SSD1306_DISPLAYON);
}

void SSD1306Display::turnOff() {
  display.ssd1306_command(SSD1306_DISPLAYOFF);
  if (_isOn) {
    if (_peripher_power) {
#if PIN_OLED_RESET >= 0
      // Driven low, not floated: sinking into an unpowered panel costs nothing
      // and holds it in reset.
      digitalWrite(PIN_OLED_RESET, LOW);
#endif
      _peripher_power->release();
      I2C_BUS_IDLE_PULLUPS(false);
    }
    _isOn = false;
  }
}

void SSD1306Display::clear() {
  display.clearDisplay();
  display.display();
}

void SSD1306Display::startFrame(ColorVal bkg) {
  display.clearDisplay();  // TODO: apply 'bkg'
  _color = SSD1306_WHITE;
  display.setTextColor(_color);
  display.setTextSize(1);
  display.cp437(true);         // Use full 256 char 'Code Page 437' font
}

void SSD1306Display::setTextSize(int sz) {
  display.setTextSize(sz);
}

void SSD1306Display::setColor(ColorVal c) {
  _color = c;
  display.setTextColor(_color);
}

void SSD1306Display::setCursor(int x, int y) {
  display.setCursor(x, y);
}

void SSD1306Display::print(const char* str) {
  display.print(str);
}

void SSD1306Display::fillRect(int x, int y, int w, int h) {
  display.fillRect(x, y, w, h, _color);
}

void SSD1306Display::drawRect(int x, int y, int w, int h) {
  display.drawRect(x, y, w, h, _color);
}

void SSD1306Display::drawXbm(int x, int y, const uint8_t* bits, int w, int h) {
  display.drawBitmap(x, y, bits, w, h, _color);
}

uint16_t SSD1306Display::getTextWidth(const char* str) {
  int16_t x1, y1;
  uint16_t w, h;
  display.getTextBounds(str, 0, 0, &x1, &y1, &w, &h);
  return w;
}

void SSD1306Display::endFrame() {
  display.display();
}
