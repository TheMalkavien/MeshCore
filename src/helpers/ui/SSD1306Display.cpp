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

bool SSD1306Display::i2c_probe(TwoWire& wire, uint8_t addr) {
  wire.beginTransmission(addr);
  uint8_t error = wire.endTransmission();
  return (error == 0);
}

bool SSD1306Display::begin() {
  if (!_isOn) {
    if (_peripher_power) _peripher_power->claim();
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
    if (_peripher_power) _peripher_power->claim();
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
      digitalWrite(PIN_OLED_RESET, LOW);
#endif
      _peripher_power->release();
    }
    _isOn = false;
  }
}

void SSD1306Display::clear() {
  display.clearDisplay();
  display.display();
}

void SSD1306Display::startFrame(Color bkg) {
  display.clearDisplay();  // TODO: apply 'bkg'
  _color = SSD1306_WHITE;
  display.setTextColor(_color);
  display.setTextSize(1);
  display.cp437(true);         // Use full 256 char 'Code Page 437' font
}

void SSD1306Display::setTextSize(int sz) {
  display.setTextSize(sz);
}

void SSD1306Display::setColor(Color c) {
  _color = (c != 0) ? SSD1306_WHITE : SSD1306_BLACK;
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
  display.drawBitmap(x, y, bits, w, h, SSD1306_WHITE);
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
