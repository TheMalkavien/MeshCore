#include "ESP32EventLoop.h"

#if defined(EVENT_DRIVEN_LOOP) && defined(ESP32) && defined(ESP_PLATFORM)

#include <Arduino.h>
#include <driver/gpio.h>
#include <esp_sleep.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>

#if defined(CONFIG_PM_ENABLE)
  #include <esp_pm.h>
#endif

#if defined(CONFIG_IDF_TARGET_ESP32S3)
  #include <soc/gpio_struct.h>
  #include <soc/rtc_cntl_reg.h>
  #include <soc/soc.h>
  #include <soc/usb_serial_jtag_reg.h>
#endif

#ifndef PM_MAX_FREQ_MHZ
  #define PM_MAX_FREQ_MHZ 80
#endif

#ifndef PM_MIN_FREQ_MHZ
  #define PM_MIN_FREQ_MHZ 40
#endif

#ifndef EVENT_LOOP_BUTTON_MAX_MS
  #define EVENT_LOOP_BUTTON_MAX_MS 30
#endif

static TaskHandle_t g_main_loop_task = nullptr;

extern "C" bool meshcore_radio_irq_pending(void);

#if defined(PIN_USER_BTN)
static volatile uint32_t g_button_irq_count = 0;
#endif

#if defined(CONFIG_IDF_TARGET_ESP32S3)
#if defined(P_LORA_DIO_1)
static volatile bool g_lora_gpio_wake_armed = false;
static volatile bool g_lora_active_level_latched = false;
#endif
#if defined(PIN_USER_BTN)
static volatile bool g_button_gpio_wake_armed = false;
static volatile bool g_button_active_level_latched = false;
#endif

static inline void IRAM_ATTR disableGPIOInterruptFromISR(gpio_num_t pin) {
  // gpio_intr_disable() is not IRAM-safe in ESP-IDF 4.4. The RadioLib ISR can
  // run while the flash cache is disabled, so use the equivalent S3 register.
  GPIO.pin[(uint32_t)pin].int_ena = 0;
}

static bool armLevelWakeSafely(gpio_num_t pin, int active_level,
                               gpio_int_type_t wake_type,
                               gpio_int_type_t normal_type,
                               volatile bool* armed,
                               volatile bool* active_level_latched) {
  if (*armed) return true;

  (void)gpio_intr_disable(pin);
  (void)gpio_wakeup_disable(pin);
  (void)gpio_set_intr_type(pin, normal_type);

  const bool active = gpio_get_level(pin) == active_level;
  if (active && *active_level_latched) {
#if defined(P_LORA_DIO_1)
    if (pin != (gpio_num_t)P_LORA_DIO_1 || meshcore_radio_irq_pending()) {
      // The original event is still pending. Keep the level interrupt masked
      // until RadioLib consumes it, otherwise it would retrigger forever.
      return false;
    }
    // RadioLib consumed the original event, but DIO1 is high again: another
    // packet completed while the GPIO interrupt was masked. Re-arm the level
    // interrupt so that second event is delivered immediately.
    *active_level_latched = false;
#else
    return false;
#endif
  }
  if (!active) *active_level_latched = false;

  *armed = true;
  if (gpio_wakeup_enable(pin, wake_type) != ESP_OK) {
    *armed = false;
    (void)gpio_set_intr_type(pin, normal_type);
  }
  (void)gpio_intr_enable(pin);
  return *armed;
}

static void armWakeSources() {
#if defined(P_LORA_DIO_1)
  (void)armLevelWakeSafely((gpio_num_t)P_LORA_DIO_1, HIGH,
                           GPIO_INTR_HIGH_LEVEL, GPIO_INTR_POSEDGE,
                           &g_lora_gpio_wake_armed,
                           &g_lora_active_level_latched);
#endif
#if defined(PIN_USER_BTN)
  (void)armLevelWakeSafely((gpio_num_t)PIN_USER_BTN, LOW,
                           GPIO_INTR_LOW_LEVEL, GPIO_INTR_ANYEDGE,
                           &g_button_gpio_wake_armed,
                           &g_button_active_level_latched);
#endif
}
#endif

static void IRAM_ATTR notifyMainLoopFromISR() {
  if (g_main_loop_task) {
    BaseType_t high_priority_woken = pdFALSE;
    vTaskNotifyGiveFromISR(g_main_loop_task, &high_priority_woken);
    if (high_priority_woken) portYIELD_FROM_ISR(high_priority_woken);
  }
}

// RadioLib calls this weak hook from its DIO1 ISR. The strong implementation
// makes packet RX/TX completion wake the blocked repeater loop immediately.
extern "C" void IRAM_ATTR meshcore_on_lora_dio1_irq(void) {
#if defined(CONFIG_IDF_TARGET_ESP32S3) && defined(P_LORA_DIO_1)
  g_lora_active_level_latched = true;
  if (g_lora_gpio_wake_armed) {
    g_lora_gpio_wake_armed = false;
    disableGPIOInterruptFromISR((gpio_num_t)P_LORA_DIO_1);
  }
#endif
  notifyMainLoopFromISR();
}

#if defined(PIN_USER_BTN)
static void IRAM_ATTR onUserButtonEdge() {
#if defined(CONFIG_IDF_TARGET_ESP32S3)
  g_button_active_level_latched = true;
  if (g_button_gpio_wake_armed) {
    g_button_gpio_wake_armed = false;
    disableGPIOInterruptFromISR((gpio_num_t)PIN_USER_BTN);
  }
#endif
  ++g_button_irq_count;
  notifyMainLoopFromISR();
}
#endif

namespace repeater_low_power {

void disableUSBSerialJTAG() {
#if defined(CONFIG_IDF_TARGET_ESP32S3) && defined(DISABLE_SERIAL_CONSOLE)
  CLEAR_PERI_REG_MASK(USB_SERIAL_JTAG_CONF0_REG, USB_SERIAL_JTAG_DP_PULLUP);
  CLEAR_PERI_REG_MASK(USB_SERIAL_JTAG_CONF0_REG, USB_SERIAL_JTAG_USB_PAD_ENABLE);
  SET_PERI_REG_MASK(RTC_CNTL_USB_CONF_REG, RTC_CNTL_USB_PAD_ENABLE_OVERRIDE);
  CLEAR_PERI_REG_MASK(RTC_CNTL_USB_CONF_REG, RTC_CNTL_USB_PAD_ENABLE);
#endif
}

void beginEventDrivenLoop() {
#if defined(CONFIG_PM_ENABLE)
  // SDKCONFIG enables the PM subsystem; this call supplies the runtime policy
  // and is what actually permits automatic light sleep.
  #if defined(CONFIG_IDF_TARGET_ESP32S3)
    esp_pm_config_esp32s3_t pm_config = {};
  #elif defined(CONFIG_IDF_TARGET_ESP32S2)
    esp_pm_config_esp32s2_t pm_config = {};
  #elif defined(CONFIG_IDF_TARGET_ESP32C3)
    esp_pm_config_esp32c3_t pm_config = {};
  #elif defined(CONFIG_IDF_TARGET_ESP32)
    esp_pm_config_esp32_t pm_config = {};
  #endif
  #if defined(CONFIG_IDF_TARGET_ESP32S3) || defined(CONFIG_IDF_TARGET_ESP32S2) || \
      defined(CONFIG_IDF_TARGET_ESP32C3) || defined(CONFIG_IDF_TARGET_ESP32)
    pm_config.max_freq_mhz = PM_MAX_FREQ_MHZ;
    pm_config.min_freq_mhz = PM_MIN_FREQ_MHZ;
    pm_config.light_sleep_enable = true;
    (void)esp_pm_configure(&pm_config);
  #endif
#endif

  g_main_loop_task = xTaskGetCurrentTaskHandle();

#if defined(P_LORA_DIO_1)
  // CONFIG_PM_SLP_DISABLE_GPIO isolates ordinary pads during automatic light
  // sleep; these event inputs must stay connected to the wake logic.
  gpio_sleep_sel_dis((gpio_num_t)P_LORA_DIO_1);
#endif
#if defined(PIN_USER_BTN)
  pinMode(PIN_USER_BTN, INPUT_PULLUP);
  gpio_sleep_sel_dis((gpio_num_t)PIN_USER_BTN);
  attachInterrupt(digitalPinToInterrupt(PIN_USER_BTN), onUserButtonEdge, CHANGE);
#endif
#if defined(CONFIG_IDF_TARGET_ESP32S3)
  if (esp_sleep_enable_gpio_wakeup() == ESP_OK) {
    armWakeSources();
  }
#endif
}

void waitForEvent(uint32_t wait_ms) {
  if (wait_ms == 0) wait_ms = 1;

#if defined(PIN_USER_BTN)
  // Keep polling briefly around an edge so the simple display UI observes both
  // press and release without sacrificing the long steady-state idle timeout.
  static uint32_t seen_button_irq_count = 0;
  static uint32_t button_fast_poll_until = 0;
  const uint32_t button_irq_count = g_button_irq_count;
  if (button_irq_count != seen_button_irq_count) {
    seen_button_irq_count = button_irq_count;
    button_fast_poll_until = millis() + 350U;
  }
  const bool button_fast_poll =
      digitalRead(PIN_USER_BTN) == LOW ||
      (button_fast_poll_until != 0 &&
       (int32_t)(millis() - button_fast_poll_until) < 0);
  if (button_fast_poll && wait_ms > EVENT_LOOP_BUTTON_MAX_MS) {
    wait_ms = EVENT_LOOP_BUTTON_MAX_MS;
  }
#endif

#if defined(CONFIG_IDF_TARGET_ESP32S3)
  // RadioLib/UI have consumed the previous IRQ by now. Re-arm only immediately
  // before blocking so an event during tickless sleep cannot be lost.
  armWakeSources();
#endif

  const TickType_t ticks = pdMS_TO_TICKS(wait_ms);
  ulTaskNotifyTake(pdTRUE, ticks ? ticks : 1);
}

}  // namespace repeater_low_power

#endif  // EVENT_DRIVEN_LOOP && ESP32 && ESP_PLATFORM
