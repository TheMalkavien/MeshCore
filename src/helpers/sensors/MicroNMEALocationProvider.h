#pragma once

#include "LocationProvider.h"
#include <MicroNMEA.h>
#include <RTClib.h>
#include <helpers/RefCountedDigitalPin.h>
#if defined(ESP_PLATFORM)
    #include <driver/gpio.h>
    #include <esp_system.h>
#endif

#ifndef GPS_EN
    #ifdef PIN_GPS_EN
        #define GPS_EN PIN_GPS_EN
    #else
        #define GPS_EN (-1)
    #endif
#endif

#ifndef GPS_EN_ACTIVE
    #ifdef PIN_GPS_EN_ACTIVE
        #define GPS_EN_ACTIVE PIN_GPS_EN_ACTIVE
    #else
        #define GPS_EN_ACTIVE HIGH
    #endif
#endif

#ifndef GPS_RESET
    #ifdef PIN_GPS_RESET
        #define GPS_RESET PIN_GPS_RESET
    #else
        #define GPS_RESET (-1)
    #endif
#endif

#ifndef GPS_RESET_ACTIVE
    #ifdef PIN_GPS_RESET_ACTIVE
        #define GPS_RESET_ACTIVE PIN_GPS_RESET_ACTIVE
    #else
        #define GPS_RESET_ACTIVE LOW
    #endif
#endif

#ifndef PIN_GPS_BACKUP
    #define PIN_GPS_BACKUP (-1)
#endif

#ifndef PIN_GPS_BACKUP_ACTIVE
    #define PIN_GPS_BACKUP_ACTIVE HIGH
#endif

#ifndef GPS_SLEEP_PIN
    #ifdef PIN_GPS_SLEEP
        #define GPS_SLEEP_PIN PIN_GPS_SLEEP
    #else
        #define GPS_SLEEP_PIN (-1)
    #endif
#endif

#ifndef PIN_GPS_SLEEP_ACTIVE
    // Logic level that keeps GNSS awake on optional sleep/wakeup pin.
    #define PIN_GPS_SLEEP_ACTIVE HIGH
#endif

#ifndef GPS_USE_SLEEP_PIN_FOR_STOP
    // 1 => stop() uses GPS_SLEEP_PIN instead of cutting GPS_EN power.
    #define GPS_USE_SLEEP_PIN_FOR_STOP 0
#endif

#ifndef GPS_HOLD_CONTROL_PINS_IN_DEEP_SLEEP
    // Boards using a dedicated GNSS wake/sleep pin usually keep the main rail
    // powered for hot starts. Retain those control levels when the ESP enters
    // deep sleep; other boards keep their existing stop() behaviour.
    #define GPS_HOLD_CONTROL_PINS_IN_DEEP_SLEEP GPS_USE_SLEEP_PIN_FOR_STOP
#endif

class MicroNMEALocationProvider : public LocationProvider {
    char _nmeaBuffer[100];
    MicroNMEA nmea;
    mesh::RTCClock* _clock;
    Stream* _gps_serial;
    RefCountedDigitalPin* _peripher_power;
    int8_t _claims = 0;
    int _pin_reset;
    int _pin_en;
    int _pin_sleep;
    unsigned long next_check = 0;
    long time_valid = 0;
    unsigned long _last_time_sync = 0;
    static const unsigned long TIME_SYNC_INTERVAL = 1800000; // Re-sync every 30 minutes
    bool _started = false;
#if defined(ESP_PLATFORM) && GPS_HOLD_CONTROL_PINS_IN_DEEP_SLEEP
    bool _deep_sleep_holds_armed = false;
#endif

    bool usesSleepPinForStop() const {
#if GPS_USE_SLEEP_PIN_FOR_STOP
        return _pin_sleep != -1 && _peripher_power == NULL;
#else
        return false;
#endif
    }

    void configureControlPin(int pin, int level) {
        if (pin == -1) {
            return;
        }
        pinMode(pin, OUTPUT);
        digitalWrite(pin, level);
    }

    void configureControlState(bool awake) {
        configureControlPin(_pin_reset, !GPS_RESET_ACTIVE);
        configureControlPin(_pin_sleep, awake ? PIN_GPS_SLEEP_ACTIVE : !PIN_GPS_SLEEP_ACTIVE);

        if (_pin_en != -1) {
            const int en_level = (awake || usesSleepPinForStop())
                ? GPS_EN_ACTIVE : !GPS_EN_ACTIVE;
            configureControlPin(_pin_en, en_level);
        }

        configureControlPin(PIN_GPS_BACKUP, PIN_GPS_BACKUP_ACTIVE);
    }

    void configureInitialControlState() {
        // Preserve the legacy powered-off/reset-held boot state for providers
        // without a dedicated sleep pin. The hot-standby path must instead keep
        // reset released so an ESP deep-sleep cycle does not force a cold start.
        configureControlPin(_pin_reset,
            usesSleepPinForStop() ? !GPS_RESET_ACTIVE : GPS_RESET_ACTIVE);
        configureControlPin(_pin_sleep, !PIN_GPS_SLEEP_ACTIVE);
        configureControlPin(_pin_en,
            usesSleepPinForStop() ? GPS_EN_ACTIVE : !GPS_EN_ACTIVE);
        configureControlPin(PIN_GPS_BACKUP, PIN_GPS_BACKUP_ACTIVE);
    }

#if defined(ESP_PLATFORM) && GPS_HOLD_CONTROL_PINS_IN_DEEP_SLEEP
    void releaseControlPinHold(int pin) {
        if (pin != -1) {
            (void)gpio_hold_dis((gpio_num_t)pin);
        }
    }

    void releaseDeepSleepControlHolds(bool woke_from_deep_sleep = false) {
        releaseControlPinHold(_pin_en);
        releaseControlPinHold(_pin_reset);
        releaseControlPinHold(_pin_sleep);
        releaseControlPinHold(PIN_GPS_BACKUP);

        if (_deep_sleep_holds_armed || woke_from_deep_sleep) {
            gpio_deep_sleep_hold_dis();
            _deep_sleep_holds_armed = false;
        }
    }

    void armControlPinHold(int pin) {
        if (pin != -1) {
            (void)gpio_hold_en((gpio_num_t)pin);
        }
    }

    void armDeepSleepControlHolds() {
        armControlPinHold(_pin_en);
        armControlPinHold(_pin_reset);
        armControlPinHold(_pin_sleep);
        armControlPinHold(PIN_GPS_BACKUP);
        gpio_deep_sleep_hold_en();
        _deep_sleep_holds_armed = true;
    }
#endif

public :
    MicroNMEALocationProvider(Stream& ser, mesh::RTCClock* clock = NULL, int pin_reset = GPS_RESET, int pin_en = GPS_EN, RefCountedDigitalPin* peripher_power=NULL, int pin_sleep = GPS_SLEEP_PIN) :
    nmea(_nmeaBuffer, sizeof(_nmeaBuffer)), _clock(clock), _gps_serial(&ser), _peripher_power(peripher_power), _pin_reset(pin_reset), _pin_en(pin_en), _pin_sleep(pin_sleep) {
#if defined(ESP_PLATFORM) && GPS_HOLD_CONTROL_PINS_IN_DEEP_SLEEP
        const bool woke_from_deep_sleep = esp_reset_reason() == ESP_RST_DEEPSLEEP;
#endif

        // Configure the desired standby state before releasing a retained pad,
        // avoiding an EN/WAKEUP glitch after a deep-sleep reset.
        configureInitialControlState();

#if defined(ESP_PLATFORM) && GPS_HOLD_CONTROL_PINS_IN_DEEP_SLEEP
        releaseDeepSleepControlHolds(woke_from_deep_sleep);
#endif

#if defined(ESP_PLATFORM) && defined(CONFIG_PM_SLP_DISABLE_GPIO)
        // Keep GPS control pins under normal GPIO control during auto light sleep.
        if (_pin_en != -1) {
            gpio_sleep_sel_dis((gpio_num_t)_pin_en);
        }
        if (_pin_reset != -1) {
            gpio_sleep_sel_dis((gpio_num_t)_pin_reset);
        }
        if (_pin_sleep != -1) {
            gpio_sleep_sel_dis((gpio_num_t)_pin_sleep);
        }
        if (PIN_GPS_BACKUP != -1) {
            gpio_sleep_sel_dis((gpio_num_t)PIN_GPS_BACKUP);
        }
#endif
    }

    void claim() {
        _claims++;
        if (_peripher_power) _peripher_power->claim();
    }

    void release() {
        if (_claims == 0) return; // avoid negative _claims
        _claims--;
        if (_peripher_power) _peripher_power->release();
    }

    void begin() override {
        if (_started) {
            return;
        }
        claim();
        configureControlState(true);
#if defined(ESP_PLATFORM) && GPS_HOLD_CONTROL_PINS_IN_DEEP_SLEEP
        releaseDeepSleepControlHolds();
#endif
        _started = true;
    }

    void reset() override {
        if (_pin_reset != -1) {
            digitalWrite(_pin_reset, GPS_RESET_ACTIVE);
            delay(10);
            digitalWrite(_pin_reset, !GPS_RESET_ACTIVE);
        }
    }

    void stop() override {
        if (!_started) {
            return;
        }

        configureControlState(false);
        release();
        _started = false;
    }

    void prepareForDeepSleep() override {
        stop();

#if defined(ESP_PLATFORM) && GPS_HOLD_CONTROL_PINS_IN_DEEP_SLEEP
        if (usesSleepPinForStop()) {
            // stop() is idempotent and may have returned because a one-shot fix
            // was already sleeping. Reassert the intended standby levels before
            // latching them for the whole MCU deep-sleep interval.
            configureControlState(false);
            armDeepSleepControlHolds();
        }
#endif
    }

    bool isEnabled() override {
        if (usesSleepPinForStop()) {
            return _started;
        }

        // directly read the enable pin if present as gps can be
        // activated/deactivated outside of here ...
        if (_pin_en != -1) {
            return digitalRead(_pin_en) == GPS_EN_ACTIVE;
        }
        // If there is no enable pin, use internal state when power is ref-counted.
        if (_peripher_power != NULL) {
            return _started;
        }
        return true; // no enable pin and no power gate: assume always active
    }

    void syncTime() override { nmea.clear(); LocationProvider::syncTime(); }
    long getLatitude() override { return nmea.getLatitude(); }
    long getLongitude() override { return nmea.getLongitude(); }
    long getAltitude() override { 
        long alt = 0;
        nmea.getAltitude(alt);
        return alt;
    }
    long satellitesCount() override { return nmea.getNumSatellites(); }
    bool isValid() override { return nmea.isValid(); }

    long getTimestamp() override {
        DateTime dt(nmea.getYear(), nmea.getMonth(),nmea.getDay(),nmea.getHour(),nmea.getMinute(),nmea.getSecond());
        return dt.unixtime();
    }

    // A fix can be reported valid from GGA before the RMC date field has been
    // parsed, and some receivers emit a rollover-corrupted year. Pushing either
    // into the RTC is worse than keeping the current time. This deliberately
    // does not gate isValid(): the position fix must not wait on a lagging RMC.
    bool hasSaneTimestamp() {
        const uint16_t year = nmea.getYear();
        return year >= 2024 && year < 2100;
    }

    void sendSentence(const char *sentence) override {
        nmea.sendSentence(*_gps_serial, sentence);
    }

    void loop() override {

        while (_gps_serial->available()) {
            char c = _gps_serial->read();
            #ifdef GPS_NMEA_DEBUG
            Serial.print(c);
            #endif
            nmea.process(c);
        }

        if (!isValid()) time_valid = 0;

        if ((long)(millis() - next_check) > 0) {
            next_check = millis() + 1000;
            // Re-enable time sync periodically when GPS has valid fix
            if (!_time_sync_needed && _clock != NULL && (millis() - _last_time_sync) > TIME_SYNC_INTERVAL) {
                _time_sync_needed = true;
            }
            if (_time_sync_needed && time_valid > 2 && hasSaneTimestamp()) {
                if (_clock != NULL) {
                    _clock->setCurrentTime(getTimestamp());
                    _time_sync_needed = false;
                    _last_time_sync = millis();
                }
            }
            if (isValid()) {
                time_valid ++;
            }
        }
    }
};
