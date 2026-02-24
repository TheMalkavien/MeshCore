#pragma once

#include "LocationProvider.h"
#include <MicroNMEA.h>
#include <RTClib.h>
#include <helpers/RefCountedDigitalPin.h>
#if defined(ESP_PLATFORM) && defined(CONFIG_PM_SLP_DISABLE_GPIO)
    #include <driver/gpio.h>
#endif

#ifndef GPS_EN
    #ifdef PIN_GPS_EN
        #define GPS_EN PIN_GPS_EN
    #else
        #define GPS_EN (-1)
    #endif
#endif

#ifndef PIN_GPS_EN_ACTIVE
    #define PIN_GPS_EN_ACTIVE HIGH
#endif

#ifndef GPS_RESET
    #ifdef PIN_GPS_RESET
        #define GPS_RESET PIN_GPS_RESET
    #else
        #define GPS_RESET (-1)
    #endif
#endif

#ifndef GPS_RESET_FORCE
    #ifdef PIN_GPS_RESET_ACTIVE
        #define GPS_RESET_FORCE PIN_GPS_RESET_ACTIVE
    #else
        #define GPS_RESET_FORCE LOW
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

class MicroNMEALocationProvider : public LocationProvider {
    char _nmeaBuffer[100];
    MicroNMEA nmea;
    mesh::RTCClock* _clock;
    Stream* _gps_serial;
    RefCountedDigitalPin* _peripher_power;
    int _pin_reset;
    int _pin_en;
    int _pin_sleep;
    long next_check = 0;
    long time_valid = 0;
    bool _started = false;

public :
    MicroNMEALocationProvider(Stream& ser, mesh::RTCClock* clock = NULL, int pin_reset = GPS_RESET, int pin_en = GPS_EN, RefCountedDigitalPin* peripher_power=NULL, int pin_sleep = GPS_SLEEP_PIN) :
    _gps_serial(&ser), nmea(_nmeaBuffer, sizeof(_nmeaBuffer)), _pin_reset(pin_reset), _pin_en(pin_en), _pin_sleep(pin_sleep), _clock(clock), _peripher_power(peripher_power) {
        if (_pin_reset != -1) {
            pinMode(_pin_reset, OUTPUT);
            digitalWrite(_pin_reset, GPS_RESET_FORCE);
        }
        if (_pin_en != -1) {
            pinMode(_pin_en, OUTPUT);
            digitalWrite(_pin_en, LOW);
        }
        if (_pin_sleep != -1) {
            pinMode(_pin_sleep, OUTPUT);
            digitalWrite(_pin_sleep, !PIN_GPS_SLEEP_ACTIVE);
        }

        if (PIN_GPS_BACKUP != -1) {
            pinMode(PIN_GPS_BACKUP, OUTPUT);
            digitalWrite(PIN_GPS_BACKUP, PIN_GPS_BACKUP_ACTIVE);
        }

#if GPS_USE_SLEEP_PIN_FOR_STOP
        // Keep GNSS main rail on when sleep pin mode is available.
        if (_pin_sleep != -1 && _peripher_power == NULL && _pin_en != -1) {
            digitalWrite(_pin_en, PIN_GPS_EN_ACTIVE);
        }
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

    void begin() override {
        if (_started) {
            return;
        }
        if (_peripher_power) _peripher_power->claim();
        if (_pin_en != -1) {
            digitalWrite(_pin_en, PIN_GPS_EN_ACTIVE);
        }
        if (_pin_sleep != -1) {
            digitalWrite(_pin_sleep, PIN_GPS_SLEEP_ACTIVE);
        }
        if (_pin_reset != -1) {
            digitalWrite(_pin_reset, !GPS_RESET_FORCE);
        }
        _started = true;
    }

    void reset() override {
        if (_pin_reset != -1) {
            digitalWrite(_pin_reset, GPS_RESET_FORCE);
            delay(10);
            digitalWrite(_pin_reset, !GPS_RESET_FORCE);
        }
    }

    void stop() override {
        if (!_started) {
            return;
        }

        bool use_sleep_pin_mode = false;
#if GPS_USE_SLEEP_PIN_FOR_STOP
        use_sleep_pin_mode = (_pin_sleep != -1 && _peripher_power == NULL);
#endif

        if (use_sleep_pin_mode) {
            digitalWrite(_pin_sleep, !PIN_GPS_SLEEP_ACTIVE);
        } else if (_pin_en != -1) {
            digitalWrite(_pin_en, !PIN_GPS_EN_ACTIVE);
        }
        if (_peripher_power) _peripher_power->release();
        _started = false;
    }

    bool isEnabled() override {
        bool use_sleep_pin_mode = false;
#if GPS_USE_SLEEP_PIN_FOR_STOP
        use_sleep_pin_mode = (_pin_sleep != -1 && _peripher_power == NULL);
#endif

        if (use_sleep_pin_mode) {
            return _started;
        }

        // directly read the enable pin if present as gps can be
        // activated/deactivated outside of here ...
        if (_pin_en != -1) {
            return digitalRead(_pin_en) == PIN_GPS_EN_ACTIVE;
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

        if (millis() > next_check) {
            next_check = millis() + 1000;
            if (_time_sync_needed && time_valid > 2) {
                if (_clock != NULL) {
                    _clock->setCurrentTime(getTimestamp());
                    _time_sync_needed = false;
                }
            }
            if (isValid()) {
                time_valid ++;
            }
        }
    }
};
