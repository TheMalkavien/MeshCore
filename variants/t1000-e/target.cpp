#include <Arduino.h>
#include "t1000e_sensors.h"
#include "target.h"
#include <helpers/sensors/MicroNMEALocationProvider.h>

T1000eBoard board;

RADIO_CLASS radio = new Module(P_LORA_NSS, P_LORA_DIO_1, P_LORA_RESET, P_LORA_BUSY, SPI);

WRAPPER_CLASS radio_driver(radio, board);

VolatileRTCClock rtc_clock;
MicroNMEALocationProvider nmea = MicroNMEALocationProvider(Serial1, &rtc_clock);
T1000SensorManager sensors = T1000SensorManager(nmea);

static uint32_t gps_powered_at = 0;
static uint32_t gps_bytes_seen = 0;
static bool gps_uart_started = false;

#ifndef T1000_GPS_BAUD_FALLBACK
  #define T1000_GPS_BAUD_FALLBACK 0
#endif

#ifndef T1000_GPS_BAUD_FALLBACK_FIRST_MS
  #define T1000_GPS_BAUD_FALLBACK_FIRST_MS 15000UL
#endif

#ifndef T1000_GPS_BAUD_FALLBACK_SECOND_MS
  #define T1000_GPS_BAUD_FALLBACK_SECOND_MS 20000UL
#endif

#if T1000_GPS_BAUD_FALLBACK == 1
static uint8_t gps_baud_phase = 0; // 0=115200, 1=9600 fallback
#endif

static void gps_uart_begin(uint32_t baud) {
#if defined(NRF52_PLATFORM) && defined(GPS_RX_PIN) && defined(GPS_TX_PIN)
  ((Uart *)&Serial1)->setPins(GPS_RX_PIN, GPS_TX_PIN);
#endif
  if (gps_uart_started) {
    Serial1.end();
    delay(10);
  }
  Serial1.begin(baud);
  gps_uart_started = true;
}

#ifdef DISPLAY_CLASS
  NullDisplayDriver display;
#endif

#ifndef LORA_CR
  #define LORA_CR      5
#endif

#ifdef RF_SWITCH_TABLE
static const uint32_t rfswitch_dios[Module::RFSWITCH_MAX_PINS] = {
  RADIOLIB_LR11X0_DIO5,
  RADIOLIB_LR11X0_DIO6,
  RADIOLIB_LR11X0_DIO7,
  RADIOLIB_LR11X0_DIO8, 
  RADIOLIB_NC
};

static const Module::RfSwitchMode_t rfswitch_table[] = {
  // mode                 DIO5  DIO6  DIO7  DIO8
  { LR11x0::MODE_STBY,   {LOW,  LOW,  LOW,  LOW  }},  
  { LR11x0::MODE_RX,     {HIGH, LOW,  LOW,  HIGH }},
  { LR11x0::MODE_TX,     {HIGH, HIGH, LOW,  HIGH }},
  { LR11x0::MODE_TX_HP,  {LOW,  HIGH, LOW,  HIGH }},
  { LR11x0::MODE_TX_HF,  {LOW,  LOW,  LOW,  LOW  }}, 
  { LR11x0::MODE_GNSS,   {LOW,  LOW,  HIGH, LOW  }},
  { LR11x0::MODE_WIFI,   {LOW,  LOW,  LOW,  LOW  }},  
  END_OF_MODE_TABLE,
};
#endif

bool radio_init() {
  //rtc_clock.begin(Wire);
  
#ifdef LR11X0_DIO3_TCXO_VOLTAGE
  float tcxo = LR11X0_DIO3_TCXO_VOLTAGE;
#else
  float tcxo = 1.6f;
#endif

  SPI.setPins(P_LORA_MISO, P_LORA_SCLK, P_LORA_MOSI);
  SPI.begin();
  int status = radio.begin(LORA_FREQ, LORA_BW, LORA_SF, LORA_CR, RADIOLIB_LR11X0_LORA_SYNC_WORD_PRIVATE, LORA_TX_POWER, 16, tcxo);
  if (status != RADIOLIB_ERR_NONE) {
    Serial.print("ERROR: radio init failed: ");
    Serial.println(status);
    return false;  // fail
  }
  
  radio.setCRC(2);
  radio.explicitHeader();

#ifdef RF_SWITCH_TABLE
  radio.setRfSwitchTable(rfswitch_dios, rfswitch_table);
#endif
#ifdef RX_BOOSTED_GAIN
  radio.setRxBoostedGainMode(RX_BOOSTED_GAIN);
#endif

  return true;  // success
}

uint32_t radio_get_rng_seed() {
  return radio.random(0x7FFFFFFF);
}

void radio_set_params(float freq, float bw, uint8_t sf, uint8_t cr) {
  radio.setFrequency(freq);
  radio.setSpreadingFactor(sf);
  radio.setBandwidth(bw);
  radio.setCodingRate(cr);
}

void radio_set_tx_power(int8_t dbm) {
  radio.setOutputPower(dbm);
}

mesh::LocalIdentity radio_new_identity() {
  RadioNoiseListener rng(radio);
  return mesh::LocalIdentity(&rng);  // create new random identity
}

void T1000SensorManager::start_gps() {
  if (gps_active) {
    return;
  }
  gps_active = true;
  gps_powered_at = millis();
  gps_bytes_seen = 0;
#if T1000_GPS_BAUD_FALLBACK == 1
  gps_baud_phase = 0;
#endif
  gps_uart_begin(115200);
  //_nmea->begin();
  // this init sequence should be better 
  // comes from seeed examples and deals with all gps pins
  pinMode(GPS_EN, OUTPUT);
  digitalWrite(GPS_EN, HIGH);
  delay(10);
  pinMode(GPS_VRTC_EN, OUTPUT);
  digitalWrite(GPS_VRTC_EN, HIGH);
  delay(10);
       
  pinMode(GPS_RESET, OUTPUT);
  digitalWrite(GPS_RESET, HIGH);
  delay(10);
  digitalWrite(GPS_RESET, LOW);
       
  pinMode(GPS_SLEEP_INT, OUTPUT);
  digitalWrite(GPS_SLEEP_INT, HIGH);
  pinMode(GPS_RTC_INT, OUTPUT);
  digitalWrite(GPS_RTC_INT, LOW);
  pinMode(GPS_RESETB, INPUT_PULLUP);
}

void T1000SensorManager::sleep_gps() {
  if (!gps_active) {
    return;
  }
  gps_active = false;
  digitalWrite(GPS_VRTC_EN, HIGH);
  digitalWrite(GPS_EN, LOW);
  digitalWrite(GPS_RESET, HIGH);
  digitalWrite(GPS_SLEEP_INT, HIGH);
  digitalWrite(GPS_RTC_INT, LOW);
  pinMode(GPS_RESETB, OUTPUT);
  digitalWrite(GPS_RESETB, LOW);
  //_nmea->stop();
}

void T1000SensorManager::stop_gps() {
  if (!gps_active) {
    return;
  }
  gps_active = false;
  digitalWrite(GPS_VRTC_EN, LOW);
  digitalWrite(GPS_EN, LOW);
  digitalWrite(GPS_RESET, HIGH);
  digitalWrite(GPS_SLEEP_INT, HIGH);
  digitalWrite(GPS_RTC_INT, LOW);
  pinMode(GPS_RESETB, OUTPUT);
  digitalWrite(GPS_RESETB, LOW);
  //_nmea->stop();
}


bool T1000SensorManager::begin() {
  // init GPS
  gps_uart_begin(115200);
  return true;
}

bool T1000SensorManager::querySensors(uint8_t requester_permissions, CayenneLPP& telemetry) {
  if (requester_permissions & TELEM_PERM_LOCATION) {   // does requester have permission?
    telemetry.addGPS(TELEM_CHANNEL_SELF, node_lat, node_lon, node_altitude);
  }
  if (requester_permissions & TELEM_PERM_ENVIRONMENT) {
    // Firmware reports light as a 0-100 % scale, but expose it via Luminosity so app labels it "Luminosity".
    telemetry.addLuminosity(TELEM_CHANNEL_SELF, t1000e_get_light());
    telemetry.addTemperature(TELEM_CHANNEL_SELF, t1000e_get_temperature());
  }
  return true;
}

void T1000SensorManager::loop() {
  static long next_gps_update = 0;

  int avail = Serial1.available();
  if (gps_active && avail > 0) {
    gps_bytes_seen += (uint32_t)avail;
  }

  _nmea->loop();

  #if T1000_GPS_BAUD_FALLBACK == 1
  // Optional fallback for units configured with non-default GNSS UART baud.
  // Disabled by default because it resets GNSS during startup and can increase TTFF.
  if (gps_active && gps_bytes_seen == 0) {
    uint32_t elapsed = (uint32_t)(millis() - gps_powered_at);
    if (gps_baud_phase == 0 && elapsed > T1000_GPS_BAUD_FALLBACK_FIRST_MS) {
      gps_baud_phase = 1;
      gps_powered_at = millis();
      gps_uart_begin(9600);
      digitalWrite(GPS_RESET, HIGH);
      delay(10);
      digitalWrite(GPS_RESET, LOW);
    } else if (gps_baud_phase == 1 && elapsed > T1000_GPS_BAUD_FALLBACK_SECOND_MS) {
      gps_baud_phase = 0;
      gps_powered_at = millis();
      gps_uart_begin(115200);
      digitalWrite(GPS_RESET, HIGH);
      delay(10);
      digitalWrite(GPS_RESET, LOW);
    }
  }
  #endif

  if (millis() > next_gps_update) {
    if (gps_active && _nmea->isValid()) {
      node_lat = ((double)_nmea->getLatitude())/1000000.;
      node_lon = ((double)_nmea->getLongitude())/1000000.;
      node_altitude = ((double)_nmea->getAltitude()) / 1000.0;
      //Serial.printf("lat %f lon %f\r\n", _lat, _lon);
    }
    next_gps_update = millis() + 1000;
  }
}

int T1000SensorManager::getNumSettings() const { return 1; }  // just one supported: "gps" (power switch)

const char* T1000SensorManager::getSettingName(int i) const {
  return i == 0 ? "gps" : NULL;
}
const char* T1000SensorManager::getSettingValue(int i) const {
  if (i == 0) {
    return gps_active ? "1" : "0";
  }
  return NULL;
}
bool T1000SensorManager::setSettingValue(const char* name, const char* value) {
  if (strcmp(name, "gps") == 0) {
    if (strcmp(value, "0") == 0) {
      sleep_gps(); // sleep for faster fix !
    } else {
      start_gps();
    }
    return true;
  }
  return false;  // not supported
}
