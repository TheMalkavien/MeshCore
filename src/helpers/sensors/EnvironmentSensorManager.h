#pragma once

#include <Mesh.h>
#include <helpers/SensorManager.h>
#include <helpers/sensors/LocationProvider.h>

class EnvironmentSensorManager : public SensorManager {
protected:
  int next_available_channel = TELEM_CHANNEL_SELF + 1;

  bool AHTX0_initialized = false;
  bool BME280_initialized = false;
  bool BMP280_initialized = false;
  bool INA3221_initialized = false;
  bool INA219_initialized = false;
  bool INA260_initialized = false;
  bool INA226_initialized = false;
  bool SHTC3_initialized = false;
  bool LPS22HB_initialized = false;
  bool MLX90614_initialized = false;
  bool VL53L0X_initialized = false;
  bool SHT4X_initialized = false;
  bool BME680_initialized = false;
  bool BMP085_initialized = false;

  bool gps_detected = false;
  bool gps_active = false;
  uint32_t gps_update_interval_sec = 1;  // Default 1 second

  #if ENV_INCLUDE_GPS
  LocationProvider* _location;
  uint32_t next_gps_update_ms = 0;
  bool gps_fix_window_active = false;
  uint32_t gps_fix_window_deadline_ms = 0;
  void start_gps();
  void stop_gps();
  void initBasicGPS();
  void updateLocationFromFix();
  void beginGPSFixWindow();
  void endGPSFixWindow(bool fixed);
  bool isGPSFixWindowExpired(uint32_t now_ms) const;
  #if defined(ESP_PLATFORM) && defined(CONFIG_PM_ENABLE)
  bool gps_pm_lock_active = false;
  void setGPSPMLightSleep(bool enable);
  #endif
  #ifdef RAK_BOARD
  void rakGPSInit();
  bool gpsIsAwake(uint8_t ioPin);
  #endif
  #endif


public:
  #if ENV_INCLUDE_GPS
  EnvironmentSensorManager(LocationProvider &location): _location(&location){};
  LocationProvider* getLocationProvider() { return _location; }
  #else
  EnvironmentSensorManager(){};
  #endif
  bool begin() override;
  bool querySensors(uint8_t requester_permissions, CayenneLPP& telemetry) override;
  #if ENV_INCLUDE_GPS
  void loop() override;
  #endif
  int getNumSettings() const override;
  const char* getSettingName(int i) const override;
  const char* getSettingValue(int i) const override;
  bool setSettingValue(const char* name, const char* value) override;
};
