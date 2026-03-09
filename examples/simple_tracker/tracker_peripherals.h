#pragma once

#include "tracker_config.h"

struct TrackerEnvExtras {
  bool has_temperature = false;
  float temperature_c = 0.0f;
  bool has_light = false;
  uint32_t light_level = 0;
};

void trackerPeripheralsBegin();
void trackerPeripheralsLoop();
void trackerPeripheralsNotifyTx();
bool trackerPeripheralsConsumeImmediateCycleRequest();
bool trackerPeripheralsIsExternalPowered();
bool trackerPeripheralsPowerButtonPressed();
bool trackerPeripheralsTxLedActive();
void trackerPeripheralsPrepareForIdleSleep();
TrackerEnvExtras trackerReadEnvExtras();
void trackerBuildEnvExtrasJson(char* out, size_t out_len);
