#pragma once

#define RADIOLIB_STATIC_ONLY 1

#include <RadioLib.h>
#include <helpers/AutoDiscoverRTCClock.h>
#include <helpers/radiolib/CustomSX1262Wrapper.h>
#include <helpers/radiolib/RadioLibWrappers.h>
#include <helpers/SensorManager.h>
#include <helpers/sensors/EnvironmentSensorManager.h>
#include <WaveshareBoard.h>

extern WaveshareBoard board;
extern WRAPPER_CLASS radio_driver;
extern AutoDiscoverRTCClock rtc_clock;
#ifdef MLK_SENSORS
extern EnvironmentSensorManager sensors;
#else
extern SensorManager sensors;
#endif

bool radio_init();
mesh::LocalIdentity radio_new_identity();

