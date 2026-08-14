#pragma once

#include <stdint.h>

namespace repeater_low_power {

// Disconnect the ESP32-S3 USB Serial/JTAG peripheral when this build has no
// local console. Call before board/Serial initialisation.
void disableUSBSerialJTAG();

// Enable DFS + automatic light sleep and register LoRa/button wake sources for
// the Arduino loop task.
void beginEventDrivenLoop();

// Block the loop task until a LoRa/button event or the supplied deadline.
void waitForEvent(uint32_t wait_ms);

}  // namespace repeater_low_power
