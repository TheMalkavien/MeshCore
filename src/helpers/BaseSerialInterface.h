#pragma once

#include <Arduino.h>

#define MAX_FRAME_SIZE  176   // +4 for transport codes (region scoping)

// USB serial writes do not need a fixed-size transport queue. Keep command
// reception and buffered BLE/WiFi/Ethernet transports at MAX_FRAME_SIZE, but
// allow a complete raw RF packet (255 bytes) plus RX_LOG type/SNR/RSSI.
#define MAX_SERIAL_TX_FRAME_SIZE  258

class BaseSerialInterface {
protected:
  BaseSerialInterface() { }

public:
  virtual void enable() = 0;
  virtual void disable() = 0;
  virtual bool isEnabled() const = 0;

  virtual bool isConnected() const = 0;
  virtual void loop() {};

  virtual bool isWriteBusy() const = 0;
  virtual size_t writeFrame(const uint8_t src[], size_t len) = 0;
  virtual size_t checkRecvFrame(uint8_t dest[]) = 0;
};
