#pragma once

#include "CustomSX1262.h"
#include "RadioLibWrappers.h"
#include "SX126xReset.h"

#ifndef USE_SX1262
#define USE_SX1262
#endif

class CustomSX1262Wrapper : public RadioLibWrapper {
  bool _rx_boosted_cache;
  bool _rx_boosted_valid;

public:
  CustomSX1262Wrapper(CustomSX1262& radio, mesh::MainBoard& board)
      : RadioLibWrapper(radio, board), _rx_boosted_cache(false), _rx_boosted_valid(false) { }

  void setParams(float freq, float bw, uint8_t sf, uint8_t cr) override {
    prepareForRadioConfig();
    cacheParams(freq, bw, sf, cr);
    ((CustomSX1262 *)_radio)->setFrequency(freq);
    ((CustomSX1262 *)_radio)->setSpreadingFactor(sf);
    ((CustomSX1262 *)_radio)->setBandwidth(bw);
    ((CustomSX1262 *)_radio)->setCodingRate(cr);
    updatePreamble(sf);
    PacketMillis pm = calcMaxPacketMillis(sf, bw, cr, preambleLengthForSF(sf));
    ((CustomSX1262 *)_radio)->setPreambleMillis(pm.preambleMillis);
    ((CustomSX1262 *)_radio)->setMaxPayloadMillis(pm.payloadMillis);
  }

  bool isReceivingPacket() override { 
    return ((CustomSX1262 *)_radio)->isReceiving();
  }
  bool isChipBusy() override {
    return ((CustomSX1262 *)_radio)->isChipBusy();
  }
  float getCurrentRSSI() override {
    return ((CustomSX1262 *)_radio)->getRSSI(false);
  }

  float packetScore(float snr, int packet_len) override {
    int sf = ((CustomSX1262 *)_radio)->spreadingFactor;
    return packetScoreInt(snr, sf, packet_len);
  }
  uint8_t getSpreadingFactor() const override { return ((CustomSX1262 *)_radio)->spreadingFactor; }
  virtual void powerOff() override {
    if (_rx_ps_armed) stopReceiveDutyCycle();
    ((CustomSX1262 *)_radio)->sleep(false);
  }

  bool supportsRxPowerSaving() const override {
#if defined(WITH_SX1262_RX_POWER_SAVING)
    return true;
#else
    return false;
#endif
  }

protected:
  int startReceiveMode() override {
    if (_rx_ps_armed) stopReceiveDutyCycle();
    if (!_rx_ps_enabled || _nf_calib_active) return _radio->startReceive();

    const RadioLibIrqFlags_t irq_flags = RADIOLIB_IRQ_RX_DEFAULT_FLAGS;
    const RadioLibIrqFlags_t irq_mask =
        (1UL << RADIOLIB_IRQ_RX_DONE) |
        (1UL << RADIOLIB_IRQ_TIMEOUT) |
        (1UL << RADIOLIB_IRQ_CRC_ERR) |
        (1UL << RADIOLIB_IRQ_HEADER_ERR);

    int err = ((CustomSX1262 *)_radio)->startReceiveDutyCycle(
        _rx_ps_rx_us, _rx_ps_sleep_us, irq_flags, irq_mask);
    if (err == RADIOLIB_ERR_NONE) {
      _rx_ps_armed = true;
      return err;
    }

    MESH_DEBUG_PRINTLN("CustomSX1262Wrapper: startReceiveDutyCycle(%d), continuous RX fallback", err);
    _radio->standby();
    ((CustomSX1262 *)_radio)->stopRTC();
    _rx_ps_armed = false;
    return _radio->startReceive();
  }

  void stopReceiveDutyCycle() override {
    _radio->standby();
    ((CustomSX1262 *)_radio)->stopRTC();
    _rx_ps_armed = false;
  }

  bool radioDeepInit() override {
    if (!((CustomSX1262 *)_radio)->std_init()) return false;
    if (_rx_boosted_valid) {
      return ((CustomSX1262 *)_radio)->setRxBoostedGainModeRetained(_rx_boosted_cache) == RADIOLIB_ERR_NONE;
    }
    return true;
  }

public:
  void doResetAGC() override {
    prepareForRadioConfig();
    sx126xResetAGC((SX126x *)_radio, getRxBoostedGainMode());
  }

  bool setRxBoostedGainMode(bool en) override {
    prepareForRadioConfig();
    const bool ok = ((CustomSX1262 *)_radio)->setRxBoostedGainModeRetained(en) == RADIOLIB_ERR_NONE;
    if (ok) {
      _rx_boosted_cache = en;
      _rx_boosted_valid = true;
    }
    return ok;
  }
  bool getRxBoostedGainMode() const override {
    return _rx_boosted_valid ? _rx_boosted_cache : ((CustomSX1262 *)_radio)->getRxBoostedGainMode();
  }
};
