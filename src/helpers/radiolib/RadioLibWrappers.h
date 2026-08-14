#pragma once

#include <Mesh.h>
#include <RadioLib.h>
#include "RXPowerSaving.h"

#ifdef USE_CC310_HW_CRYPTO
#include <Adafruit_nRFCrypto.h>
#endif
struct PacketMillis {
  uint32_t preambleMillis;  // preamble-detect -> header-valid deadline
  uint32_t payloadMillis;   // header-valid   -> rx-done deadline
};

class RadioLibWrapper : public mesh::Radio {
protected:
  PhysicalLayer* _radio;
  mesh::MainBoard* _board;
  uint32_t n_recv, n_sent, n_recv_errors;
  int16_t _noise_floor, _threshold;
  float _last_rssi, _last_snr;
  bool _cad_enabled;
  uint16_t _num_floor_samples;
  int32_t _floor_sample_sum;
  uint8_t _preamble_sf;

  bool _rx_ps_enabled;
  bool _rx_ps_armed;
  bool _rx_hold_continuous;
  uint32_t _rx_ps_rx_us;
  uint32_t _rx_ps_sleep_us;

  // A healthy SX1262 duty cycle produces BUSY transitions. The watchdog first
  // performs a soft re-arm, then a hardware radio reset if the waveform stays
  // stuck or receive mode repeatedly fails to start.
  bool _wd_last_busy;
  uint8_t _wd_stage;
  uint8_t _wd_strikes;
  uint8_t _startrx_fails;
  unsigned long _wd_last_transition;
  unsigned long _wd_stuck_thresh;
  unsigned long _wd_observe_until;
  uint32_t _wd_observe_ms;
  uint32_t n_wd_soft, n_wd_hard;

  // Runtime values reapplied after a watchdog hard reset.
  float _cur_freq, _cur_bw;
  uint8_t _cur_sf, _cur_cr;
  int8_t _cur_dbm;
  bool _params_valid, _dbm_valid;

  // Duty-cycled RX cannot provide a trustworthy instantaneous noise floor
  // during its sleep windows, so calibration briefly switches to continuous RX.
  bool _nf_calib_active;
  unsigned long _nf_last_calib;
  unsigned long _nf_calib_deadline;
  unsigned long _nf_sample_from;

  void idle();
  void startRecv();
  void rxPsWatchdogCheck();
  void noiseFloorCalibCheck();
  void endNoiseFloorCalib(unsigned long now);
  bool needsNoiseFloor() const { return _threshold != 0; }
  void prepareForRadioConfig();
  void cacheParams(float freq, float bw, uint8_t sf, uint8_t cr) {
    _cur_freq = freq;
    _cur_bw = bw;
    _cur_sf = sf;
    _cur_cr = cr;
    _params_valid = true;
  }
  virtual int startReceiveMode();
  virtual void stopReceiveDutyCycle();
  virtual bool isPacketReady();
  virtual bool isChipBusy() { return false; }
  virtual bool radioDeepInit() { return false; }
  float packetScoreInt(float snr, int sf, int packet_len);
  virtual bool isReceivingPacket() =0;
  virtual void doResetAGC();

public:
  RadioLibWrapper(PhysicalLayer& radio, mesh::MainBoard& board)
      : _radio(&radio), _board(&board), _preamble_sf(0), _rx_ps_enabled(false), _rx_ps_armed(false),
        _rx_hold_continuous(false), _rx_ps_rx_us(meshcore::rxps::DEFAULT_RX_US),
        _rx_ps_sleep_us(meshcore::rxps::DEFAULT_SLEEP_US), _wd_last_busy(false), _wd_stage(0),
        _wd_strikes(0), _startrx_fails(0), _wd_last_transition(0), _wd_stuck_thresh(0),
        _wd_observe_until(0), _wd_observe_ms(0), _cur_freq(0), _cur_bw(0), _cur_sf(0), _cur_cr(0),
        _cur_dbm(0), _params_valid(false), _dbm_valid(false), _nf_calib_active(false),
        _nf_last_calib(0), _nf_calib_deadline(0), _nf_sample_from(0) {
    n_recv = n_sent = n_recv_errors = n_wd_soft = n_wd_hard = 0;
    _last_rssi = _last_snr = 0;
  }

  void begin() override;
  virtual void powerOff() { _radio->sleep(); }
  int recvRaw(uint8_t* bytes, int sz) override;
  void onReceiveProcessed() override;
  uint32_t getEstAirtimeFor(int len_bytes) override;
  bool startSendRaw(const uint8_t* bytes, int len) override;
  bool isSendComplete() override;
  void onSendFinished() override;
  bool isInRecvMode() const override;
  bool supportsRxPowerSaving() const override { return false; }
  bool setRxPowerSaving(bool enabled, uint32_t rx_us, uint32_t sleep_us) override;
  bool prepareForSleep();
  bool isChannelActive();

  bool isReceiving() override {
    if (isReceivingPacket()) return true;

    return isChannelActive();
  }

  virtual void setParams(float freq, float bw, uint8_t sf, uint8_t cr) = 0;
  uint32_t getRngSeed();
  void setTxPower(int8_t dbm);

  virtual float getCurrentRSSI() =0;
  virtual uint8_t getSpreadingFactor() const { return LORA_SF; }
  static uint16_t preambleLengthForSF(uint8_t sf) { return sf <= 8 ? 32 : 16; }
  void updatePreamble(uint8_t sf) { _preamble_sf = sf; _radio->setPreambleLength(preambleLengthForSF(sf)); }
  PacketMillis calcMaxPacketMillis(uint8_t sf, float bw, uint8_t cr, uint8_t preambleSymbols);
  virtual int16_t performChannelScan();

  int getNoiseFloor() const override { return _noise_floor; }
  void triggerNoiseFloorCalibrate(int threshold) override;
  void setCADEnabled(bool enable) override { _cad_enabled = enable; }
  void resetAGC() override;

  void loop() override;

  uint32_t getPacketsRecv() const { return n_recv; }
  uint32_t getPacketsRecvErrors() const { return n_recv_errors; }
  uint32_t getPacketsSent() const { return n_sent; }
  uint32_t getRxPsWatchdogSoftCount() const override { return n_wd_soft; }
  uint32_t getRxPsWatchdogHardCount() const override { return n_wd_hard; }
  bool isRxPowerSavingEnabled() const override { return _rx_ps_enabled; }
  bool isRxPowerSavingArmed() const override { return _rx_ps_armed; }
  bool isRxPowerSavingMaintenanceActive() const override {
    return _nf_calib_active || _wd_observe_until != 0;
  }
  bool isWatchdogObserving() const { return _wd_observe_until != 0; }
  bool isCalibratingNoiseFloor() const { return _nf_calib_active; }
  bool isNoiseFloorSampling() const;
  void resetStats() { n_recv = n_sent = n_recv_errors = n_wd_soft = n_wd_hard = 0; }

  // final: these must return the metadata cached by recvRaw() before the RX
  // re-arm. A subclass reading the chip live would report the wrong packet's
  // SNR/RSSI whenever RX duty-cycle power saving is armed.
  float getLastRSSI() const override final;
  float getLastSNR() const override final;

  float packetScore(float snr, int packet_len) override { return packetScoreInt(snr, 10, packet_len); }  // assume sf=10

  virtual bool setRxBoostedGainMode(bool) { return false; }
  virtual bool getRxBoostedGainMode() const { return false; }
  
  virtual bool configSideDetectors(const uint8_t sideDetSFs[], uint8_t num, float bw) { return false; }
};

/**
 * \brief  an RNG impl using the noise from the LoRa radio as entropy.
 *         NOTE: this is VERY SLOW!  Use only for things like creating new LocalIdentity
*/
class RadioNoiseListener : public mesh::RNG {
  PhysicalLayer* _radio;
public:
  RadioNoiseListener(PhysicalLayer& radio): _radio(&radio) { }

  void random(uint8_t* dest, size_t sz) override {
#ifdef USE_CC310_HW_CRYPTO
    // CC310 TRNG is higher quality and environment-independent vs radio RSSI noise.
    nRFCrypto.Random.generate(dest, (uint16_t)sz);
#else
    for (int i = 0; i < sz; i++) {
      dest[i] = _radio->randomByte() ^ (::random(0, 256) & 0xFF);
    }
#endif
  }
};
