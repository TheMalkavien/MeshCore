
#define RADIOLIB_STATIC_ONLY 1
#include "RadioLibWrappers.h"

#define STATE_IDLE       0
#define STATE_RX         1
#define STATE_TX_WAIT    3
#define STATE_TX_DONE    4
#define STATE_INT_READY 16

#define NUM_NOISE_FLOOR_SAMPLES  64
#define SAMPLING_THRESHOLD  14

#define NF_CALIB_INTERVAL_MS  60000UL
#define NF_CALIB_TIMEOUT_MS   5000UL
#define NF_CALIB_SETTLE_MS    20UL

static volatile uint8_t state = STATE_IDLE;
static RadioLibWrapper* g_radio_for_sleep = NULL;

// Optional board hook for platforms that want to wake the MCU on radio DIO1 IRQ
// while RadioLib owns the interrupt callback on that pin.
extern "C" void meshcore_on_lora_dio1_irq(void) __attribute__((weak));
extern "C" void meshcore_on_lora_dio1_irq(void) {
}
extern "C" bool meshcore_radio_irq_pending(void) __attribute__((weak));
extern "C" bool meshcore_radio_irq_pending(void) {
  return (state & STATE_INT_READY) != 0;
}
extern "C" bool meshcore_radio_prepare_for_sleep(void) __attribute__((weak));
extern "C" bool meshcore_radio_prepare_for_sleep(void) {
  return g_radio_for_sleep ? g_radio_for_sleep->prepareForSleep() : (state & STATE_INT_READY) != 0;
}

// this function is called when a complete packet
// is transmitted by the module
static
#if defined(ESP8266) || defined(ESP32)
  ICACHE_RAM_ATTR
#endif
void setFlag(void) {
  // we sent a packet, set the flag
  state |= STATE_INT_READY;
  meshcore_on_lora_dio1_irq();
}

void RadioLibWrapper::begin() {
  g_radio_for_sleep = this;
  _radio->setPacketReceivedAction(setFlag);  // this is also SentComplete interrupt
  _preamble_sf = getSpreadingFactor();
  _radio->setPreambleLength(preambleLengthForSF(_preamble_sf)); // longer preamble for lower SF improves reliability
  state = STATE_IDLE;

  if (_board->getStartupReason() == BD_STARTUP_RX_PACKET) {  // received a LoRa packet (while in deep sleep)
    setFlag(); // LoRa packet is already received
  }

  _noise_floor = 0;
  _threshold = 0;
  _cad_enabled = false;

  // start average out some samples
  _num_floor_samples = 0;
  _floor_sample_sum = 0;
}

uint32_t RadioLibWrapper::getRngSeed() {
  return _radio->random(0x7FFFFFFF);
}

void RadioLibWrapper::setTxPower(int8_t dbm) {
  prepareForRadioConfig();
  _cur_dbm = dbm;
  _dbm_valid = true;
  _radio->setOutputPower(dbm);
}

void RadioLibWrapper::idle() {
  _radio->standby();
  state = STATE_IDLE;   // need another startReceive()
}

void RadioLibWrapper::triggerNoiseFloorCalibrate(int threshold) {
  _threshold = threshold;
  if (_num_floor_samples >= NUM_NOISE_FLOOR_SAMPLES) {  // ignore trigger if currently sampling
    _num_floor_samples = 0;
    _floor_sample_sum = 0;
  }
}

void RadioLibWrapper::doResetAGC() {
  _radio->sleep();  // warm sleep to reset analog frontend
}

void RadioLibWrapper::resetAGC() {
  // Do not reset while a packet or a transmission is in progress.
  if ((state & STATE_INT_READY) != 0 || isReceivingPacket() ||
      (state & ~STATE_INT_READY) == STATE_TX_WAIT) return;

  doResetAGC();
  state = STATE_IDLE;   // trigger a startReceive()

  // Reset noise floor sampling so it reconverges from scratch.
  // Without this, a stuck _noise_floor of -120 makes the sampling threshold
  // too low (-106) to accept normal samples (~-105), self-reinforcing the
  // stuck value even after the receiver has recovered.
  _noise_floor = 0;
  _num_floor_samples = 0;
  _floor_sample_sum = 0;
}

void RadioLibWrapper::rxPsWatchdogCheck() {
  // A pending IRQ or TX is already proof of activity; never disturb either.
  if ((state & STATE_INT_READY) != 0 || (state & ~STATE_INT_READY) == STATE_TX_WAIT) {
    _wd_observe_until = 0;
    return;
  }

  const unsigned long now = millis();
  bool tripped = false;

  if (_rx_ps_armed && state == STATE_RX && _wd_stuck_thresh > 0) {
    const bool busy = isChipBusy();
    if (busy != _wd_last_busy) {
      _wd_last_busy = busy;
      _wd_last_transition = now;
      _wd_stage = 0;
      _wd_strikes = 0;
      _wd_observe_until = 0;
    } else if (_wd_observe_until != 0) {
      if ((long)(now - _wd_observe_until) >= 0) {
        _wd_observe_until = 0;
        if (!busy && isReceivingPacket()) {
          // Extended RX after preamble detection legitimately holds BUSY low.
          _wd_last_transition = now;
          _wd_strikes = 0;
        } else if (++_wd_strikes >= 2) {
          _wd_strikes = 0;
          tripped = true;
        } else {
          _wd_last_transition = now;
        }
      }
    } else if (now - _wd_last_transition > _wd_stuck_thresh) {
      _wd_observe_until = now + _wd_observe_ms;
      if (_wd_observe_until == 0) _wd_observe_until = 1;
    }
  } else {
    _wd_observe_until = 0;
  }

  if (_startrx_fails >= 3) tripped = true;
  if (!tripped) return;

  _wd_last_transition = now;
  _startrx_fails = 0;
  _wd_observe_until = 0;

  if (_wd_stage == 0) {
    _wd_stage = 1;
    n_wd_soft++;
    MESH_DEBUG_PRINTLN("RadioLibWrapper: RXPS watchdog soft re-arm");
    state = STATE_IDLE;
  } else {
    _wd_stage = 2;
    n_wd_hard++;
    MESH_DEBUG_PRINTLN("RadioLibWrapper: RXPS watchdog hard radio reset");
    if (radioDeepInit()) {
      _rx_ps_armed = false;
      _radio->setPacketReceivedAction(setFlag);
      if (_params_valid) setParams(_cur_freq, _cur_bw, _cur_sf, _cur_cr);
      if (_dbm_valid) _radio->setOutputPower(_cur_dbm);
    }
    state = STATE_IDLE;
  }
}

void RadioLibWrapper::noiseFloorCalibCheck() {
  const unsigned long now = millis();
  if (_nf_calib_active) {
    // A zero threshold can also be set while a window is open; close it early.
    if (!_rx_ps_enabled || !needsNoiseFloor() || (long)(now - _nf_calib_deadline) >= 0) {
      endNoiseFloorCalib(now);
    }
  } else if (_rx_ps_enabled && _rx_ps_armed && needsNoiseFloor() && state == STATE_RX &&
             (_nf_last_calib == 0 || now - _nf_last_calib >= NF_CALIB_INTERVAL_MS) &&
             !isReceivingPacket()) {
    _nf_calib_active = true;
    _nf_calib_deadline = now + NF_CALIB_TIMEOUT_MS;
    _nf_sample_from = now + NF_CALIB_SETTLE_MS;
    _num_floor_samples = 0;
    _floor_sample_sum = 0;
    state = STATE_IDLE;
  }
}

void RadioLibWrapper::endNoiseFloorCalib(unsigned long now) {
  _nf_calib_active = false;
  _nf_last_calib = now;
  if ((state & STATE_INT_READY) == 0 && (state & ~STATE_INT_READY) != STATE_TX_WAIT) {
    state = STATE_IDLE;
  }
}

void RadioLibWrapper::loop() {
  if (_rx_ps_enabled) rxPsWatchdogCheck();
  noiseFloorCalibCheck();

  if (state == STATE_RX && _num_floor_samples < NUM_NOISE_FLOOR_SAMPLES) {
    if (!_rx_ps_armed && !(_nf_calib_active && (long)(millis() - _nf_sample_from) < 0) &&
        !isReceivingPacket()) {
      int rssi = getCurrentRSSI();
      if (rssi < _noise_floor + SAMPLING_THRESHOLD) {  // only consider samples below current floor + sampling THRESHOLD
        _num_floor_samples++;
        _floor_sample_sum += rssi;
      }
    }
  } else if (_num_floor_samples >= NUM_NOISE_FLOOR_SAMPLES && _floor_sample_sum != 0) {
    _noise_floor = _floor_sample_sum / NUM_NOISE_FLOOR_SAMPLES;
    if (_noise_floor < -120) {
      _noise_floor = -120;    // clamp to lower bound of -120dBi
    }
    _floor_sample_sum = 0;

    #ifdef MESH_DEBUG_NOISE_FLOOR
    MESH_DEBUG_PRINTLN("RadioLibWrapper: noise_floor = %d", (int)_noise_floor);
    #endif

    if (_nf_calib_active) endNoiseFloorCalib(millis());
  }
}

void RadioLibWrapper::startRecv() {
  #if defined(USE_LR2021)
  _radio->standby(); // without this LR2021 can throw -706 when calling startReceive after hardware CAD when side detectors are enabled
  #endif
  int err = startReceiveMode();
  if (err == RADIOLIB_ERR_NONE) {
    state = STATE_RX;
    _startrx_fails = 0;
    if (_rx_ps_armed) {
      _wd_last_busy = isChipBusy();
      _wd_last_transition = millis();

      const uint32_t rx_ms = _rx_ps_rx_us / 1000;
      const uint32_t sleep_ms = _rx_ps_sleep_us / 1000;
      _wd_stuck_thresh = (rx_ms + sleep_ms) + 2 * (2 * rx_ms + sleep_ms) +
                         getEstAirtimeFor(MAX_TRANS_UNIT) + 1000;
      if (_wd_stuck_thresh < 60000) _wd_stuck_thresh = 60000;
      _wd_observe_ms = rx_ms + sleep_ms + 50;
      if (_wd_observe_ms > 1500) _wd_observe_ms = 1500;
    }
  } else {
    if (_startrx_fails < 255) _startrx_fails++;
    MESH_DEBUG_PRINTLN("RadioLibWrapper: error: startReceiveMode(%d)", err);
  }
}

int RadioLibWrapper::startReceiveMode() {
  return _radio->startReceive();
}

void RadioLibWrapper::stopReceiveDutyCycle() {
  _radio->standby();
  _rx_ps_armed = false;
}

bool RadioLibWrapper::isPacketReady() {
  if (!_rx_ps_armed) return true;
  // RX timeout/header-error IRQs are also routed to DIO1 in duty-cycle mode.
  // Reading in those cases would return stale bytes from the previous packet.
  return _radio->checkIrq(RADIOLIB_IRQ_RX_DONE) != 0;
}

void RadioLibWrapper::prepareForRadioConfig() {
  if (_rx_ps_armed) {
    stopReceiveDutyCycle();
  } else if ((state & ~STATE_INT_READY) == STATE_RX) {
    _radio->standby();
  }
  _rx_hold_continuous = false;
  state = STATE_IDLE;
}

bool RadioLibWrapper::prepareForSleep() {
  if (state & STATE_INT_READY) return true;
  if ((state & ~STATE_INT_READY) != STATE_RX) startRecv();
  return (state & STATE_INT_READY) != 0;
}

bool RadioLibWrapper::setRxPowerSaving(bool enabled, uint32_t rx_us, uint32_t sleep_us) {
  if (enabled && (!supportsRxPowerSaving() || !meshcore::rxps::isValidPeriod(rx_us) ||
                  !meshcore::rxps::isValidPeriod(sleep_us))) {
    return false;
  }

  _rx_ps_enabled = enabled;
  _rx_ps_rx_us = rx_us;
  _rx_ps_sleep_us = sleep_us;
  _wd_stage = 0;
  _wd_strikes = 0;
  _wd_observe_until = 0;

  // Leave a completed packet and an in-flight TX untouched. Otherwise switch
  // modes immediately; the next recvRaw() performs the actual re-arm.
  if ((state & STATE_INT_READY) == 0 && (state & ~STATE_INT_READY) != STATE_TX_WAIT) {
    if (_rx_ps_armed || (state & ~STATE_INT_READY) == STATE_RX) stopReceiveDutyCycle();
    state = STATE_IDLE;
  }
  return true;
}

bool RadioLibWrapper::isInRecvMode() const {
  return (state & ~STATE_INT_READY) == STATE_RX;
}

int RadioLibWrapper::recvRaw(uint8_t* bytes, int sz) {
  int len = 0;
  if (state & STATE_INT_READY) {
    if (isPacketReady()) {
      if (_rx_ps_armed) stopReceiveDutyCycle();
      len = _radio->getPacketLength();
      if (len > 0) {
        if (len > sz) { len = sz; }
        // Cache metadata before readData()/RX re-arm changes radio state.
        _last_snr = _radio->getSNR();
        _last_rssi = _radio->getRSSI();
        int err = _radio->readData(bytes, len);
        if (err != RADIOLIB_ERR_NONE) {
          MESH_DEBUG_PRINTLN("RadioLibWrapper: error: readData(%d)", err);
          len = 0;
          n_recv_errors++;
        } else {
        //  Serial.print("  readData() -> "); Serial.println(len);
          n_recv++;
        }
      }
    }
    #if defined(USE_LR2021)
    state = STATE_RX;     // LR2021 stays in Rx after readData, calling startReceive while still in Rx throws -706 errors
    #else
    state = STATE_IDLE;   // need another startReceive()
    #endif
  }

  if (len > 0 && _rx_ps_enabled) {
    // Keep plain RX while Dispatcher consumes the cached metadata and packet.
    // onReceiveProcessed() then restores duty-cycle RX unless TX started.
    _rx_hold_continuous = true;
    int err = _radio->startReceive();
    if (err == RADIOLIB_ERR_NONE) {
      state = STATE_RX;
      if (_nf_calib_active) _nf_sample_from = millis() + NF_CALIB_SETTLE_MS;
    } else {
      MESH_DEBUG_PRINTLN("RadioLibWrapper: error: continuous RX after packet (%d)", err);
    }
    return len;
  }

  if (state != STATE_RX) {
    startRecv();
  }
  return len;
}

void RadioLibWrapper::onReceiveProcessed() {
  if (!_rx_hold_continuous) return;

  if ((state & ~STATE_INT_READY) == STATE_TX_WAIT) {
    _rx_hold_continuous = false;
    return;
  }
  if ((state & STATE_INT_READY) != 0 || isReceivingPacket()) return;

  _rx_hold_continuous = false;
  if (!_rx_ps_enabled || _nf_calib_active) return;

  state = STATE_IDLE;
  startRecv();
}

uint32_t RadioLibWrapper::getEstAirtimeFor(int len_bytes) {
  return _radio->getTimeOnAir(len_bytes) / 1000;
}

bool RadioLibWrapper::startSendRaw(const uint8_t* bytes, int len) {
  if (_rx_ps_armed) {
    // A pending RxDutyCycle RTC event can otherwise abort SetTx mid-packet.
    stopReceiveDutyCycle();
  }
  _rx_hold_continuous = false;
  _board->onBeforeTransmit();
  int err = _radio->startTransmit((uint8_t *) bytes, len);
  if (err == RADIOLIB_ERR_NONE) {
    state = STATE_TX_WAIT;
    return true;
  }
  MESH_DEBUG_PRINTLN("RadioLibWrapper: error: startTransmit(%d)", err);
  idle();   // trigger another startRecv()
  _board->onAfterTransmit();
  return false;
}

bool RadioLibWrapper::isSendComplete() {
  if (state & STATE_INT_READY) {
    state = STATE_IDLE;
    n_sent++;
    return true;
  }
  return false;
}

void RadioLibWrapper::onSendFinished() {
  _radio->finishTransmit();
  _board->onAfterTransmit();
  state = STATE_IDLE;
}

int16_t RadioLibWrapper::performChannelScan() {
  return _radio->scanChannel();
}

bool RadioLibWrapper::isChannelActive() {
  // int.thresh: RSSI-based interference detection (relative to noise floor)
  if (_threshold != 0 && !(_rx_ps_armed && isChipBusy()) &&
      getCurrentRSSI() > _noise_floor + _threshold) return true;

  // cad: hardware channel activity detection
  if (_cad_enabled) {
    if (_rx_ps_armed) {
      // Stop both the duty sequencer and its RTC before CAD. A pending RTC
      // event can otherwise return the chip to standby while scanChannel()
      // waits for CAD_DONE.
      stopReceiveDutyCycle();
    }
    int16_t result = performChannelScan();
    // scanChannel() triggers DIO interrupt (CAD done) which sets STATE_INT_READY
    // via setFlag() ISR. Clear it before restarting RX so recvRaw() doesn't
    // try to read a non-existent packet and count a spurious recv error.
    state = STATE_IDLE;
    startRecv();
    if (result != RADIOLIB_CHANNEL_FREE) return true;
  }

  return false;
}

float RadioLibWrapper::getLastRSSI() const {
  return _last_rssi;
}
float RadioLibWrapper::getLastSNR() const {
  return _last_snr;
}

// Approximate SNR threshold per SF for successful reception (based on Semtech datasheets)
static float snr_threshold[] = {
    -7.5,  // SF7 needs at least -7.5 dB SNR
    -10,   // SF8 needs at least -10 dB SNR
    -12.5, // SF9 needs at least -12.5 dB SNR
    -15,  // SF10 needs at least -15 dB SNR
    -17.5,// SF11 needs at least -17.5 dB SNR
    -20   // SF12 needs at least -20 dB SNR
};

float RadioLibWrapper::packetScoreInt(float snr, int sf, int packet_len) {
  if (sf < 7) return 0.0f;

  if (snr < snr_threshold[sf - 7]) return 0.0f;    // Below threshold, no chance of success

  auto success_rate_based_on_snr = (snr - snr_threshold[sf - 7]) / 10.0;
  auto collision_penalty = 1 - (packet_len / 256.0);   // Assuming max packet of 256 bytes

  return max(0.0, min(1.0, success_rate_based_on_snr * collision_penalty));
}

PacketMillis RadioLibWrapper::calcMaxPacketMillis(uint8_t sf, float bw, uint8_t cr, uint8_t preambleSymbols) {
  // based on RadioLib's calculateTimeOnAir()
  uint32_t tsym_us = ((uint32_t)10000 << sf) / (bw * 10);
  uint32_t sfCoeff1_x4 = (sf == 5 || sf == 6) ? 25 : 17; // 6.25 : 4.25, semtech magic numbers to account for sync word + sfd

  // preamble + syncword + sfd + header
  uint32_t preamble_us = (((preambleSymbols + 8) * 4 + sfCoeff1_x4) * tsym_us) / 4;

  // airtime for max packet at current radio settings
  uint32_t total_us   = _radio->getTimeOnAir(MAX_TRANS_UNIT);
  // airtime for payload only (no preamble, header or SOF)
  uint32_t payload_us = total_us > preamble_us ? total_us - preamble_us : 4000 - preamble_us; // fallback to 4 secs at worst case
  // rescale payload_us for max possible CR
  if (cr >= 5 && cr < 8) { payload_us = (payload_us * 8) / cr; }

  return PacketMillis {(preamble_us + 999) / 1000, (payload_us + 999) / 1000};
}
