
#define RADIOLIB_STATIC_ONLY 1
#include "RadioLibWrappers.h"

#define STATE_IDLE       0
#define STATE_RX         1
#define STATE_TX_WAIT    3
#define STATE_TX_DONE    4
#define STATE_INT_READY 16

#define NUM_NOISE_FLOOR_SAMPLES  64
#define MIN_NOISE_FLOOR_SAMPLES  12       // enough to publish a floor from a starved batch
#define SAMPLING_THRESHOLD  14
#define NOISE_FLOOR_BATCH_TIMEOUT  30000  // ms a single calibration batch may take

static volatile uint8_t state = STATE_IDLE;

// this function is called when a complete packet
// is transmitted by the module
static
#if defined(ESP8266) || defined(ESP32)
  ICACHE_RAM_ATTR
#endif
void setFlag(void) {
  // we sent a packet, set the flag
  state |= STATE_INT_READY;
}

void RadioLibWrapper::begin() {
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
  _mid_packet = false;

  // start average out some samples
  startFloorBatch();
}

void RadioLibWrapper::startFloorBatch() {
  _num_floor_samples = 0;
  _floor_sample_sum = 0;
  _floor_batch_start = millis();
}

void RadioLibWrapper::publishNoiseFloor() {
  if (_num_floor_samples == 0) return;

  int32_t nf = _floor_sample_sum / (int32_t)_num_floor_samples;
  if (nf < -120) {
    nf = -120;    // clamp to lower bound of -120dBi
  }
  _noise_floor = (int16_t)nf;
  _floor_sample_sum = 0;

  #ifdef MESH_DEBUG_NOISE_FLOOR
  MESH_DEBUG_PRINTLN("RadioLibWrapper: noise_floor = %d (%d samples)", (int)_noise_floor,
                     (int)_num_floor_samples);
  #endif
}

uint32_t RadioLibWrapper::getRngSeed() {
  return _radio->random(0x7FFFFFFF);
}

void RadioLibWrapper::setTxPower(int8_t dbm) {
#if defined(USE_LR2021)
  idle();
#endif
  _radio->setOutputPower(dbm);
}

void RadioLibWrapper::idle() {
  _radio->standby();
  state = STATE_IDLE;   // need another startReceive()
}

void RadioLibWrapper::triggerNoiseFloorCalibrate(int threshold) {
  _threshold = threshold;
  if (_num_floor_samples >= NUM_NOISE_FLOOR_SAMPLES) {  // previous batch is complete
    startFloorBatch();
    return;
  }

  // A batch is still running. Samples are only accepted below _noise_floor + SAMPLING_THRESHOLD,
  // so if the ambient floor has risen by more than that, NOTHING is accepted: the batch never
  // completes, every later trigger is ignored as 'still sampling', and _noise_floor stays frozen
  // at its old low value for good. With int.thresh enabled that reads as a channel that is
  // permanently busy, because getCurrentRSSI() is then always above _noise_floor + threshold.
  // So the batch gets a deadline.
  if ((uint32_t)(millis() - _floor_batch_start) < NOISE_FLOOR_BATCH_TIMEOUT) return;

  if (_num_floor_samples >= MIN_NOISE_FLOOR_SAMPLES) {
    publishNoiseFloor();    // short batch, but enough samples to mean something
  } else {
    // Not even that. The stored floor is the thing blocking acceptance, so drop it and let the
    // next batch converge unfiltered, the same way it does at boot.
    MESH_DEBUG_PRINTLN("RadioLibWrapper: noise floor batch starved (%d samples), reconverging",
                       (int)_num_floor_samples);
    _noise_floor = 0;
  }
  startFloorBatch();
}

void RadioLibWrapper::doResetAGC() {
  _radio->sleep();  // warm sleep to reset analog frontend
}

void RadioLibWrapper::resetAGC() {
  // make sure we're not mid-receive of packet!
  if ((state & STATE_INT_READY) != 0 || isReceivingPacket()) return;

  doResetAGC();
  state = STATE_IDLE;   // trigger a startReceive()

  // Reset noise floor sampling so it reconverges from scratch.
  // Without this, a stuck _noise_floor of -120 makes the sampling threshold
  // too low (-106) to accept normal samples (~-105), self-reinforcing the
  // stuck value even after the receiver has recovered.
  _noise_floor = 0;
  startFloorBatch();
}

void RadioLibWrapper::loop() {
  if (_num_floor_samples < NUM_NOISE_FLOOR_SAMPLES) {
    if (state == STATE_RX && !isReceivingPacket()) {
      int rssi = getCurrentRSSI();
      if (rssi < _noise_floor + SAMPLING_THRESHOLD) {  // only consider samples below current floor + sampling THRESHOLD
        _num_floor_samples++;
        _floor_sample_sum += rssi;
      }
    }
  } else if (_floor_sample_sum != 0) {
    publishNoiseFloor();
  }
}

void RadioLibWrapper::startRecv() {
  #if defined(USE_LR2021)
  _radio->standby(); // without this LR2021 can throw -706 when calling startReceive after hardware CAD when side detectors are enabled
  #endif
  int err = _radio->startReceive();
  if (err == RADIOLIB_ERR_NONE) {
    state = STATE_RX;
  } else {
    MESH_DEBUG_PRINTLN("RadioLibWrapper: error: startReceive(%d)", err);
  }
}

bool RadioLibWrapper::isInRecvMode() const {
  return (state & ~STATE_INT_READY) == STATE_RX;
}

int RadioLibWrapper::recvRaw(uint8_t* bytes, int sz) {
  int len = 0;
  if (state & STATE_INT_READY) {
    len = _radio->getPacketLength();
    if (len > 0) {
      if (len > sz) { len = sz; }
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
    #if defined(USE_LR2021)
    state = STATE_RX;     // LR2021 stays in Rx after readData, calling startReceive while still in Rx throws -706 errors
    #else
    state = STATE_IDLE;   // need another startReceive()
    #endif
  }

  if (state != STATE_RX) {
    int err = _radio->startReceive();
    if (err == RADIOLIB_ERR_NONE) {
      state = STATE_RX;
    } else {
      MESH_DEBUG_PRINTLN("RadioLibWrapper: error: startReceive(%d)", err);
    }
  }
  return len;
}

uint32_t RadioLibWrapper::getEstAirtimeFor(int len_bytes) {
  return _radio->getTimeOnAir(len_bytes) / 1000;
}

bool RadioLibWrapper::startSendRaw(const uint8_t* bytes, int len) {
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
  // int.thresh: RSSI-based interference detection (relative to noise floor).
  //
  // Two reads, both required to be over the threshold. A single instantaneous RSSI sample has
  // several dB of spread, and one high outlier is enough to stall a transmit for a whole retry
  // period. A real carrier lasts hundreds of milliseconds, so it comfortably survives both
  // reads; the second read only happens when the first was already over, so a free channel
  // still costs exactly one SPI transaction.
  if (_threshold != 0) {
    float limit = (float)(_noise_floor + _threshold);
    if (getCurrentRSSI() > limit && getCurrentRSSI() > limit) return true;
  }

  // cad: hardware channel activity detection
  if (_cad_enabled) {
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
  return _radio->getRSSI();
}
float RadioLibWrapper::getLastSNR() const {
  return _radio->getSNR();
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

  PacketMillis pm {(preamble_us + 999) / 1000, (payload_us + 999) / 1000};

  // Cached for the dispatcher's transmit deadlines. Those have to be derived from the same
  // airtime figures the radio uses to time out its own stuck Rx IRQ flags, otherwise the two
  // disagree and a legitimately long reception gets cut short by a forced transmit.
  _preamble_millis = pm.preambleMillis;
  _max_packet_millis = pm.preambleMillis + pm.payloadMillis;

  return pm;
}
