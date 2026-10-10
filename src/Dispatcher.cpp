#include "Dispatcher.h"

#if MESH_PACKET_LOGGING
  #include <Arduino.h>
#endif

#include <math.h>

#if defined(ARDUINO_ARCH_RP2040) && defined(MLK_RP2040_LOWPOWER)
  #include <pico/time.h>   // best_effort_wfe_or_timeout(), make_timeout_time_ms()
#endif

namespace mesh {

#define MAX_RX_DELAY_MILLIS        32000  // 32 seconds
#define MIN_TX_BUDGET_RESERVE_MS   100    // min budget (ms) required before allowing next TX
#define MIN_TX_BUDGET_AIRTIME_DIV  2      // require at least 1/N of estimated airtime as budget before TX

#define CAD_FAIL_MAX_DURATION_FLOOR      4000   // legacy fixed value, kept as the floor at fast presets
#define CAD_FAIL_MAX_DURATION_CEIL      12000   // a preset slower than this cannot be rescued by waiting
#define CHANNEL_BUSY_MAX_DURATION_FLOOR   400   // a bare preamble detect never justifies more than this
#define CAD_RETRY_MAX_SHIFT                 2   // busy backoff tops out at 4x the base retry delay

#ifndef NOISE_FLOOR_CALIB_INTERVAL
  #define NOISE_FLOOR_CALIB_INTERVAL   2000     // 2 seconds
#endif

void Dispatcher::begin() {
  n_sent_flood = n_sent_direct = 0;
  n_recv_flood = n_recv_direct = 0;
  _err_flags = 0;
  radio_nonrx_start = _ms->getMillis();

  duty_cycle_window_ms = getDutyCycleWindowMs();
  float duty_cycle = currentDutyCycle();
  tx_budget_ms = (unsigned long)(duty_cycle_window_ms * duty_cycle);
  last_budget_update = _ms->getMillis();

  _radio->begin();
  prev_isrecv_mode = _radio->isInRecvMode();
}

float Dispatcher::getAirtimeBudgetFactor() const {
  return 1.0;
}

void Dispatcher::updateTxBudget() {
  unsigned long now = _ms->getMillis();
  unsigned long elapsed = now - last_budget_update;

  float duty_cycle = currentDutyCycle();
  unsigned long max_budget = (unsigned long)(getDutyCycleWindowMs() * duty_cycle);
  unsigned long refill = (unsigned long)(elapsed * duty_cycle);
  
  if (refill > 0) {
    tx_budget_ms += refill;
    if (tx_budget_ms > max_budget) {
      tx_budget_ms = max_budget;
    }
    last_budget_update = now;
  }
}

int Dispatcher::calcRxDelay(float score, uint32_t air_time) const {
  return (int) ((powf(10.0f, 0.85f - score) - 1.0f) * air_time);
}

float Dispatcher::currentDutyCycle() {
  // Memoised: recompute only when the airtime budget factor actually changes,
  // avoiding a soft-float division on every TX-budget update on the M0+.
  float f = getAirtimeBudgetFactor();
  if (f != _cached_airtime_factor) {
    _cached_airtime_factor = f;
    _cached_duty_cycle = 1.0f / (1.0f + f);
  }
  return _cached_duty_cycle;
}

uint32_t Dispatcher::getCADFailRetryDelay() const {
  return 200;
}
uint32_t Dispatcher::getCADFailMaxDuration() const {
  // Derived from the same airtime figure the radio uses to time out its own stuck Rx IRQ
  // flags, plus margin, so the radio always releases a stuck flag before the dispatcher
  // gives up on the channel. A constant cannot hold that invariant: one 255 byte packet is
  // 2.3s at SF8/BW62.5, but 8.4s at SF10/BW62.5 and 6.7s at SF12/BW250.
  uint32_t t = _radio->getMaxPacketMillis();
  if (t == 0) return CAD_FAIL_MAX_DURATION_FLOOR;   // radio cannot say: keep the legacy 4s
  t += t / 8 + 250;
  if (t < CAD_FAIL_MAX_DURATION_FLOOR) t = CAD_FAIL_MAX_DURATION_FLOOR;
  if (t > CAD_FAIL_MAX_DURATION_CEIL) t = CAD_FAIL_MAX_DURATION_CEIL;
  return t;
}
uint32_t Dispatcher::getChannelBusyMaxDuration() const {
  uint32_t t = _radio->getPreambleMillis();
  // Twice the preamble-to-header window: a real packet produces a header (or a header error)
  // within one such window, so anything still 'busy' after two of them was not a packet.
  t = (t == 0) ? CHANNEL_BUSY_MAX_DURATION_FLOOR : t * 2 + 100;
  if (t < CHANNEL_BUSY_MAX_DURATION_FLOOR) t = CHANNEL_BUSY_MAX_DURATION_FLOOR;
  uint32_t cap = getCADFailMaxDuration();
  return t > cap ? cap : t;
}

void Dispatcher::loop() {
  if (millisHasNowPassed(next_floor_calib_time)) {
    _radio->triggerNoiseFloorCalibrate(getInterferenceThreshold());
    _radio->setCADEnabled(getCADEnabled());
    next_floor_calib_time = futureMillis(NOISE_FLOOR_CALIB_INTERVAL);
  }
  _radio->loop();

  // check for radio 'stuck' in mode other than Rx
  bool is_recv = _radio->isInRecvMode();
  if (is_recv != prev_isrecv_mode) {
    prev_isrecv_mode = is_recv;
    if (!is_recv) {
      radio_nonrx_start = _ms->getMillis();
    }
  }
  if (!is_recv && _ms->getMillis() - radio_nonrx_start > 8000) {   // radio has not been in Rx mode for 8 seconds!
    _err_flags |= ERR_EVENT_STARTRX_TIMEOUT;
  }

  if (outbound) {  // waiting for outbound send to be completed
    if (_radio->isSendComplete()) {
      long t = _ms->getMillis() - outbound_start;
      total_air_time += t;
      //Serial.print("  airtime="); Serial.println(t);

      updateTxBudget();

      if (t > tx_budget_ms) {
        tx_budget_ms = 0;
      } else {
        tx_budget_ms -= t;
      }

      if (tx_budget_ms < MIN_TX_BUDGET_RESERVE_MS) {
        float duty_cycle = currentDutyCycle();
        unsigned long needed = MIN_TX_BUDGET_RESERVE_MS - tx_budget_ms;
        next_tx_time = futureMillis((unsigned long)(needed / duty_cycle));
      } else {
        next_tx_time = _ms->getMillis();
      }

      _radio->onSendFinished();
      logTx(outbound, 2 + outbound->getPathByteLen() + outbound->payload_len);
      if (outbound->isRouteFlood()) {
        n_sent_flood++;
      } else {
        n_sent_direct++;
      }
      releasePacket(outbound);  // return to pool
      outbound = NULL;
    } else if (millisHasNowPassed(outbound_expiry)) {
      MESH_DEBUG_PRINTLN("%s Dispatcher::loop(): WARNING: outbound packed send timed out!", getLogDateTime());

      _radio->onSendFinished();
      logTxFail(outbound, 2 + outbound->getPathByteLen() + outbound->payload_len);

      releasePacket(outbound);  // return to pool
      outbound = NULL;
    } else {
      return;  // can't do any more radio activity until send is complete or timed out
    }

    // going back into receive mode now...
    next_agc_reset_time = futureMillis(getAGCResetInterval());
  }

  if (getAGCResetInterval() > 0 && millisHasNowPassed(next_agc_reset_time)) {
    _radio->resetAGC();
    next_agc_reset_time = futureMillis(getAGCResetInterval());
  }

  // check inbound (delayed) queue
  {
    Packet* pkt = _mgr->getNextInbound(_ms->getMillis());
    if (pkt) {
      processRecvPacket(pkt);
    }
  }
  checkRecv();
  checkSend();

#if defined(ARDUINO_ARCH_RP2040) && defined(MLK_RP2040_LOWPOWER)
  // Low-power idle: sleep the core until the nearest scheduled deadline (delayed inbound,
  // scheduled TX, noise-floor calibration, and subclass work reported by nextAppWake())
  // or until the radio DIO1 IRQ fires. best_effort_wfe_or_timeout() arms a hardware timer
  // alarm (the same mechanism the core's sleep_ms uses) and waits on WFE, so wake-up does
  // NOT rely on SysTick surviving WFI on RP2040. Never idle while a TX is in flight.
  if (outbound == NULL) {
    uint32_t sleep_ms = idleSleepMillis(_ms->getMillis());
    if (sleep_ms > 0) {
      best_effort_wfe_or_timeout(make_timeout_time_ms(sleep_ms));
    }
  }
#endif
}

uint32_t Dispatcher::idleSleepMillis(uint32_t now) const {
  uint32_t sleep_ms = 2000;   // cap = noise-floor calib interval; also a safety net
  int32_t d;

  // periodic noise-floor calibration (always scheduled)
  d = (int32_t)(next_floor_calib_time - now);
  if (d <= 0) return 0;
  if ((uint32_t)d < sleep_ms) sleep_ms = (uint32_t)d;

  // periodic AGC reset (only when enabled)
  if (getAGCResetInterval() > 0) {
    d = (int32_t)(next_agc_reset_time - now);
    if (d <= 0) return 0;
    if ((uint32_t)d < sleep_ms) sleep_ms = (uint32_t)d;
  }

  // scheduled outbound (delayed TX) and delayed inbound (score-delayed RX) queues
  uint32_t sched = _mgr->getNextOutboundSchedule();
  if (sched != 0xFFFFFFFF) {
    d = (int32_t)(sched - now);
    if (d <= 0) return 0;
    if ((uint32_t)d < sleep_ms) sleep_ms = (uint32_t)d;
  }
  sched = _mgr->getNextInboundSchedule();
  if (sched != 0xFFFFFFFF) {
    d = (int32_t)(sched - now);
    if (d <= 0) return 0;
    if ((uint32_t)d < sleep_ms) sleep_ms = (uint32_t)d;
  }

  // subclass-specific timed work (flood-retry, ping, ...)
  uint32_t app = nextAppWake(now);
  if (app != 0) {
    d = (int32_t)(app - now);
    if (d <= 0) return 0;
    if ((uint32_t)d < sleep_ms) sleep_ms = (uint32_t)d;
  }

  return sleep_ms;
}

bool Dispatcher::tryParsePacket(Packet* pkt, const uint8_t* raw, int len) {
  int i = 0;

  pkt->header = raw[i++];
  if (pkt->getPayloadVer() > PAYLOAD_VER_1) {
    MESH_DEBUG_PRINTLN("%s Dispatcher::checkRecv(): unsupported packet version", getLogDateTime());
    return false;
  }

  if (pkt->hasTransportCodes()) {
    memcpy(&pkt->transport_codes[0], &raw[i], 2); i += 2;
    memcpy(&pkt->transport_codes[1], &raw[i], 2); i += 2;
  } else {
    pkt->transport_codes[0] = pkt->transport_codes[1] = 0;
  }

  pkt->path_len = raw[i++];
  uint8_t path_mode = pkt->path_len >> 6;  // upper 2 bits (legacy firmware: 00)
  if (path_mode == 3) {   // Reserved for future
    MESH_DEBUG_PRINTLN("%s Dispatcher::checkRecv(): unsupported path mode: 3", getLogDateTime());
    return false;
  }

  uint8_t path_byte_len = (pkt->path_len & 63) * pkt->getPathHashSize();
  if (path_byte_len > MAX_PATH_SIZE || i + path_byte_len > len) {
    MESH_DEBUG_PRINTLN("%s Dispatcher::checkRecv(): partial or corrupt packet received, len=%d", getLogDateTime(), len);
    return false;
  }

  memcpy(pkt->path, &raw[i], path_byte_len); i += path_byte_len;

  pkt->payload_len = len - i;  // payload is remainder
  if (pkt->payload_len > sizeof(pkt->payload)) {
    MESH_DEBUG_PRINTLN("%s Dispatcher::checkRecv(): packet payload too big, payload_len=%d", getLogDateTime(), (uint32_t)pkt->payload_len);
    return false;
  }

  memcpy(pkt->payload, &raw[i], pkt->payload_len);

  return true;  // success
}

void Dispatcher::checkRecv() {
  Packet* pkt;
  float score;
  uint32_t air_time;
  {
    uint8_t raw[MAX_TRANS_UNIT+1];
    int len = _radio->recvRaw(raw, MAX_TRANS_UNIT);
    if (len > 0) {
      logRxRaw(_radio->getLastSNR(), _radio->getLastRSSI(), raw, len);

      pkt = _mgr->allocNew();
      if (pkt == NULL) {
        MESH_DEBUG_PRINTLN("%s Dispatcher::checkRecv(): WARNING: received data, no unused packets available!", getLogDateTime());
      } else {
        if (tryParsePacket(pkt, raw, len)) {
          pkt->_snr = _radio->getLastSNR() * 4.0f;
          score = _radio->packetScore(_radio->getLastSNR(), len);
          air_time = _radio->getEstAirtimeFor(len);
          rx_air_time += air_time;
        } else {
          _mgr->free(pkt);  // put back into pool
          pkt = NULL;
        }
      }
    } else {
      pkt = NULL;
    }
  }
  if (pkt) {
    #if MESH_PACKET_LOGGING
    Serial.print(getLogDateTime());
    Serial.printf(": RX, len=%d (type=%d, route=%s, payload_len=%d) SNR=%d RSSI=%d score=%d time=%d", 
            pkt->getRawLength(), pkt->getPayloadType(), pkt->isRouteDirect() ? "D" : "F", pkt->payload_len,
            (int)pkt->getSNR(), (int)_radio->getLastRSSI(), (int)(score*1000), air_time);

    static uint8_t packet_hash[MAX_HASH_SIZE];
    pkt->calculatePacketHash(packet_hash);
    Serial.print(" hash=");
    mesh::Utils::printHex(Serial, packet_hash, MAX_HASH_SIZE);

    if (pkt->getPayloadType() == PAYLOAD_TYPE_PATH || pkt->getPayloadType() == PAYLOAD_TYPE_REQ
        || pkt->getPayloadType() == PAYLOAD_TYPE_RESPONSE || pkt->getPayloadType() == PAYLOAD_TYPE_TXT_MSG) {
      Serial.printf(" [%02X -> %02X]\n", (uint32_t)pkt->payload[1], (uint32_t)pkt->payload[0]);
    } else {
      Serial.printf("\n");
    }
    #endif
    logRx(pkt, pkt->getRawLength(), score);   // hook for custom logging

    if (pkt->isRouteFlood()) {
      n_recv_flood++;

      int _delay = calcRxDelay(score, air_time);
      if (_delay < 50) {
        MESH_DEBUG_PRINTLN("%s Dispatcher::checkRecv(), score delay below threshold (%d)", getLogDateTime(), _delay);
        processRecvPacket(pkt);   // is below the score delay threshold, so process immediately
      } else {
        MESH_DEBUG_PRINTLN("%s Dispatcher::checkRecv(), score delay is: %d millis", getLogDateTime(), _delay);
        if (_delay > MAX_RX_DELAY_MILLIS) {
          _delay = MAX_RX_DELAY_MILLIS;
        }
        _mgr->queueInbound(pkt, futureMillis(_delay)); // add to delayed inbound queue
      }
    } else {
      n_recv_direct++;
      processRecvPacket(pkt);
    }
  }
}

void Dispatcher::processRecvPacket(Packet* pkt) {
  DispatcherAction action = onRecvPacket(pkt);
  if (action == ACTION_RELEASE) {
    _mgr->free(pkt);
  } else if (action == ACTION_MANUAL_HOLD) {
    // sub-class is wanting to manually hold Packet instance, and call releasePacket() at appropriate time
  } else {   // ACTION_RETRANSMIT*
    uint8_t priority = (action >> 24) - 1;
    uint32_t _delay = action & 0xFFFFFF;

    _mgr->queueOutbound(pkt, priority, futureMillis(_delay));
  }
}

void Dispatcher::checkSend() {
  if (_mgr->getOutboundCount(_ms->getMillis()) == 0) {
    cad_busy_start = 0;   // nothing pending, so there is no busy episode to be timing
    return;
  }
  
  updateTxBudget();
  
  uint32_t est_airtime = _radio->getEstAirtimeFor(MAX_TRANS_UNIT);
  if (tx_budget_ms < est_airtime / MIN_TX_BUDGET_AIRTIME_DIV) {
    float duty_cycle = currentDutyCycle();
    unsigned long needed = est_airtime / MIN_TX_BUDGET_AIRTIME_DIV - tx_budget_ms;
    next_tx_time = futureMillis((unsigned long)(needed / duty_cycle));
    // Deferring on the airtime budget is not channel activity. Leaving cad_busy_start set
    // here lets a stale timestamp survive the whole deferral, so the first busy observation
    // after the budget refills is already 'past the deadline' - and the transmit is forced
    // with no backoff at all, straight on top of whatever is on air.
    cad_busy_start = 0;
    return;
  }
  
  if (!millisHasNowPassed(next_tx_time)) return;
  if (_radio->isReceiving()) {
    // Two very different conditions arrive here. Mid-packet means a valid header was seen and
    // the radio is demodulating a real packet: transmitting aborts that receive and destroys a
    // packet we may have been about to relay, so it gets the long, airtime-derived deadline.
    // Anything else - a preamble detect that never produced a header, RSSI over the noise
    // floor, a hardware CAD hit - may be noise, and gets a short one.
    if (cad_busy_start == 0) {
      cad_busy_start = _ms->getMillis();   // record when CAD busy state started
      cad_retry_shift = 0;
      cad_busy_mid_packet = false;
      cad_force_jitter = getCADFailForceJitter();   // drawn once, so it is stable for the episode
    }
    // Sticky for the whole episode. Once a real header has been seen, the long deadline stays
    // even if the busy indication drops back to a bare preamble: the elapsed time was spent
    // receiving a real packet, and measuring it against the short deadline would instantly
    // force a transmit over whatever arrived next. Sticky rather than restarting the clock,
    // because restarting it would let alternating preamble/header states defer forever - and
    // the point of the deadline is that the node always eventually transmits.
    if (_radio->isMidPacket()) cad_busy_mid_packet = true;

    unsigned long busy_for = _ms->getMillis() - cad_busy_start;
    unsigned long limit = cad_busy_mid_packet ? getCADFailMaxDuration()
                                             : getChannelBusyMaxDuration();
    // Jitter is capped at half the deadline it perturbs, so it decorrelates nodes without
    // swamping the short noise deadline - an airtime-scaled offset can be several times it.
    unsigned long jitter = cad_force_jitter > limit / 2 ? limit / 2 : cad_force_jitter;
    limit += jitter;

    if (busy_for > limit) {
      _err_flags |= ERR_EVENT_CAD_TIMEOUT;

      MESH_DEBUG_PRINTLN("%s Dispatcher::checkSend(): CAD busy max duration reached! (%d ms, mid_packet=%d)",
                         getLogDateTime(), (int)busy_for, (int)cad_busy_mid_packet);
      // channel activity has gone on too long... (Radio might be in a bad state)
      // force the pending transmit below...
    } else {
      // Truncated exponential backoff. Re-probing on a fixed short period for seconds on end
      // is wasted work, and with hardware CAD enabled every probe takes the receiver off air.
      // The last wait is trimmed so the backoff never overshoots the deadline.
      unsigned long d = (unsigned long)getCADFailRetryDelay() << cad_retry_shift;
      if (cad_retry_shift < CAD_RETRY_MAX_SHIFT) cad_retry_shift++;
      unsigned long remaining = limit - busy_for;
      if (d > remaining) d = remaining > 0 ? remaining : 1;
      next_tx_time = futureMillis((int)d);
      return;
    }
  }
  cad_busy_start = 0;  // reset busy state

  outbound = _mgr->getNextOutbound(_ms->getMillis());
  if (outbound) {
    int len = 0;
    uint8_t raw[MAX_TRANS_UNIT];

    raw[len++] = outbound->header;
    if (outbound->hasTransportCodes()) {
      memcpy(&raw[len], &outbound->transport_codes[0], 2); len += 2;
      memcpy(&raw[len], &outbound->transport_codes[1], 2); len += 2;
    }
    raw[len++] = outbound->path_len;
    len += Packet::writePath(&raw[len], outbound->path, outbound->path_len);

    if (len + outbound->payload_len > MAX_TRANS_UNIT) {
      MESH_DEBUG_PRINTLN("%s Dispatcher::checkSend(): FATAL: Invalid packet queued... too long, len=%d", getLogDateTime(), len + outbound->payload_len);
      _mgr->free(outbound);
      outbound = NULL;
    } else {
      memcpy(&raw[len], outbound->payload, outbound->payload_len); len += outbound->payload_len;

      uint32_t max_airtime = _radio->getEstAirtimeFor(len)*3/2;
      outbound_start = _ms->getMillis();
      bool success = _radio->startSendRaw(raw, len);
      if (!success) {
        MESH_DEBUG_PRINTLN("%s Dispatcher::loop(): ERROR: send start failed!", getLogDateTime());
        n_tx_start_fails++;

        // startTransmit() refusing is a radio state error, not a busy channel. Dropping the
        // packet here loses it silently - no counter, and logTxFail() only writes anything if
        // packet logging happens to be on - which is exactly the kind of invisible loss that
        // makes a missing relay impossible to diagnose in the field. Re-queue a bounded number
        // of times instead, so a genuinely wedged radio still gives up rather than looping.
        //
        // The priority is not recoverable once dequeued: it lives in the send queue, not in
        // the Packet. Re-queueing at the top is the closest thing available, and it is not a
        // promotion - this was the highest priority due packet a moment ago.
        if (tx_start_fail_streak < getTxStartFailRetries()) {
          tx_start_fail_streak++;
          _mgr->queueOutbound(outbound, 0, futureMillis(getCADFailRetryDelay()));
          outbound = NULL;
          return;
        }
        tx_start_fail_streak = 0;

        logTxFail(outbound, outbound->getRawLength());
  
        releasePacket(outbound);  // return to pool
        outbound = NULL;
        return;
      }
      tx_start_fail_streak = 0;
      outbound_expiry = futureMillis(max_airtime);

    #if MESH_PACKET_LOGGING
      Serial.print(getLogDateTime());
      Serial.printf(": TX, len=%d (type=%d, route=%s, payload_len=%d)", 
            len, outbound->getPayloadType(), outbound->isRouteDirect() ? "D" : "F", outbound->payload_len);
      if (outbound->getPayloadType() == PAYLOAD_TYPE_PATH || outbound->getPayloadType() == PAYLOAD_TYPE_REQ
        || outbound->getPayloadType() == PAYLOAD_TYPE_RESPONSE || outbound->getPayloadType() == PAYLOAD_TYPE_TXT_MSG) {
        Serial.printf(" [%02X -> %02X]\n", (uint32_t)outbound->payload[1], (uint32_t)outbound->payload[0]);
      } else {
        Serial.printf("\n");
      }
    #endif
    }
  }
}

Packet* Dispatcher::obtainNewPacket() {
  auto pkt = _mgr->allocNew();  // TODO: zero out all fields
  if (pkt == NULL) {
    _err_flags |= ERR_EVENT_FULL;
  } else {
    pkt->payload_len = pkt->path_len = 0;
    pkt->_snr = 0;
  }
  return pkt;
}

void Dispatcher::releasePacket(Packet* packet) {
  _mgr->free(packet);
}

void Dispatcher::sendPacket(Packet* packet, uint8_t priority, uint32_t delay_millis) {
  if (!Packet::isValidPathLen(packet->path_len) || packet->payload_len > MAX_PACKET_PAYLOAD) {
    MESH_DEBUG_PRINTLN("%s Dispatcher::sendPacket(): ERROR: invalid packet... path_len=%d, payload_len=%d", getLogDateTime(), (uint32_t) packet->path_len, (uint32_t) packet->payload_len);
    _mgr->free(packet);
  } else {
    _mgr->queueOutbound(packet, priority, futureMillis(delay_millis));
  }
}

// Utility function -- handles the case where millis() wraps around back to zero
//   2's complement arithmetic will handle any unsigned subtraction up to HALF the word size (32-bits in this case)
bool Dispatcher::millisHasNowPassed(unsigned long timestamp) const {
  return (long)(_ms->getMillis() - timestamp) > 0;
}

unsigned long Dispatcher::futureMillis(int millis_from_now) const {
  return _ms->getMillis() + millis_from_now;
}

}