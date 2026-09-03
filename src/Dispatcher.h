#pragma once

#include <MeshCore.h>
#include <Identity.h>
#include <Packet.h>
#include <Utils.h>
#include <string.h>

namespace mesh {

/**
 * \brief  Abstraction of local/volatile clock with Millisecond granularity.
*/
class MillisecondClock {
public:
  virtual unsigned long getMillis() = 0;
};

/**
 * \brief  Abstraction of this device's packet radio.
*/
class Radio {
public:
  virtual void begin() { }

  /**
   * \brief  polls for incoming raw packet.
   * \param  bytes  destination to store incoming raw packet.
   * \param  sz   maximum packet size allowed.
   * \returns 0 if no incoming data, otherwise length of complete packet received.
  */
  virtual int recvRaw(uint8_t* bytes, int sz) = 0;

  /**
   * \returns  estimated transmit air-time needed for packet of 'len_bytes', in milliseconds.
  */
  virtual uint32_t getEstAirtimeFor(int len_bytes) = 0;

  virtual float packetScore(float snr, int packet_len) = 0;

  /**
   * \brief  starts the raw packet send. (no wait)
   * \param  bytes   the raw packet data
   * \param  len  the length in bytes
   * \returns true if successfully started
  */
  virtual bool startSendRaw(const uint8_t* bytes, int len) = 0;

  /**
   * \returns true if the previous 'startSendRaw()' completed successfully.
  */
  virtual bool isSendComplete() = 0;

  /**
   * \brief  a hook for doing any necessary clean up after transmit.
  */
  virtual void onSendFinished() = 0;

  /**
   * \brief  do any processing needed on each loop cycle
   */
  virtual void loop() { }

  virtual int getNoiseFloor() const { return 0; }

  virtual void triggerNoiseFloorCalibrate(int threshold) { }

  virtual void setCADEnabled(bool enable) { }

  virtual void resetAGC() { }

  virtual bool isInRecvMode() const = 0;

  /**
   * \returns  true if the radio is currently mid-receive of a packet.
  */
  virtual bool isReceiving() { return false; }

  /**
   * \brief  Whether the busy state reported by the most recent isReceiving() call was a
   *         valid LoRa header, ie. a real packet is being demodulated right now.
   *
   * Only meaningful immediately after isReceiving() returned true; implementations latch it
   * there rather than re-reading the radio. A busy state that is NOT mid-packet - a bare
   * preamble detect that never produced a header, RSSI above the noise floor, a hardware CAD
   * hit - may well be noise, and the dispatcher may transmit through it after a short
   * deadline. Mid-packet must not be interrupted: transmitting aborts the receive and
   * destroys the very packet we were about to relay.
   *
   * The default is the conservative answer, ie. the pre-existing behaviour of never forcing
   * a transmit early. Radios that can tell the two apart override it.
  */
  virtual bool isMidPacket() { return true; }

  /**
   * \returns  worst case milliseconds a single packet can occupy the channel at the current
   *           radio settings (preamble + maximum payload), or 0 if the radio cannot say.
  */
  virtual uint32_t getMaxPacketMillis() const { return 0; }

  /**
   * \returns  milliseconds from preamble-detect to header-valid at the current radio
   *           settings, ie. how long a bare preamble detect can legitimately stay raised.
   *           0 if the radio cannot say.
  */
  virtual uint32_t getPreambleMillis() const { return 0; }

  virtual float getLastRSSI() const { return 0; }
  virtual float getLastSNR() const { return 0; }
};

/**
 * \brief  An abstraction for managing instances of Packets (eg. in a static pool),
 *        and for managing the outbound packet queue.
*/
class PacketManager {
public:
  virtual Packet* allocNew() = 0;
  virtual void free(Packet* packet) = 0;

  virtual void queueOutbound(Packet* packet, uint8_t priority, uint32_t scheduled_for) = 0;
  virtual Packet* getNextOutbound(uint32_t now) = 0;    // by priority
  virtual int getOutboundCount(uint32_t now) const = 0;
  virtual int getOutboundTotal() const = 0;
  virtual int getFreeCount() const = 0;
  virtual Packet* getOutboundByIdx(int i) = 0;
  virtual Packet* removeOutboundByIdx(int i) = 0;
  virtual void queueInbound(Packet* packet, uint32_t scheduled_for) = 0;
  virtual Packet* getNextInbound(uint32_t now) = 0;
};

typedef uint32_t  DispatcherAction;

#define ACTION_RELEASE           (0)
#define ACTION_MANUAL_HOLD       (1)
#define ACTION_RETRANSMIT(pri)   (((uint32_t)1 + (pri))<<24)
#define ACTION_RETRANSMIT_DELAYED(pri, _delay)  ((((uint32_t)1 + (pri))<<24) | (_delay))

#define ERR_EVENT_FULL              (1 << 0)
#define ERR_EVENT_CAD_TIMEOUT       (1 << 1)
#define ERR_EVENT_STARTRX_TIMEOUT   (1 << 2)

/**
 * \brief  The low-level task that manages detecting incoming Packets, and the queueing
 *      and scheduling of outbound Packets.
*/
class Dispatcher {
  Packet* outbound;  // current outbound packet
  unsigned long outbound_expiry, outbound_start, total_air_time, rx_air_time;
  unsigned long next_tx_time;
  unsigned long cad_busy_start;
  unsigned long cad_force_jitter;    // per-episode random offset applied to the force deadline
  bool     cad_busy_mid_packet;      // a valid header was seen at some point in this busy episode
  uint8_t  cad_retry_shift;          // exponential backoff step within the current busy episode
  uint8_t  tx_start_fail_streak;     // consecutive startSendRaw() failures
  uint32_t n_tx_start_fails;         // startSendRaw() failures since boot (or last resetStats)
  unsigned long radio_nonrx_start;
  unsigned long next_floor_calib_time, next_agc_reset_time;
  bool  prev_isrecv_mode;
  uint32_t n_sent_flood, n_sent_direct;
  uint32_t n_recv_flood, n_recv_direct;
  unsigned long tx_budget_ms;
  unsigned long last_budget_update;
  unsigned long duty_cycle_window_ms;
  float _cached_airtime_factor, _cached_duty_cycle;   // memoised duty cycle (see currentDutyCycle)

  void processRecvPacket(Packet* pkt);
  void updateTxBudget();
  float currentDutyCycle();

protected:
  PacketManager* _mgr;
  Radio* _radio;
  MillisecondClock* _ms;
  uint16_t _err_flags;

  Dispatcher(Radio& radio, MillisecondClock& ms, PacketManager& mgr)
    : _radio(&radio), _ms(&ms), _mgr(&mgr)
  {
    outbound = NULL;
    total_air_time = rx_air_time = 0;
    next_tx_time = ms.getMillis();
    cad_busy_start = 0;
    cad_force_jitter = 0;
    cad_busy_mid_packet = false;
    cad_retry_shift = 0;
    tx_start_fail_streak = 0;
    n_tx_start_fails = 0;
    next_floor_calib_time = next_agc_reset_time = 0;
    _err_flags = 0;
    radio_nonrx_start = 0;
    prev_isrecv_mode = true;
    tx_budget_ms = 0;
    last_budget_update = 0;
    duty_cycle_window_ms = 3600000;
    _cached_airtime_factor = -1.0f;   // force currentDutyCycle() to compute on first use
    _cached_duty_cycle = 0.5f;
  }

  virtual DispatcherAction onRecvPacket(Packet* pkt) = 0;

  virtual void logRxRaw(float snr, float rssi, const uint8_t raw[], int len) { }   // custom hook

  virtual void logRx(Packet* packet, int len, float score) { }   // hooks for custom logging
  virtual void logTx(Packet* packet, int len) { }
  virtual void logTxFail(Packet* packet, int len) { }
  virtual const char* getLogDateTime() { return ""; }

  virtual float getAirtimeBudgetFactor() const;
  virtual int calcRxDelay(float score, uint32_t air_time) const;
  virtual uint32_t getCADFailRetryDelay() const;

  /**
   * \brief  How long a busy channel may hold off a pending transmit before it is forced
   *         through, when the radio reports it is mid-packet (a valid header was seen).
   *
   * Must exceed the radio's own stuck-Rx-IRQ timeout, which is derived from the worst case
   * airtime of a maximum length packet. If it does not, a legitimately long reception trips
   * this deadline and the forced transmit destroys it. That is why it cannot be a constant:
   * at SF10+ on 62.5kHz, SF11+ on 125kHz or SF12 on 250kHz, one 255 byte packet already
   * takes longer than the 4 seconds this used to be fixed at.
  */
  virtual uint32_t getCADFailMaxDuration() const;

  /**
   * \brief  Same, for a busy channel that is NOT mid-packet - a preamble detect that never
   *         produced a header, RSSI above the noise floor, or a hardware CAD hit.
   *
   * Any of those can be noise, and holding a transmit for seconds on noise is how a node
   * stalls on an otherwise usable channel. Bounded above by getCADFailMaxDuration() so a
   * subclass that shortens that one (during an OTA session, say) shortens this one with it.
  */
  virtual uint32_t getChannelBusyMaxDuration() const;

  /**
   * \returns  a random offset added to both deadlines above, drawn once per busy episode.
   *
   * Every node that heard the same flood begins its backoff at very nearly the same instant,
   * so without this they all reach the deadline together and the recovery transmit becomes a
   * synchronised collision. The base class has no RNG; Mesh overrides this.
  */
  virtual uint32_t getCADFailForceJitter() const { return 0; }

  /**
   * \returns  how many times a packet is re-queued after startSendRaw() fails, before it is
   *           finally dropped. 0 restores the old drop-on-first-failure behaviour.
  */
  virtual uint8_t getTxStartFailRetries() const { return 2; }
  virtual int getInterferenceThreshold() const { return 0; }    // disabled by default
  virtual bool getCADEnabled() const { return false; }    // hardware CAD disabled by default
  virtual int getAGCResetInterval() const { return 0; }    // disabled by default
  virtual unsigned long getDutyCycleWindowMs() const { return 3600000; }

  /**
   * \brief  Nearest scheduled wake time (absolute millis) from subclass-specific timed
   *         work, or 0 if there is none.
   *
   * A subclass that runs its own timers off loop() (retry queues, in-flight probes, ...)
   * reports its earliest deadline here. Platforms whose loop() idles the core between
   * radio events consult it so they never sleep past such a deadline. The base class has
   * no timed work of its own.
   */
  virtual uint32_t nextAppWake(uint32_t now) const { return 0; }

public:
  void begin();
  void loop();

  Packet* obtainNewPacket();
  void releasePacket(Packet* packet);
  void sendPacket(Packet* packet, uint8_t priority, uint32_t delay_millis=0);

  unsigned long getTotalAirTime() const { return total_air_time; }
  unsigned long getReceiveAirTime() const {return rx_air_time; }
  unsigned long getRemainingTxBudget() const { return tx_budget_ms; }
  uint32_t getNumSentFlood() const { return n_sent_flood; }
  uint32_t getNumSentDirect() const { return n_sent_direct; }
  uint32_t getNumRecvFlood() const { return n_recv_flood; }
  uint32_t getNumRecvDirect() const { return n_recv_direct; }
  uint32_t getNumTxStartFails() const { return n_tx_start_fails; }
  void resetStats() {
    n_sent_flood = n_sent_direct = n_recv_flood = n_recv_direct = 0;
    n_tx_start_fails = 0;
    _err_flags = 0;
  }

  // helper methods
  bool millisHasNowPassed(unsigned long timestamp) const;
  unsigned long futureMillis(int millis_from_now) const;

  bool tryParsePacket(Packet* pkt, const uint8_t* raw, int len);

private:
  void checkRecv();
  void checkSend();
};

}
