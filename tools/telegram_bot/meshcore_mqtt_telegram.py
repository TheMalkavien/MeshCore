#!/usr/bin/env python3
"""
MeshCore → Telegram relay bot

Subscribes to a MeshCore MQTT bridge (mode packets) and forwards decoded
channel text messages to a Telegram chat.

The MeshCore device/repeater must have WITH_MQTT_BRIDGE firmware and
packets mode enabled (set mqtt.packets on).

MQTT topic:  meshcore/{iata}/{device_id}/packets
Payload:     JSON with fields origin, packet_type, direction, raw, hash, ...
             packet_type "5" = PAYLOAD_TYPE_GRP_TXT (group channel message)
             raw = hex-encoded raw LoRa packet (AES-128-ECB encrypted payload)
             hash = unique packet fingerprint for deduplication

Features:
  - Automatic MQTT reconnection with exponential backoff
  - Telegram retry on transient errors (rate-limit aware)
  - Deduplication by packet hash (same packet heard by multiple nodes)
  - Per-sender throttling (configurable cooldown between messages from same sender)
  - Global throttling (configurable minimum interval between any two forwarded messages)
  - Periodic stats logging

Usage:
    pip install -r requirements.txt
    python meshcore_mqtt_telegram.py \\
        --mqtt-host broker.local \\
        --channel-psk izOH6cXN6mrJ5e26oRXNcg== \\
        --telegram-token 123456:AAABBBCCC \\
        --telegram-chat -100123456789

Channel PSK: base64 (from MeshCore app export) or hex (16 bytes).
"""

import argparse
import base64
import hashlib
import hmac as _hmac
import json
import logging
import threading
import time
from collections import OrderedDict
from typing import Dict, Optional, Tuple

import paho.mqtt.client as mqtt
import requests
from Crypto.Cipher import AES

# ─── MeshCore protocol constants (src/MeshCore.h + src/Packet.h) ──────────────

PATH_HASH_SIZE       = 1    # bytes: truncated SHA-256 of channel secret
CIPHER_MAC_SIZE      = 2    # bytes: leading HMAC-SHA256 truncated to 2 bytes
CIPHER_KEY_SIZE      = 16   # bytes: AES-128 key size
PUB_KEY_SIZE         = 32   # bytes: full GroupChannel::secret array size
CIPHER_BLOCK_SIZE    = 16   # bytes: AES block size
PAYLOAD_TYPE_GRP_TXT = 0x05 # group channel text message

PH_ROUTE_MASK  = 0x03
PH_TYPE_SHIFT  = 2
PH_TYPE_MASK   = 0x0F

# Route types that carry 4-byte transport codes in the wire format
ROUTE_TRANSPORT_FLOOD  = 0x00
ROUTE_TRANSPORT_DIRECT = 0x03

# Telegram API
TG_API_BASE = "https://api.telegram.org/bot"
TG_MAX_RETRIES     = 3
TG_RETRY_BASE_SECS = 2

# MQTT reconnect backoff
MQTT_INITIAL_RECONNECT_DELAY = 2
MQTT_MAX_RECONNECT_DELAY     = 120

log = logging.getLogger("meshcore_tg")


# ─── Crypto (mirrors src/Utils.cpp) ───────────────────────────────────────────

def psk_to_bytes(psk_str: str) -> bytes:
    """Accept 16-byte PSK as base64 or hex, return raw 16 bytes."""
    s = psk_str.strip()
    for decoder in (
        lambda v: base64.b64decode(v),
        lambda v: bytes.fromhex(v),
    ):
        try:
            b = decoder(s)
            if len(b) in (16, 32):
                return b[:16]
        except Exception:
            pass
    raise ValueError(
        f"Cannot parse PSK {s!r}. "
        "Expected 16 bytes as base64 (e.g. izOH6cXN6mrJ5e26oRXNcg==) or 32 hex chars."
    )


def make_secret_32(psk_16: bytes) -> bytes:
    """Pad 16-byte PSK to 32 bytes (upper half zeros) as stored in GroupChannel::secret."""
    return psk_16 + bytes(16)


def compute_channel_hash(psk_16: bytes) -> int:
    """
    channel.hash = SHA-256(secret[:16])[0]
    Mirrors: Utils::sha256(hash, PATH_HASH_SIZE=1, secret, 16)
    """
    return hashlib.sha256(psk_16).digest()[0]


def mac_then_decrypt(secret_32: bytes, mac_and_ciphertext: bytes) -> Optional[bytes]:
    """
    Mirrors Utils::MACThenDecrypt:
      1. HMAC-SHA256(key=secret_32, data=ciphertext)[:2] must match the leading 2 bytes.
      2. AES-128-ECB decrypt each 16-byte block (key = secret_32[:16]).
    Returns plaintext (multiple of 16, zero-padded) or None on MAC failure.
    """
    if len(mac_and_ciphertext) <= CIPHER_MAC_SIZE:
        return None

    mac        = mac_and_ciphertext[:CIPHER_MAC_SIZE]
    ciphertext = mac_and_ciphertext[CIPHER_MAC_SIZE:]

    if not ciphertext or len(ciphertext) % CIPHER_BLOCK_SIZE != 0:
        return None

    expected = _hmac.new(secret_32, ciphertext, hashlib.sha256).digest()[:CIPHER_MAC_SIZE]
    if not _hmac.compare_digest(mac, expected):
        return None

    cipher = AES.new(secret_32[:CIPHER_KEY_SIZE], AES.MODE_ECB)
    return b"".join(
        cipher.decrypt(ciphertext[i : i + CIPHER_BLOCK_SIZE])
        for i in range(0, len(ciphertext), CIPHER_BLOCK_SIZE)
    )


# ─── Packet parsing (mirrors Packet::writeTo / Packet::readFrom) ──────────────

def parse_grp_txt_packet(
    raw_hex: str,
    secret_32: bytes,
    expected_hash_byte: int,
) -> Optional[Tuple[str, str]]:
    """
    Parse and decrypt a raw GRP_TXT LoRa packet from its hex representation.

    Wire layout (Packet::writeTo):
        [0]     header: route(bits 0-1) | payload_type(bits 2-5) | ver(bits 6-7)
        [1:5]   transport_codes (only when route is TRANSPORT_FLOOD or TRANSPORT_DIRECT)
        [next]  path_len byte: hash_count(bits 0-5) | (hash_size-1)(bits 6-7)
        [path]  path_hash_count × path_hash_size bytes
        [rest]  GRP_TXT payload:
                  [0]    channel_hash byte  (SHA-256(secret[:16])[0])
                  [1:3]  HMAC-SHA256[:2]
                  [3:]   AES-128-ECB ciphertext
                Decrypted (BaseChatMesh::sendGroupMessage):
                  [0:4]  timestamp (uint32 LE, not forwarded)
                  [4:]   "sender_name: message_text"  (null-padded)

    Returns (sender, message) or None.
    """
    try:
        raw = bytes.fromhex(raw_hex)
    except ValueError:
        return None

    if len(raw) < 3:
        return None

    idx        = 0
    header     = raw[idx]; idx += 1
    route_type = header & PH_ROUTE_MASK
    pkt_type   = (header >> PH_TYPE_SHIFT) & PH_TYPE_MASK

    if pkt_type != PAYLOAD_TYPE_GRP_TXT:
        return None

    if route_type in (ROUTE_TRANSPORT_FLOOD, ROUTE_TRANSPORT_DIRECT):
        if idx + 4 > len(raw):
            return None
        idx += 4  # skip transport_codes[0] + transport_codes[1]

    if idx >= len(raw):
        return None

    path_len_byte   = raw[idx]; idx += 1
    path_hash_count = path_len_byte & 0x3F
    path_hash_size  = (path_len_byte >> 6) + 1
    path_byte_len   = path_hash_count * path_hash_size

    if idx + path_byte_len > len(raw):
        return None
    idx += path_byte_len

    payload = raw[idx:]

    if len(payload) < PATH_HASH_SIZE + CIPHER_MAC_SIZE + CIPHER_BLOCK_SIZE:
        return None

    if payload[0] != expected_hash_byte:
        return None  # different channel

    plaintext = mac_then_decrypt(secret_32, payload[PATH_HASH_SIZE:])
    if plaintext is None:
        return None  # wrong PSK or hash collision

    # Plaintext layout: [0:4] timestamp (LE uint32) | [4] txt_type (0=PLAIN) | [5:] "sender: msg"
    # onChannelMessageRecv receives &data[5], so we must skip 5 bytes, not 4.
    if len(plaintext) < 6:
        return None

    text = plaintext[5:].rstrip(b"\x00").decode("utf-8", errors="replace")
    sep  = text.find(": ")
    if sep >= 0:
        return text[:sep], text[sep + 2:]
    return "?", text


# ─── Dedup cache ───────────────────────────────────────────────────────────────

class DedupCache:
    """
    Time-bounded set of packet hashes.
    Entries expire after `ttl` seconds; the cache is lazily pruned.
    """

    def __init__(self, ttl: float):
        self._ttl   = ttl
        self._cache: OrderedDict[str, float] = OrderedDict()  # hash → first_seen
        self._lock  = threading.Lock()
        self._last_prune = time.monotonic()

    def is_duplicate(self, pkt_hash: str) -> bool:
        """Return True if this hash was seen within the TTL window (and record it if new)."""
        now = time.monotonic()
        with self._lock:
            self._maybe_prune(now)
            if pkt_hash in self._cache:
                return True
            self._cache[pkt_hash] = now
            return False

    def _maybe_prune(self, now: float) -> None:
        if now - self._last_prune < 30:
            return
        self._last_prune = now
        cutoff  = now - self._ttl
        expired = [h for h, t in self._cache.items() if t < cutoff]
        for h in expired:
            del self._cache[h]
        if expired:
            log.debug(f"[dedup] pruned {len(expired)} expired entries, {len(self._cache)} remain")

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._cache)


# ─── Throttle ─────────────────────────────────────────────────────────────────

class Throttle:
    """
    Per-sender and global message rate limiter.

    per_sender_secs : minimum gap between two messages from the same sender (0 = off)
    global_secs     : minimum gap between any two forwarded messages          (0 = off)
    """

    def __init__(self, per_sender_secs: float = 0.0, global_secs: float = 0.0):
        self._per_sender = per_sender_secs
        self._global     = global_secs
        self._sender_ts: Dict[str, float] = {}
        self._global_ts: float = 0.0
        self._lock = threading.Lock()

    def check(self, sender: str) -> Tuple[bool, str]:
        """
        Return (allowed, reason).
        reason is empty string when allowed, otherwise a human-readable explanation.
        """
        if self._per_sender <= 0 and self._global <= 0:
            return True, ""

        now = time.monotonic()
        with self._lock:
            if self._global > 0:
                gap = now - self._global_ts
                if gap < self._global:
                    wait = self._global - gap
                    return False, f"global throttle, {wait:.1f}s remaining"

            if self._per_sender > 0:
                last = self._sender_ts.get(sender, 0.0)
                gap  = now - last
                if gap < self._per_sender:
                    wait = self._per_sender - gap
                    return False, f"sender throttle for {sender!r}, {wait:.1f}s remaining"

            # Allow — record timestamps
            if self._global > 0:
                self._global_ts = now
            if self._per_sender > 0:
                self._sender_ts[sender] = now

            return True, ""

    def cleanup_stale_senders(self) -> int:
        """Remove senders whose cooldown has long expired (> 10× per_sender period)."""
        if self._per_sender <= 0:
            return 0
        cutoff = time.monotonic() - self._per_sender * 10
        with self._lock:
            stale = [s for s, t in self._sender_ts.items() if t < cutoff]
            for s in stale:
                del self._sender_ts[s]
            return len(stale)


# ─── Telegram sender ──────────────────────────────────────────────────────────

class TelegramSender:
    """Thread-safe Telegram message sender with retry and rate-limit handling.

    chat_id may be:
      "-1002252977991"          plain supergroup / channel
      "1002252977991_239175"    supergroup forum topic  (CHAT_ID_THREAD_ID)
      "-1002252977991_239175"   same with explicit minus sign

    For the CHAT_ID_THREAD_ID form the underscore separates the chat ID from
    the message_thread_id (Telegram forum topic).  If the chat_id part is
    positive it is sent as-is; prepend '-' manually if needed.
    """

    def __init__(self, token: str, chat_id: str):
        self._url     = f"{TG_API_BASE}{token}/sendMessage"
        self._session = requests.Session()

        # Parse optional CHAT_ID_THREAD_ID format
        if "_" in chat_id:
            # Split on the LAST underscore to handle negative IDs like -1002252977991_239
            last_sep = chat_id.rfind("_")
            self._chat_id   = chat_id[:last_sep]
            self._thread_id: Optional[int] = int(chat_id[last_sep + 1:])
        else:
            self._chat_id   = chat_id
            self._thread_id = None

        # Telegram supergroup/channel IDs are always negative in the Bot API.
        # Users often copy them without the leading minus (e.g. "1002252977991"
        # instead of "-1002252977991").  Auto-fix: if it looks like a bare
        # supergroup ID (all digits, no leading minus) prepend "-".
        cid = self._chat_id
        if cid.lstrip("0123456789") == "" and len(cid) > 10:
            self._chat_id = "-" + cid
            log.warning(
                f"[telegram] chat_id {cid!r} looks like a supergroup ID without "
                f"the leading minus — using {self._chat_id!r} instead. "
                "Pass it with an explicit '-' to silence this warning."
            )

        thread_info = f", thread_id={self._thread_id}" if self._thread_id else ""
        log.info(f"[telegram] target: chat_id={self._chat_id!r}{thread_info}")

    def send(self, text: str) -> bool:
        """Send a message, retrying up to TG_MAX_RETRIES times. Returns True on success."""
        payload: dict = {"chat_id": self._chat_id, "text": text}
        if self._thread_id is not None:
            payload["message_thread_id"] = self._thread_id

        for attempt in range(TG_MAX_RETRIES):
            try:
                resp = self._session.post(
                    self._url,
                    json=payload,
                    timeout=15,
                )
            except requests.RequestException as exc:
                delay = TG_RETRY_BASE_SECS ** attempt
                log.warning(
                    f"[telegram] send attempt {attempt + 1}/{TG_MAX_RETRIES} failed: {exc}"
                    + (f", retrying in {delay:.0f}s" if attempt + 1 < TG_MAX_RETRIES else "")
                )
                if attempt + 1 < TG_MAX_RETRIES:
                    time.sleep(delay)
                continue

            if resp.status_code == 429:
                # Telegram rate limit: respect the retry_after header
                try:
                    retry_after = int(resp.json()["parameters"]["retry_after"])
                except Exception:
                    retry_after = 30
                log.warning(f"[telegram] rate-limited, waiting {retry_after}s before retry")
                time.sleep(retry_after)
                continue

            if resp.ok:
                log.debug(f"[telegram] message sent OK (attempt {attempt + 1})")
                return True

            log.error(
                f"[telegram] API error {resp.status_code} (attempt {attempt + 1}/{TG_MAX_RETRIES}): "
                f"{resp.text[:200]}"
            )
            # 4xx errors (except 429) are not retryable
            if 400 <= resp.status_code < 500:
                return False

        log.error(f"[telegram] gave up after {TG_MAX_RETRIES} attempts")
        return False


# ─── Stats ────────────────────────────────────────────────────────────────────

class Stats:
    def __init__(self):
        self._lock = threading.Lock()
        self.reset()

    def reset(self):
        with getattr(self, "_lock", threading.Lock()):
            self.mqtt_connects       = 0
            self.mqtt_disconnects    = 0
            self.packets_received    = 0  # GRP_TXT rx, our channel hash
            self.deduplicated        = 0
            self.throttled           = 0
            self.forwarded           = 0
            self.telegram_errors     = 0
            self._period_start       = time.monotonic()

    def inc(self, field: str, n: int = 1):
        with self._lock:
            setattr(self, field, getattr(self, field) + n)

    def snapshot(self) -> dict:
        with self._lock:
            elapsed = time.monotonic() - self._period_start
            return {
                "uptime_s":          round(elapsed),
                "mqtt_connects":     self.mqtt_connects,
                "mqtt_disconnects":  self.mqtt_disconnects,
                "packets_received":  self.packets_received,
                "deduplicated":      self.deduplicated,
                "throttled":         self.throttled,
                "forwarded":         self.forwarded,
                "telegram_errors":   self.telegram_errors,
            }


# ─── MQTT bot ─────────────────────────────────────────────────────────────────

class MeshCoreMQTTBot:
    def __init__(
        self,
        mqtt_host: str,
        mqtt_port: int,
        mqtt_user: Optional[str],
        mqtt_password: Optional[str],
        mqtt_topic: str,
        psk_16: bytes,
        telegram_token: str,
        telegram_chat: str,
        dedup_ttl: float        = 120.0,
        throttle_per_sender: float = 0.0,
        throttle_global: float  = 0.0,
        stats_interval: float   = 600.0,
    ):
        self._host  = mqtt_host
        self._port  = mqtt_port
        self._user  = mqtt_user
        self._pass  = mqtt_password
        self._topic = mqtt_topic

        self._secret_32 = make_secret_32(psk_16)
        self._ch_hash   = compute_channel_hash(psk_16)

        self._tg     = TelegramSender(telegram_token, telegram_chat)
        self._dedup  = DedupCache(ttl=dedup_ttl)
        self._throttle = Throttle(per_sender_secs=throttle_per_sender,
                                  global_secs=throttle_global)
        self._stats  = Stats()
        self._stats_interval = stats_interval
        self._stats_timer: Optional[threading.Timer] = None

        log.info(
            f"Config — channel hash: 0x{self._ch_hash:02X} | "
            f"dedup TTL: {dedup_ttl}s | "
            f"throttle per-sender: {throttle_per_sender}s | "
            f"throttle global: {throttle_global}s"
        )

    # ── Stats logging ─────────────────────────────────────────────────────────

    def _schedule_stats(self) -> None:
        if self._stats_interval <= 0:
            return
        self._stats_timer = threading.Timer(self._stats_interval, self._emit_stats)
        self._stats_timer.daemon = True
        self._stats_timer.start()

    def _emit_stats(self) -> None:
        s = self._stats.snapshot()
        log.info(
            f"[stats] uptime={s['uptime_s']}s | "
            f"received={s['packets_received']} | "
            f"dedup={s['deduplicated']} | "
            f"throttled={s['throttled']} | "
            f"forwarded={s['forwarded']} | "
            f"tg_errors={s['telegram_errors']} | "
            f"mqtt_reconnects={s['mqtt_disconnects']}"
        )
        self._throttle.cleanup_stale_senders()
        self._schedule_stats()  # reschedule

    # ── MQTT callbacks ────────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, rc) -> None:
        RC_NAMES = {
            0: "OK",
            1: "bad protocol",
            2: "client ID rejected",
            3: "broker unavailable",
            4: "bad credentials",
            5: "not authorised",
        }
        if rc == 0:
            self._stats.inc("mqtt_connects")
            log.info(f"[mqtt] connected to {self._host}:{self._port}")
            result, mid = client.subscribe(self._topic)
            if result == mqtt.MQTT_ERR_SUCCESS:
                log.info(f"[mqtt] subscribed to {self._topic!r} (mid={mid})")
            else:
                log.error(f"[mqtt] subscribe failed (result={result})")
        else:
            reason = RC_NAMES.get(rc, f"code {rc}")
            log.error(f"[mqtt] connection refused: {reason}")

    def _on_disconnect(self, client, userdata, rc) -> None:
        self._stats.inc("mqtt_disconnects")
        if rc == 0:
            log.info("[mqtt] disconnected cleanly")
        else:
            log.warning(
                f"[mqtt] unexpected disconnect (rc={rc}), "
                "paho will reconnect automatically"
            )

    def _on_subscribe(self, _client, _userdata, mid, granted_qos) -> None:
        log.debug(f"[mqtt] subscription confirmed mid={mid} qos={granted_qos}")

    def _on_message(self, client, userdata, msg) -> None:
        # ── Parse JSON ────────────────────────────────────────────────────────
        try:
            data = json.loads(msg.payload)
        except (ValueError, UnicodeDecodeError) as exc:
            log.debug(f"[mqtt] non-JSON on {msg.topic}: {exc}")
            return

        # Only PACKET frames (type field discriminates from status/raw messages)
        if data.get("type") != "PACKET":
            return

        # Only RX direction (ignore TX echoes from the bridge node itself)
        if data.get("direction") != "rx":
            return

        # Only GRP_TXT (packet_type "5")
        if str(data.get("packet_type", "")) != str(PAYLOAD_TYPE_GRP_TXT):
            return

        raw_hex  = data.get("raw", "")
        pkt_hash = data.get("hash", "").upper()
        origin   = data.get("origin", "?")
        snr      = data.get("SNR", "?")
        rssi     = data.get("RSSI", "?")

        if not raw_hex:
            return

        log.debug(
            f"[mqtt] GRP_TXT from node {origin!r} | "
            f"hash={pkt_hash} SNR={snr} RSSI={rssi} | "
            f"topic={msg.topic}"
        )

        # ── Packet decryption ─────────────────────────────────────────────────
        result = parse_grp_txt_packet(raw_hex, self._secret_32, self._ch_hash)
        if result is None:
            # Either different channel, wrong PSK, or malformed — all silent at DEBUG
            log.debug(f"[mqtt] packet {pkt_hash}: not our channel or decrypt failed")
            return

        sender, message = result
        self._stats.inc("packets_received")

        # ── Deduplication ─────────────────────────────────────────────────────
        if pkt_hash and self._dedup.is_duplicate(pkt_hash):
            self._stats.inc("deduplicated")
            log.info(
                f"[dedup] SKIP {pkt_hash} from {sender!r} "
                f"(already seen, heard again by {origin!r})"
            )
            return

        # ── Throttling ────────────────────────────────────────────────────────
        allowed, reason = self._throttle.check(sender)
        if not allowed:
            self._stats.inc("throttled")
            log.info(f"[throttle] SKIP message from {sender!r}: {reason}")
            return

        # ── Forward to Telegram ───────────────────────────────────────────────
        tg_text = f"[{sender}] : {message}"
        log.info(
            f"[forward] [{sender}] : {message!r} "
            f"(hash={pkt_hash}, SNR={snr}, RSSI={rssi})"
        )

        if self._tg.send(tg_text):
            self._stats.inc("forwarded")
        else:
            self._stats.inc("telegram_errors")
            log.error(
                f"[telegram] failed to forward message from {sender!r}: {message!r}"
            )

    # ── Main run loop ─────────────────────────────────────────────────────────

    def run(self) -> None:
        self._schedule_stats()

        client = mqtt.Client(client_id="meshcore_tg_bot", clean_session=True)
        if self._user:
            client.username_pw_set(self._user, self._pass)

        client.on_connect    = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_subscribe  = self._on_subscribe
        client.on_message    = self._on_message

        # paho will reconnect automatically after a disconnect
        client.reconnect_delay_set(
            min_delay=MQTT_INITIAL_RECONNECT_DELAY,
            max_delay=MQTT_MAX_RECONNECT_DELAY,
        )

        # Initial connection with exponential backoff
        delay = MQTT_INITIAL_RECONNECT_DELAY
        while True:
            try:
                log.info(f"[mqtt] connecting to {self._host}:{self._port}…")
                client.connect(self._host, self._port, keepalive=60)
                break
            except (OSError, ConnectionRefusedError) as exc:
                log.warning(
                    f"[mqtt] initial connection failed: {exc} — "
                    f"retrying in {delay}s"
                )
                time.sleep(delay)
                delay = min(delay * 2, MQTT_MAX_RECONNECT_DELAY)

        # Blocking loop — paho handles reconnection transparently
        try:
            client.loop_forever()
        except KeyboardInterrupt:
            log.info("[mqtt] shutdown requested")
        finally:
            if self._stats_timer:
                self._stats_timer.cancel()
            client.disconnect()
            self._emit_stats()
            log.info("[mqtt] disconnected")


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        description="Forward MeshCore channel messages from MQTT to Telegram",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
PREREQUISITES
  The MeshCore device/repeater must run WITH_MQTT_BRIDGE firmware with:
    set mqtt.server  <broker_host>
    set mqtt.packets on
  Published topic: meshcore/{iata}/{device_id}/packets

CHANNEL PSK (--channel-psk)
  Base64 (from MeshCore companion app channel export):
    izOH6cXN6mrJ5e26oRXNcg==
  Hex (32 hex chars = 16 bytes):
    8b338f9e5cd27ac4...

THROTTLING
  --throttle-per-sender 30   → one message per sender per 30 s
  --throttle-global 5        → at most one message every 5 s total

DEDUPLICATION
  --dedup-ttl 120            → ignore duplicate hashes seen within 120 s
  (A packet heard by 3 nodes arrives 3 times with the same hash field.)

EXAMPLES
  # Minimal
  python meshcore_mqtt_telegram.py \\
      --mqtt-host 192.168.1.10 \\
      --channel-psk izOH6cXN6mrJ5e26oRXNcg== \\
      --telegram-token 7123456789:AAExxxxxx \\
      --telegram-chat -100123456789

  # With auth, throttle, custom topic
  python meshcore_mqtt_telegram.py \\
      --mqtt-host broker.example.com --mqtt-port 8883 \\
      --mqtt-user alice --mqtt-password s3cr3t \\
      --mqtt-topic "meshcore/CDG/+/packets" \\
      --channel-psk izOH6cXN6mrJ5e26oRXNcg== \\
      --telegram-token 7123456789:AAExxxxxx \\
      --telegram-chat @my_channel \\
      --throttle-per-sender 30 \\
      --throttle-global 5 \\
      --dedup-ttl 180 \\
      --debug
""",
    )

    # MQTT
    p.add_argument("--mqtt-host", required=True,
                   help="MQTT broker hostname or IP address")
    p.add_argument("--mqtt-port", type=int, default=1883,
                   help="MQTT broker port (default: 1883)")
    p.add_argument("--mqtt-user", default=None,
                   help="MQTT username (optional)")
    p.add_argument("--mqtt-password", default=None,
                   help="MQTT password (optional)")
    p.add_argument("--mqtt-topic", default="meshcore/+/+/packets",
                   help="MQTT topic pattern (default: meshcore/+/+/packets)")

    # Channel
    p.add_argument("--channel-psk", required=True,
                   help="Channel PSK as base64 or 32-char hex (16 bytes)")

    # Telegram
    p.add_argument("--telegram-token", required=True,
                   help="Telegram Bot API token (from @BotFather)")
    p.add_argument("--telegram-chat", required=True,
                   help=(
                       "Telegram chat ID: plain group/channel (-1002252977991), "
                       "@username, or CHAT_ID_THREAD_ID for forum topics "
                       "(e.g. 1002252977991_239175)"
                   ))

    # Deduplication
    p.add_argument("--dedup-ttl", type=float, default=120.0, metavar="SECONDS",
                   help="Packet hash TTL for deduplication (default: 120s, 0 to disable)")

    # Throttling
    p.add_argument("--throttle-per-sender", type=float, default=0.0, metavar="SECONDS",
                   help="Min seconds between messages from the same sender (default: 0 = off)")
    p.add_argument("--throttle-global", type=float, default=0.0, metavar="SECONDS",
                   help="Min seconds between any two forwarded messages (default: 0 = off)")

    # Diagnostics
    p.add_argument("--stats-interval", type=float, default=600.0, metavar="SECONDS",
                   help="How often to log periodic stats (default: 600s, 0 to disable)")
    p.add_argument("--debug", action="store_true",
                   help="Enable DEBUG logging (very verbose)")

    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        psk_16 = psk_to_bytes(args.channel_psk)
    except ValueError as exc:
        log.error(str(exc))
        raise SystemExit(1)

    bot = MeshCoreMQTTBot(
        mqtt_host           = args.mqtt_host,
        mqtt_port           = args.mqtt_port,
        mqtt_user           = args.mqtt_user,
        mqtt_password       = args.mqtt_password,
        mqtt_topic          = args.mqtt_topic,
        psk_16              = psk_16,
        telegram_token      = args.telegram_token,
        telegram_chat       = args.telegram_chat,
        dedup_ttl           = args.dedup_ttl,
        throttle_per_sender = args.throttle_per_sender,
        throttle_global     = args.throttle_global,
        stats_interval      = args.stats_interval,
    )
    bot.run()


if __name__ == "__main__":
    main()
