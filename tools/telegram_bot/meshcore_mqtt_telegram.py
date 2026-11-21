#!/usr/bin/env python3
"""
MeshCore → Telegram relay bot

Subscribes to a MeshCore MQTT bridge (mode packets) and to optional decoded
Home Assistant channel-message events, then forwards channel text messages to
one or more Telegram chats.

The MeshCore device/repeater must have WITH_MQTT_BRIDGE firmware and
packets mode enabled (set mqtt.packets on).

MQTT topic:  meshcore/{iata}/{device_id}/packets
Payload:     JSON with fields origin, packet_type, direction, raw, hash, ...
             packet_type "5" = PAYLOAD_TYPE_GRP_TXT (group channel message)
             raw = hex-encoded raw LoRa packet (AES-128-ECB encrypted payload)
             hash = unique packet fingerprint for deduplication

Fallback topic: meshcore/{iata}/{device_id}/messages
Payload:        JSON with type "MESSAGE", direction "rx", message_type
                "channel", channel/channel_idx, sender, message and timestamp.
                This path keeps Telegram delivery working when the companion
                emits the decoded message but no RX_LOG_DATA/raw packet.

Features:
  - Automatic MQTT reconnection with exponential backoff
  - Telegram retry on transient errors (rate-limit aware)
  - Deduplication by packet hash (same packet heard by multiple nodes)
  - Unlimited MeshCore channel PSK -> Telegram chat mappings
  - JSON configuration file plus repeatable command-line mappings
  - Per-sender throttling (configurable cooldown between messages from same sender)
  - Global throttling (configurable minimum interval between any two forwarded messages)
  - Periodic stats logging

Usage:
    pip install -r requirements.txt
    python meshcore_mqtt_telegram.py \\
        --mqtt-host broker.local \\
        --telegram-token 123456:AAABBBCCC \\
        --channel-map 'izOH6cXN6mrJ5e26oRXNcg==:-100123456789' \\
        --channel-map '00112233445566778899aabbccddeeff:@another_channel'

The legacy --channel-psk/--telegram-chat pair remains supported.

Channel PSK: base64 (from MeshCore app export) or hex (16 bytes).
"""

import argparse
import base64
import hashlib
import hmac as _hmac
import json
import logging
import queue
import threading
import time
import unicodedata
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
        lambda v: base64.b64decode(v, validate=True),
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
) -> Optional[Tuple[str, str, str, int]]:
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
                  [4]    txt_type  (0 = PLAIN, skip)
                  [5:]   "sender_name: message_text"  (null-padded)

    Returns (sender, message, stable_id, sender_timestamp) or None.

    stable_id is a SHA-256 fingerprint of the MAC+ciphertext.  Unlike the
    'hash' field in the MQTT JSON (which uses Packet::calculatePacketHash and
    includes path_len — a value that changes at every relay hop and may also
    reflect a recycled packet-pool pointer in the MQTT bridge), this fingerprint
    is identical for the same logical message regardless of which relay heard it
    or at which hop count.
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

    mac_and_cipher = payload[PATH_HASH_SIZE:]
    plaintext = mac_then_decrypt(secret_32, mac_and_cipher)
    if plaintext is None:
        return None  # wrong PSK or hash collision

    # Stable cross-hop dedup ID: SHA-256 of MAC+ciphertext (hop-invariant).
    # The JSON 'hash' field is NOT used because Packet::calculatePacketHash()
    # mixes in path_len which increments at every relay hop.
    stable_id = hashlib.sha256(mac_and_cipher).hexdigest()[:16].upper()

    # Plaintext layout: [0:4] timestamp (LE uint32) | [4] txt_type (0=PLAIN) | [5:] "sender: msg"
    # onChannelMessageRecv receives &data[5], so we must skip 5 bytes, not 4.
    if len(plaintext) < 6:
        return None

    # The low two bits are the retry attempt. Only the upper bits identify the
    # text type, so values 0..3 are all plain channel text.
    if (plaintext[4] >> 2) != 0:
        return None

    sender_timestamp = int.from_bytes(plaintext[:4], byteorder="little")
    text = plaintext[5:].rstrip(b"\x00").decode("utf-8", errors="replace")
    sep  = text.find(": ")
    if sep >= 0:
        return text[:sep], text[sep + 2:], stable_id, sender_timestamp
    return "?", text, stable_id, sender_timestamp


def normalize_sender_timestamp(value: object) -> Optional[int]:
    """Normalize a decoded HA event timestamp to a non-negative Unix integer."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        timestamp = int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None
    return timestamp if timestamp >= 0 else None


def make_message_dedup_id(
    channel_hash: int,
    sender_timestamp: Optional[int],
    sender: str,
    message: str,
) -> str:
    """Build one ID shared by raw packets and decoded-message fallbacks."""
    timestamp = sender_timestamp if sender_timestamp is not None else 0
    identity = bytearray(b"meshcore-grp-txt-v1\x00")
    identity.append(channel_hash & 0xFF)
    identity.extend(timestamp.to_bytes(8, byteorder="little", signed=False))
    identity.extend(sender.encode("utf-8", errors="replace"))
    identity.append(0)
    identity.extend(message.encode("utf-8", errors="replace"))
    return hashlib.sha256(identity).hexdigest()[:16].upper()


def make_message_content_id(channel_hash: int, sender: str, message: str) -> str:
    """Build a short-lived fallback ID when producers disagree on timestamp."""
    identity = bytearray(b"meshcore-grp-txt-content-v1\x00")
    identity.append(channel_hash & 0xFF)
    identity.extend(
        unicodedata.normalize("NFC", sender).encode("utf-8", errors="replace")
    )
    identity.append(0)
    identity.extend(
        unicodedata.normalize("NFC", message).encode("utf-8", errors="replace")
    )
    return hashlib.sha256(identity).hexdigest()[:16].upper()


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
        if self._ttl <= 0:
            return False
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
                # NB: log the exception *type* only — str(exc) embeds the request URL,
                # which contains the bot token.
                log.warning(
                    f"[telegram] send attempt {attempt + 1}/{TG_MAX_RETRIES} failed: {type(exc).__name__}"
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


class ChannelMapping:
    """Validated configuration for one MeshCore channel -> Telegram target."""

    def __init__(
        self,
        psk_16: bytes,
        telegram_chat: str,
        name: str,
        channel_name: Optional[str] = None,
        channel_idx: Optional[int] = None,
    ):
        self.psk_16 = psk_16
        self.telegram_chat = telegram_chat
        self.name = name
        self.channel_name = channel_name if channel_name is not None else name
        self.channel_idx = channel_idx


class ChannelRoute:
    """Runtime state derived from a ChannelMapping."""

    def __init__(
        self,
        mapping: ChannelMapping,
        telegram_token: str,
    ):
        self.name = mapping.name
        self.telegram_chat = mapping.telegram_chat
        self.secret_32 = make_secret_32(mapping.psk_16)
        self.channel_hash = compute_channel_hash(mapping.psk_16)
        self.channel_name = mapping.channel_name
        self.channel_idx = mapping.channel_idx
        self.telegram = TelegramSender(telegram_token, mapping.telegram_chat)

    def matches_decoded_channel(
        self,
        channel_name: str,
        channel_idx: Optional[int],
    ) -> bool:
        """Match a plaintext HA channel event to this configured route."""
        if self.channel_idx is not None:
            if channel_idx is None or channel_idx != self.channel_idx:
                return False
        if self.channel_name:
            if not channel_name:
                return self.channel_idx is not None
            return channel_name.casefold() == self.channel_name.casefold()
        return self.channel_idx is not None


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
            self.decoded_fallbacks   = 0
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
                "decoded_fallbacks": self.decoded_fallbacks,
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
        telegram_token: str,
        channel_mappings: List[ChannelMapping],
        mqtt_message_topic: str = "meshcore/+/+/messages",
        dedup_ttl: float        = 120.0,
        content_dedup_ttl: float = 5.0,
        throttle_per_sender: float = 0.0,
        throttle_global: float  = 0.0,
        stats_interval: float   = 600.0,
    ):
        self._host  = mqtt_host
        self._port  = mqtt_port
        self._user  = mqtt_user
        self._pass  = mqtt_password
        self._topic = mqtt_topic
        self._message_topic = mqtt_message_topic

        self._routes = [
            ChannelRoute(mapping, telegram_token)
            for mapping in channel_mappings
        ]
        self._dedup  = DedupCache(ttl=dedup_ttl)
        self._content_dedup = DedupCache(ttl=content_dedup_ttl)
        self._throttle = Throttle(per_sender_secs=throttle_per_sender,
                                  global_secs=throttle_global)
        self._stats  = Stats()
        self._stats_interval = stats_interval
        self._stats_timer: Optional[threading.Timer] = None

        # Telegram is delivered from a dedicated worker thread so its retry/rate-limit
        # sleeps never block the MQTT network thread (which must keep servicing keepalive).
        self._tg_queue: "queue.Queue" = queue.Queue(maxsize=1000)
        self._tg_worker: Optional[threading.Thread] = None

        log.info(
            f"Config — {len(self._routes)} channel mapping(s) | "
            f"dedup TTL: {dedup_ttl}s | "
            f"content dedup TTL: {content_dedup_ttl}s | "
            f"throttle per-sender: {throttle_per_sender}s | "
            f"throttle global: {throttle_global}s"
        )
        for route in self._routes:
            log.info(
                f"[route:{route.name}] channel hash 0x{route.channel_hash:02X} "
                f"| decoded channel={route.channel_name!r}"
                f"{f' idx={route.channel_idx}' if route.channel_idx is not None else ''} "
                f"-> Telegram {route.telegram_chat!r}"
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
            f"decoded_fallbacks={s['decoded_fallbacks']} | "
            f"dedup={s['deduplicated']} | "
            f"throttled={s['throttled']} | "
            f"forwarded={s['forwarded']} | "
            f"tg_errors={s['telegram_errors']} | "
            f"mqtt_disconnects={s['mqtt_disconnects']}"
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
            topics = list(dict.fromkeys((self._topic, self._message_topic)))
            for topic in topics:
                result, mid = client.subscribe(topic)
                if result == mqtt.MQTT_ERR_SUCCESS:
                    log.info(f"[mqtt] subscribed to {topic!r} (mid={mid})")
                else:
                    log.error(f"[mqtt] subscribe to {topic!r} failed (result={result})")
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

    def _tg_worker_loop(self) -> None:
        """Consume the Telegram send queue off the MQTT thread."""
        while True:
            item = self._tg_queue.get()
            if item is None:      # shutdown sentinel
                break
            route, sender, message, tg_text = item
            try:
                ok = route.telegram.send(tg_text)
            except Exception:
                log.exception(f"[telegram:{route.name}] unexpected error while sending")
                ok = False
            if ok:
                self._stats.inc("forwarded")
            else:
                self._stats.inc("telegram_errors")
                log.error(
                    f"[telegram:{route.name}] failed to forward message "
                    f"from {sender!r}: {message!r}"
                )

    def _on_message(self, client, userdata, msg) -> None:
        # A malformed publish must never escape into paho's loop (which would kill
        # the bot); swallow and log any unexpected error here.
        try:
            self._handle_message(msg)
        except Exception:
            log.exception("[mqtt] error handling message; ignoring")

    def _handle_message(self, msg) -> None:
        # ── Parse JSON ────────────────────────────────────────────────────────
        try:
            data = json.loads(msg.payload)
        except (ValueError, UnicodeDecodeError) as exc:
            log.debug(f"[mqtt] non-JSON on {msg.topic}: {exc}")
            return

        if data.get("type") == "MESSAGE":
            self._handle_decoded_channel_message(data, msg.topic)
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
        result = None
        matched_routes = []
        for route in self._routes:
            route_result = parse_grp_txt_packet(
                raw_hex,
                route.secret_32,
                route.channel_hash,
            )
            if route_result is not None:
                # Several mappings may intentionally use the same MeshCore PSK
                # to fan one channel out to several Telegram destinations.
                result = route_result
                matched_routes.append(route)

        if result is None:
            # Either a different channel, a wrong PSK, or a malformed packet.
            log.debug(
                f"[mqtt] packet mqtt_hash={pkt_hash}: "
                "no configured channel matched or decrypt failed"
            )
            return

        sender, message, _raw_stable_id, sender_timestamp = result
        self._stats.inc("packets_received")
        stable_id = make_message_dedup_id(
            matched_routes[0].channel_hash,
            sender_timestamp,
            sender,
            message,
        )
        content_id = make_message_content_id(
            matched_routes[0].channel_hash,
            sender,
            message,
        )

        # ── Deduplication ─────────────────────────────────────────────────────
        # The canonical ID is independent of the reception path and can also be
        # reproduced from the decoded Home Assistant fallback event.
        if (
            self._dedup.is_duplicate(stable_id)
            or self._content_dedup.is_duplicate(content_id)
        ):
            self._stats.inc("deduplicated")
            log.info(
                f"[dedup] SKIP id={stable_id} from {sender!r} "
                f"(already seen, heard again by node {origin!r})"
            )
            return

        # ── Throttling ────────────────────────────────────────────────────────
        allowed, reason = self._throttle.check(sender)
        if not allowed:
            self._stats.inc("throttled")
            log.info(f"[throttle] SKIP message from {sender!r}: {reason}")
            return

        # ── Forward to Telegram (async: hand off to the worker thread) ────────
        tg_text = f"[{sender}] : {message}"
        route_names = ", ".join(route.name for route in matched_routes)
        log.info(
            f"[forward:{route_names}] [{sender}] : {message!r} "
            f"(id={stable_id}, SNR={snr}, RSSI={rssi})"
        )

        for route in matched_routes:
            try:
                self._tg_queue.put_nowait((route, sender, message, tg_text))
            except queue.Full:
                self._stats.inc("telegram_errors")
                log.warning(
                    f"[telegram:{route.name}] send queue full — "
                    "dropping message (Telegram backlog?)"
                )

    # ── Main run loop ─────────────────────────────────────────────────────────

    def _handle_decoded_channel_message(self, data: dict, topic: str) -> None:
        """Forward a plaintext HA meshcore_message when its raw RX_LOG is absent."""
        if data.get("direction") != "rx" or data.get("outgoing") is True:
            return
        if str(data.get("message_type", "")).casefold() != "channel":
            return

        sender_value = data.get("sender", data.get("sender_name"))
        message_value = data.get("message")
        if not isinstance(sender_value, str) or not sender_value.strip():
            log.debug(f"[mqtt] decoded channel message without sender on {topic}")
            return
        if not isinstance(message_value, str):
            log.debug(f"[mqtt] decoded channel message without text on {topic}")
            return

        sender = sender_value.strip()
        message = message_value
        channel_name = str(data.get("channel", data.get("channel_name", ""))).strip()
        channel_idx = normalize_sender_timestamp(data.get("channel_idx"))
        matched_routes = [
            route
            for route in self._routes
            if route.matches_decoded_channel(channel_name, channel_idx)
        ]
        if not matched_routes:
            log.debug(
                f"[mqtt] decoded channel {channel_name!r} idx={channel_idx!r}: "
                "no configured route matched"
            )
            return

        sender_timestamp = normalize_sender_timestamp(
            data.get("sender_timestamp", data.get("timestamp"))
        )
        stable_id = make_message_dedup_id(
            matched_routes[0].channel_hash,
            sender_timestamp,
            sender,
            message,
        )
        content_id = make_message_content_id(
            matched_routes[0].channel_hash,
            sender,
            message,
        )
        self._stats.inc("packets_received")
        self._stats.inc("decoded_fallbacks")

        if (
            self._dedup.is_duplicate(stable_id)
            or self._content_dedup.is_duplicate(content_id)
        ):
            self._stats.inc("deduplicated")
            log.info(
                f"[dedup] SKIP id={stable_id} from {sender!r} "
                f"(already seen, decoded source {data.get('origin', 'Home Assistant')!r})"
            )
            return

        allowed, reason = self._throttle.check(sender)
        if not allowed:
            self._stats.inc("throttled")
            log.info(f"[throttle] SKIP message from {sender!r}: {reason}")
            return

        tg_text = f"[{sender}] : {message}"
        route_names = ", ".join(route.name for route in matched_routes)
        snr = data.get("SNR", data.get("snr", "?"))
        rssi = data.get("RSSI", data.get("rssi", "?"))
        log.info(
            f"[forward:{route_names}] [{sender}] : {message!r} "
            f"(id={stable_id}, source=decoded, SNR={snr}, RSSI={rssi})"
        )

        for route in matched_routes:
            try:
                self._tg_queue.put_nowait((route, sender, message, tg_text))
            except queue.Full:
                self._stats.inc("telegram_errors")
                log.warning(
                    f"[telegram:{route.name}] send queue full — "
                    "dropping message (Telegram backlog?)"
                )

    def run(self) -> None:
        self._schedule_stats()

        self._tg_worker = threading.Thread(
            target=self._tg_worker_loop, name="tg-sender", daemon=True
        )
        self._tg_worker.start()

        # paho-mqtt 2.x requires an explicit callback API version; request VERSION1 so the
        # v1 callback signatures used below keep working. paho 1.x lacks the enum → fall back.
        try:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION1,
                client_id="meshcore_tg_bot",
                clean_session=True,
            )
        except (AttributeError, TypeError):
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
            self._tg_queue.put(None)          # stop the Telegram worker
            if self._tg_worker:
                self._tg_worker.join(timeout=5)
            self._emit_stats()
            log.info("[mqtt] disconnected")


# ─── CLI ──────────────────────────────────────────────────────────────────────

CONFIG_DEFAULTS = {
    "mqtt_port": 1883,
    "mqtt_user": None,
    "mqtt_password": None,
    "mqtt_topic": "meshcore/+/+/packets",
    "mqtt_message_topic": "meshcore/+/+/messages",
    "dedup_ttl": 120.0,
    "content_dedup_ttl": 5.0,
    "throttle_per_sender": 0.0,
    "throttle_global": 0.0,
    "stats_interval": 600.0,
    "debug": False,
}

CONFIG_SCALAR_KEYS = {
    "mqtt_host", "mqtt_port", "mqtt_user", "mqtt_password", "mqtt_topic",
    "mqtt_message_topic",
    "telegram_token", "channel_psk", "telegram_chat", *CONFIG_DEFAULTS.keys(),
}


def load_config_file(filename: str) -> dict:
    """Load and validate the top level of a JSON configuration file."""
    path = Path(filename)
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"Cannot read config file {str(path)!r}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in config file {str(path)!r} at "
            f"line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(config, dict):
        raise ValueError("The config file root must be a JSON object")
    unknown = sorted(set(config) - (CONFIG_SCALAR_KEYS | {"channel_mappings"}))
    if unknown:
        raise ValueError(f"Unknown config key(s): {', '.join(unknown)}")
    return config


def parse_channel_map(value: str) -> Tuple[str, str]:
    """Parse the repeatable CLI PSK:TELEGRAM_CHAT mapping syntax."""
    if ":" not in value:
        raise ValueError(f"Invalid channel mapping {value!r}; expected PSK:TELEGRAM_CHAT")
    psk, telegram_chat = value.rsplit(":", 1)
    if not psk.strip() or not telegram_chat.strip():
        raise ValueError(
            f"Invalid channel mapping {value!r}; PSK and Telegram chat are required"
        )
    return psk.strip(), telegram_chat.strip()


def make_channel_mapping(
    psk: object,
    chat: object,
    name: object,
    channel_name: object = None,
    channel_idx: object = None,
) -> ChannelMapping:
    if not isinstance(psk, str):
        raise ValueError(f"Channel {name!r}: psk must be a string")
    if not isinstance(chat, (str, int)):
        raise ValueError(f"Channel {name!r}: telegram_chat must be a string or integer")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Each channel mapping name must be a non-empty string")
    if channel_name is not None and (
        not isinstance(channel_name, str) or not channel_name.strip()
    ):
        raise ValueError(f"Channel {name!r}: channel_name must be a non-empty string")
    if channel_idx is not None:
        if isinstance(channel_idx, bool):
            raise ValueError(f"Channel {name!r}: channel_idx must be an integer")
        try:
            channel_idx = int(channel_idx)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Channel {name!r}: channel_idx must be an integer") from exc
        if not 0 <= channel_idx <= 255:
            raise ValueError(f"Channel {name!r}: channel_idx must be between 0 and 255")
    telegram_chat = str(chat).strip()
    if not telegram_chat:
        raise ValueError(f"Channel {name!r}: telegram_chat cannot be empty")
    try:
        psk_16 = psk_to_bytes(psk)
    except ValueError as exc:
        raise ValueError(f"Channel {name!r}: {exc}") from exc
    return ChannelMapping(
        psk_16,
        telegram_chat,
        name.strip(),
        channel_name.strip() if isinstance(channel_name, str) else None,
        channel_idx,
    )


def resolve_configuration(args: argparse.Namespace) -> dict:
    """Merge defaults, JSON config and CLI arguments, then validate them."""
    config = load_config_file(args.config) if args.config else {}
    resolved = {}
    for key, default in CONFIG_DEFAULTS.items():
        cli_value = getattr(args, key)
        resolved[key] = cli_value if cli_value is not None else config.get(key, default)
    for key in ("mqtt_host", "telegram_token"):
        cli_value = getattr(args, key)
        resolved[key] = cli_value if cli_value is not None else config.get(key)

    for required_key in ("mqtt_host", "telegram_token"):
        if not isinstance(resolved[required_key], str) or not resolved[required_key].strip():
            raise ValueError(
                f"Missing required setting {required_key!r} "
                f"(use --{required_key.replace('_', '-')} or the config file)"
            )
        resolved[required_key] = resolved[required_key].strip()
    for key in ("mqtt_topic", "mqtt_message_topic"):
        if not isinstance(resolved[key], str) or not resolved[key].strip():
            raise ValueError(f"{key} must be a non-empty string")
        resolved[key] = resolved[key].strip()
    try:
        resolved["mqtt_port"] = int(resolved["mqtt_port"])
        for key in (
            "dedup_ttl",
            "content_dedup_ttl",
            "throttle_per_sender",
            "throttle_global",
            "stats_interval",
        ):
            resolved[key] = float(resolved[key])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid numeric setting in configuration: {exc}") from exc
    if not 1 <= resolved["mqtt_port"] <= 65535:
        raise ValueError("mqtt_port must be between 1 and 65535")
    for key in (
        "dedup_ttl",
        "content_dedup_ttl",
        "throttle_per_sender",
        "throttle_global",
        "stats_interval",
    ):
        if resolved[key] < 0:
            raise ValueError(f"{key} cannot be negative")
    if not isinstance(resolved["debug"], bool):
        raise ValueError("debug must be true or false in the config file")

    raw_config_mappings = config.get("channel_mappings", [])
    if not isinstance(raw_config_mappings, list):
        raise ValueError("channel_mappings must be a JSON array")
    mappings = []
    for index, item in enumerate(raw_config_mappings):
        if not isinstance(item, dict):
            raise ValueError(f"channel_mappings[{index}] must be a JSON object")
        unknown = sorted(
            set(item)
            - {"name", "psk", "telegram_chat", "channel_name", "channel_idx"}
        )
        if unknown:
            raise ValueError(
                f"channel_mappings[{index}] has unknown key(s): {', '.join(unknown)}"
            )
        missing = {"psk", "telegram_chat"} - set(item)
        if missing:
            raise ValueError(
                f"channel_mappings[{index}] is missing: {', '.join(sorted(missing))}"
            )
        name = item.get("name", f"channel-{len(mappings) + 1}")
        mappings.append(
            make_channel_mapping(
                item["psk"],
                item["telegram_chat"],
                name,
                item.get("channel_name"),
                item.get("channel_idx"),
            )
        )

    # CLI mappings add routes to those from the config file instead of replacing them.
    for value in args.channel_map or []:
        psk, chat = parse_channel_map(value)
        mappings.append(make_channel_mapping(psk, chat, f"channel-{len(mappings) + 1}"))

    # Backward-compatible single-channel pair, accepted in either CLI or JSON.
    legacy_psk = args.channel_psk if args.channel_psk is not None else config.get("channel_psk")
    legacy_chat = args.telegram_chat if args.telegram_chat is not None else config.get("telegram_chat")
    if (legacy_psk is None) != (legacy_chat is None):
        raise ValueError("--channel-psk and --telegram-chat must be provided together")
    if legacy_psk is not None:
        mappings.append(
            make_channel_mapping(legacy_psk, legacy_chat, f"channel-{len(mappings) + 1}")
        )
    if not mappings:
        raise ValueError(
            "No channel mapping configured; use --channel-map, channel_mappings in "
            "the config file, or the legacy --channel-psk/--telegram-chat pair"
        )

    names = [mapping.name for mapping in mappings]
    if len(names) != len(set(names)):
        raise ValueError("Channel mapping names must be unique")
    pairs = [(mapping.psk_16, mapping.telegram_chat) for mapping in mappings]
    if len(pairs) != len(set(pairs)):
        raise ValueError("Duplicate channel PSK / Telegram chat mapping")
    resolved["channel_mappings"] = mappings
    return resolved


def build_argument_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Forward MeshCore channel messages from MQTT to Telegram",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
CONFIG FILE
  --config config.json loads MQTT, Telegram and channel settings from JSON.
  Scalar command-line options override their config value. Every --channel-map
  adds a mapping to channel_mappings from the file.

CHANNEL MAPPINGS
  Repeat --channel-map PSK:TELEGRAM_CHAT as many times as needed. PSK accepts
  base64 (from a MeshCore channel export) or 32 hexadecimal characters.

EXAMPLES
  # Two mappings on one bot
  python meshcore_mqtt_telegram.py \\
      --mqtt-host 192.168.1.10 \\
      --telegram-token 7123456789:AAExxxxxx \\
      --channel-map 'izOH6cXN6mrJ5e26oRXNcg==:-100123456789' \\
      --channel-map '00112233445566778899aabbccddeeff:@another_channel'

  # All settings in a JSON file
  python meshcore_mqtt_telegram.py --config meshcore_mqtt_telegram.json

  # Legacy single-channel syntax (still supported)
  python meshcore_mqtt_telegram.py \\
      --mqtt-host 192.168.1.10 \\
      --channel-psk izOH6cXN6mrJ5e26oRXNcg== \\
      --telegram-token 7123456789:AAExxxxxx \\
      --telegram-chat -100123456789
""",
    )
    p.add_argument("--config", metavar="FILE", help="JSON configuration file")
    p.add_argument("--mqtt-host", default=None, help="MQTT broker hostname or IP address")
    p.add_argument("--mqtt-port", type=int, default=None,
                   help="MQTT broker port (default: 1883)")
    p.add_argument("--mqtt-user", default=None, help="MQTT username (optional)")
    p.add_argument("--mqtt-password", default=None, help="MQTT password (optional)")
    p.add_argument("--mqtt-topic", default=None,
                   help="MQTT topic pattern (default: meshcore/+/+/packets)")
    p.add_argument("--mqtt-message-topic", default=None,
                   help="Decoded HA message topic (default: meshcore/+/+/messages)")
    p.add_argument("--telegram-token", default=None,
                   help="Telegram Bot API token (from @BotFather)")
    p.add_argument("--channel-map", action="append", default=None,
                   metavar="PSK:TELEGRAM_CHAT",
                   help="MeshCore PSK to Telegram target mapping; repeatable")
    p.add_argument("--channel-psk", default=None,
                   help="Legacy single-channel PSK (use with --telegram-chat)")
    p.add_argument("--telegram-chat", default=None,
                   help="Legacy single Telegram target (use with --channel-psk)")
    p.add_argument("--dedup-ttl", type=float, default=None, metavar="SECONDS",
                   help="Duplicate TTL (default: 120s, 0 to disable)")
    p.add_argument("--content-dedup-ttl", type=float, default=None, metavar="SECONDS",
                   help="Same-content duplicate TTL (default: 5s, 0 to disable)")
    p.add_argument("--throttle-per-sender", type=float, default=None, metavar="SECONDS",
                   help="Min seconds between messages from one sender (default: off)")
    p.add_argument("--throttle-global", type=float, default=None, metavar="SECONDS",
                   help="Min seconds between any two incoming messages (default: off)")
    p.add_argument("--stats-interval", type=float, default=None, metavar="SECONDS",
                   help="Periodic stats interval (default: 600s, 0 to disable)")
    p.add_argument("--debug", action="store_true", default=None,
                   help="Enable DEBUG logging (very verbose)")
    return p


def main() -> None:
    p = build_argument_parser()

    args = p.parse_args()

    try:
        config = resolve_configuration(args)
    except ValueError as exc:
        p.error(str(exc))

    logging.basicConfig(
        level=logging.DEBUG if config["debug"] else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    bot = MeshCoreMQTTBot(
        mqtt_host           = config["mqtt_host"],
        mqtt_port           = config["mqtt_port"],
        mqtt_user           = config["mqtt_user"],
        mqtt_password       = config["mqtt_password"],
        mqtt_topic          = config["mqtt_topic"],
        mqtt_message_topic  = config["mqtt_message_topic"],
        telegram_token      = config["telegram_token"],
        channel_mappings    = config["channel_mappings"],
        dedup_ttl           = config["dedup_ttl"],
        content_dedup_ttl   = config["content_dedup_ttl"],
        throttle_per_sender = config["throttle_per_sender"],
        throttle_global     = config["throttle_global"],
        stats_interval      = config["stats_interval"],
    )
    bot.run()


if __name__ == "__main__":
    main()
