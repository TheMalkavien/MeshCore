#!/usr/bin/env python3
"""MeshCore MQTT group-text decoder and tracker extractor.

This bridge subscribes to MeshCore MQTT packet messages, decrypts group text
(PAYLOAD_TYPE_GRP_TXT = 5), parses tracker payloads (JSON or legacy text),
and republishes normalized JSON for Node-RED/Home Assistant automation.

Requirements:
  pip install paho-mqtt pycryptodome

Environment shortcuts (optional):
  MESHCORE_MQTT_HOST
  MESHCORE_MQTT_PORT
  MESHCORE_MQTT_USER
  MESHCORE_MQTT_PASS
  MESHCORE_MQTT_TOPIC
  MESHCORE_MQTT_OUT_PREFIX
  MESHCORE_CHANNELS   (format: "Name=PSK,Other=PSK2")
  MESHCORE_DEBUG
  MESHCORE_VERBOSE_DEBUG
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import re
import struct
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover
    mqtt = None

try:
    from Crypto.Cipher import AES
except ImportError:  # pragma: no cover
    AES = None

PAYLOAD_TYPE_GRP_TXT = 0x05
PATH_HASH_SIZE = 1
CIPHER_MAC_SIZE = 2
CIPHER_KEY_SIZE = 16
FULL_SECRET_SIZE = 32

TRACKER_RE = re.compile(
    r"GPS\s+lat=(?P<lat>-?\d+(?:\.\d+)?)\s+lon=(?P<lon>-?\d+(?:\.\d+)?)"
    r"(?:\s+alt=(?P<alt>-?\d+(?:\.\d+)?)m)?"
    r"(?:\s+sats=(?P<sats>-?\d+))?"
    r"(?:\s+spd=(?P<spd>-?\d+(?:\.\d+)?)(?:\s*km/h)?)?"
    r"(?:\s+dir=(?P<dir>-?\d+(?:\.\d+)?)(?:\s*deg)?)?"
    r"(?:\s+fix=(?P<fix>[A-Za-z0-9_\-]+))?",
    re.IGNORECASE,
)

SENDER_GPS_PREFIX_RE = re.compile(r"^(?P<sender>[^:]{1,96}):\s*(?P<body>GPS\b.*)$", re.IGNORECASE)
SENDER_JSON_PREFIX_RE = re.compile(r"^(?P<sender>[^:]{1,96}):\s*(?P<body>\{.*)$", re.IGNORECASE)


@dataclass(frozen=True)
class KeyCandidate:
    secret32: bytes
    hash_len: int
    source: str


@dataclass
class ChannelConfig:
    name: str
    psk_raw: str
    candidates: List[KeyCandidate]


def _require_crypto() -> None:
    if AES is None:
        raise SystemExit("Missing dependency: pycryptodome (pip install pycryptodome)")


def _vdebug(enabled: bool, message: str, *args) -> None:
    if enabled:
        logging.debug("[verbose] " + message, *args)


def _shorten(value: str, max_len: int = 220) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def _parse_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _unique_bytes(items: Sequence[Tuple[bytes, str]]) -> List[Tuple[bytes, str]]:
    seen = set()
    out: List[Tuple[bytes, str]] = []
    for value, label in items:
        if value in seen:
            continue
        seen.add(value)
        out.append((value, label))
    return out


def _expand_psk_candidates(psk: str) -> List[KeyCandidate]:
    psk = psk.strip()
    raw_candidates: List[Tuple[bytes, str]] = []

    if re.fullmatch(r"[0-9A-Fa-f]{32}|[0-9A-Fa-f]{64}", psk):
        raw_candidates.append((bytes.fromhex(psk), "hex"))

    try:
        decoded = base64.b64decode(psk, validate=True)
        if decoded:
            raw_candidates.append((decoded, "base64"))
            try:
                text = decoded.decode("ascii")
                if re.fullmatch(r"[0-9A-Fa-f]{32}|[0-9A-Fa-f]{64}", text):
                    raw_candidates.append((bytes.fromhex(text), "base64-ascii-hex"))
            except UnicodeDecodeError:
                pass
    except (binascii.Error, ValueError):
        pass

    # Last-resort literal bytes (useful for troubleshooting custom keys)
    raw_candidates.append((psk.encode("utf-8"), "utf8-literal"))

    unique_raw = _unique_bytes(raw_candidates)
    out: List[KeyCandidate] = []
    for raw, source in unique_raw:
        if len(raw) == 16:
            secret32 = raw + (b"\x00" * 16)
            out.append(KeyCandidate(secret32=secret32, hash_len=16, source=source))
        elif len(raw) == 32:
            out.append(KeyCandidate(secret32=raw, hash_len=32, source=source))

    # Remove exact duplicate key material/hash_len pairs
    dedup = {}
    for cand in out:
        dedup[(cand.secret32, cand.hash_len)] = cand
    return list(dedup.values())


def _parse_channels(raw: str) -> List[ChannelConfig]:
    channels: List[ChannelConfig] = []
    if not raw:
        return channels

    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Invalid channel entry '{item}'. Expected Name=PSK")
        name, psk = item.split("=", 1)
        name = name.strip()
        psk = psk.strip()
        if not name or not psk:
            raise ValueError(f"Invalid channel entry '{item}'. Name and PSK are required")

        cands = _expand_psk_candidates(psk)
        if not cands:
            raise ValueError(f"Channel '{name}' has no usable key candidates")
        channels.append(ChannelConfig(name=name, psk_raw=psk, candidates=cands))

    return channels


def _channel_hash_byte(secret32: bytes, hash_len: int) -> int:
    return hashlib.sha256(secret32[:hash_len]).digest()[0]


def _decode_group_text(raw_hex: str, channels: Sequence[ChannelConfig], verbose: bool = False) -> Optional[dict]:
    _require_crypto()
    try:
        raw = bytes.fromhex(raw_hex)
    except ValueError:
        _vdebug(verbose, "group decode failed: raw hex invalid")
        return None

    if len(raw) < 4:
        _vdebug(verbose, "group decode failed: frame too short len=%s", len(raw))
        return None

    idx = 0
    header = raw[idx]
    idx += 1

    route_type = header & 0x03
    payload_type = (header >> 2) & 0x0F

    # Transport route types include 2x uint16 transport codes in header.
    if route_type in (0x00, 0x03):
        if len(raw) < idx + 4:
            _vdebug(verbose, "group decode failed: missing transport header route_type=%s", route_type)
            return None
        idx += 4

    if len(raw) < idx + 1:
        _vdebug(verbose, "group decode failed: missing path length byte")
        return None

    path_len = raw[idx]
    idx += 1
    if len(raw) < idx + path_len:
        _vdebug(
            verbose,
            "group decode failed: path overflow path_len=%s frame_len=%s",
            path_len,
            len(raw),
        )
        return None
    idx += path_len

    payload = raw[idx:]
    _vdebug(
        verbose,
        "group frame parsed: route_type=%s payload_type=%s path_len=%s payload_len=%s",
        route_type,
        payload_type,
        path_len,
        len(payload),
    )
    if payload_type != PAYLOAD_TYPE_GRP_TXT:
        _vdebug(verbose, "group decode skipped: payload_type=%s (expected=%s)", payload_type, PAYLOAD_TYPE_GRP_TXT)
        return None

    if len(payload) < PATH_HASH_SIZE + CIPHER_MAC_SIZE + 16:
        _vdebug(verbose, "group decode failed: payload too short len=%s", len(payload))
        return None

    channel_hash = payload[0]
    mac = payload[1 : 1 + CIPHER_MAC_SIZE]
    ciphertext = payload[1 + CIPHER_MAC_SIZE :]

    if len(ciphertext) % 16 != 0:
        _vdebug(verbose, "group decode failed: ciphertext len=%s not multiple of 16", len(ciphertext))
        return None

    _vdebug(verbose, "group crypto: channel_hash=%02X mac=%s cipher_len=%s", channel_hash, mac.hex(), len(ciphertext))
    for channel in channels:
        _vdebug(verbose, "checking channel=%s candidates=%s", channel.name, len(channel.candidates))
        for cand in channel.candidates:
            expected_hash = _channel_hash_byte(cand.secret32, cand.hash_len)
            if expected_hash != channel_hash:
                _vdebug(
                    verbose,
                    "candidate miss channel=%s source=%s expected_hash=%02X",
                    channel.name,
                    cand.source,
                    expected_hash,
                )
                continue

            calc_mac = hmac.new(cand.secret32, ciphertext, hashlib.sha256).digest()[:CIPHER_MAC_SIZE]
            if calc_mac != mac:
                _vdebug(
                    verbose,
                    "candidate mac mismatch channel=%s source=%s calc=%s msg=%s",
                    channel.name,
                    cand.source,
                    calc_mac.hex(),
                    mac.hex(),
                )
                continue

            plain = AES.new(cand.secret32[:CIPHER_KEY_SIZE], AES.MODE_ECB).decrypt(ciphertext)
            if len(plain) < 5:
                _vdebug(verbose, "candidate decrypt too short channel=%s source=%s", channel.name, cand.source)
                continue

            mesh_ts = struct.unpack_from("<I", plain, 0)[0]
            txt_type = plain[4]
            text_bytes = plain[5:]
            text = text_bytes.split(b"\x00", 1)[0].decode("utf-8", errors="replace")
            _vdebug(
                verbose,
                "decrypt success channel=%s source=%s mesh_ts=%s text_type=%s flags=%s text=%r",
                channel.name,
                cand.source,
                mesh_ts,
                (txt_type >> 2),
                (txt_type & 0x03),
                _shorten(text),
            )

            return {
                "channel": channel.name,
                "channel_hash": f"{channel_hash:02X}",
                "mesh_timestamp": mesh_ts,
                "text_type": (txt_type >> 2),
                "text_flags": (txt_type & 0x03),
                "text": text,
                "key_source": cand.source,
            }

    _vdebug(verbose, "group decode failed: no channel/key candidate matched")
    return None


def _extract_sender_and_body(text: str) -> Tuple[Optional[str], str]:
    stripped = text.strip()
    if not stripped:
        return None, stripped
    if stripped[0] == "{":
        return None, stripped

    # Primary path for tracker payloads emitted as "<sender>: <body>".
    # Use right split so sender names containing ":" still parse when body is JSON/GPS.
    if ": " in stripped:
        sender_part, _, body_part = stripped.rpartition(": ")
        sender = sender_part.strip()
        body = body_part.strip()
        if sender and body and (body.startswith("{") or body.upper().startswith("GPS")):
            return sender, body

    m = SENDER_GPS_PREFIX_RE.match(stripped)
    if m:
        return m.group("sender").strip(), m.group("body").strip()

    m = SENDER_JSON_PREFIX_RE.match(stripped)
    if m:
        return m.group("sender").strip(), m.group("body").strip()

    return None, stripped


def _coerce_float(value) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip()
        if not v or v.lower() in {"null", "none", "n/a", "na"}:
            return None
        try:
            return float(v)
        except ValueError:
            return None
    return None


def _coerce_int(value) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        v = value.strip()
        if not v or v.lower() in {"null", "none", "n/a", "na"}:
            return None
        try:
            return int(float(v))
        except ValueError:
            return None
    return None


def _first_present(mapping: dict, keys: Sequence[str]):
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _parse_tracker_json(body: str, verbose: bool = False) -> Optional[dict]:
    try:
        payload = json.loads(body)
    except (TypeError, json.JSONDecodeError):
        _vdebug(verbose, "tracker json parse failed: invalid json body=%r", _shorten(body))
        return None

    if not isinstance(payload, dict):
        _vdebug(verbose, "tracker json parse failed: payload type=%s is not object", type(payload).__name__)
        return None

    tracker_obj = payload
    nested = payload.get("tracker")
    if isinstance(nested, dict):
        tracker_obj = nested

    out = {}
    lat = _coerce_float(_first_present(tracker_obj, ("lat",)))
    lon = _coerce_float(_first_present(tracker_obj, ("lon",)))
    if lat is not None and lon is not None:
        out["lat"] = lat
        out["lon"] = lon

    alt = _coerce_float(_first_present(tracker_obj, ("alt_m", "alt")))
    if alt is not None:
        out["alt_m"] = alt

    sats = _coerce_int(_first_present(tracker_obj, ("sats", "sat")))
    if sats is not None:
        out["sats"] = sats

    speed = _coerce_float(_first_present(tracker_obj, ("speed_kmh", "spd_kmh", "spd")))
    if speed is not None:
        out["speed_kmh"] = speed

    heading = _coerce_float(_first_present(tracker_obj, ("heading_deg", "dir_deg", "dir", "heading")))
    if heading is not None:
        out["heading_deg"] = heading

    fix = _first_present(tracker_obj, ("fix",))
    if isinstance(fix, str) and fix:
        out["fix"] = fix

    version = _coerce_int(_first_present(tracker_obj, ("ver", "version", "v")))
    if version is not None:
        out["version"] = version

    tracker_type = _first_present(tracker_obj, ("type", "t"))
    if isinstance(tracker_type, str) and tracker_type:
        out["type"] = tracker_type

    event = _first_present(tracker_obj, ("event", "evt"))
    if isinstance(event, str) and event:
        out["event"] = event

    reason = _first_present(tracker_obj, ("reason", "err"))
    if isinstance(reason, str) and reason:
        out["reason"] = reason

    # Position report: require lat/lon.
    if "lat" in out and "lon" in out:
        _vdebug(verbose, "tracker json parsed as position: %s", out)
        return out
    # Tracker event without coordinates (timeout, gps_unavailable, ...).
    if out.get("type") == "tracker" and ("event" in out or "reason" in out):
        _vdebug(verbose, "tracker json parsed as event: %s", out)
        return out
    _vdebug(verbose, "tracker json parse rejected: missing lat/lon and no tracker event fields")
    return None


def _parse_tracker(body: str, verbose: bool = False) -> Optional[dict]:
    json_tracker = _parse_tracker_json(body, verbose=verbose)
    if json_tracker:
        return json_tracker

    m = TRACKER_RE.search(body)
    if not m:
        _vdebug(verbose, "tracker regex parse failed body=%r", _shorten(body))
        return None

    out = {
        "lat": float(m.group("lat")),
        "lon": float(m.group("lon")),
    }
    if m.group("alt") is not None:
        out["alt_m"] = float(m.group("alt"))
    if m.group("sats") is not None:
        out["sats"] = int(m.group("sats"))
    if m.group("spd") is not None:
        out["speed_kmh"] = float(m.group("spd"))
    if m.group("dir") is not None:
        out["heading_deg"] = float(m.group("dir"))
    if m.group("fix") is not None:
        out["fix"] = m.group("fix")
    _vdebug(verbose, "tracker regex parsed: %s", out)
    return out


def _decode_packet_message(
    topic: str,
    payload: dict,
    channels: Sequence[ChannelConfig],
    received_at: Optional[int] = None,
    verbose: bool = False,
) -> Optional[dict]:
    packet_type_raw = payload.get("packet_type", payload.get("payload_type", payload.get("type")))
    packet_type: Optional[int] = None
    if isinstance(packet_type_raw, int):
        packet_type = packet_type_raw
    elif isinstance(packet_type_raw, str):
        s = packet_type_raw.strip().lower()
        if s.isdigit():
            packet_type = int(s)
        elif s.startswith("0x"):
            try:
                packet_type = int(s, 16)
            except ValueError:
                packet_type = None
        elif s in {"grp_txt", "group_text", "group-txt"}:
            packet_type = PAYLOAD_TYPE_GRP_TXT

    _vdebug(verbose, "packet inspection topic=%s packet_type_raw=%r resolved=%r", topic, packet_type_raw, packet_type)
    if packet_type != PAYLOAD_TYPE_GRP_TXT:
        logging.debug("skip packet: unsupported packet_type=%r topic=%s", packet_type_raw, topic)
        return None

    raw_hex = payload.get("raw")
    if raw_hex is None and isinstance(payload.get("packet"), dict):
        raw_hex = payload["packet"].get("raw")
    if raw_hex is None:
        raw_hex = payload.get("data")
    if not raw_hex:
        logging.debug("skip packet: missing raw hex topic=%s keys=%s", topic, sorted(payload.keys()))
        return None
    if not isinstance(raw_hex, str):
        _vdebug(verbose, "raw frame ignored topic=%s: raw type=%s is not string", topic, type(raw_hex).__name__)
        return None

    _vdebug(verbose, "raw frame candidate located topic=%s hex_len=%s", topic, len(raw_hex))
    decoded = _decode_group_text(raw_hex, channels, verbose=verbose)
    if not decoded:
        logging.debug("skip packet: group decrypt failed topic=%s", topic)
        return None

    sender, body = _extract_sender_and_body(decoded["text"])
    _vdebug(
        verbose,
        "decoded text topic=%s channel=%s sender=%r body=%r",
        topic,
        decoded.get("channel"),
        sender,
        _shorten(body),
    )
    tracker = _parse_tracker(body, verbose=verbose)

    parts = topic.split("/")
    iata = parts[1] if len(parts) > 1 else "UNK"
    device_id = parts[2] if len(parts) > 2 else payload.get("origin_id", "unknown")

    base = {
        "source_topic": topic,
        "iata": iata,
        "device_id": device_id,
        "origin": payload.get("origin"),
        "origin_id": payload.get("origin_id"),
        "packet_hash": payload.get("hash"),
        "packet_timestamp": payload.get("timestamp"),
        "decoded": decoded,
        "sender": sender,
        "body": body,
        "received_at": int(time.time()) if received_at is None else received_at,
    }
    if tracker:
        _vdebug(verbose, "tracker payload extracted topic=%s keys=%s", topic, sorted(tracker.keys()))
        base["tracker"] = tracker
    else:
        logging.debug("decoded text but no tracker payload topic=%s text=%r", topic, decoded["text"])
        _vdebug(verbose, "decoded message has no tracker payload topic=%s", topic)
    return base


class BridgeApp:
    def __init__(self, args: argparse.Namespace) -> None:
        if mqtt is None:
            raise SystemExit("Missing dependency: paho-mqtt (pip install paho-mqtt)")
        _require_crypto()
        self.args = args
        self.channels = _parse_channels(args.channels)
        if not self.channels:
            raise SystemExit("No channels configured. Use --channels or MESHCORE_CHANNELS")
        self._dedupe_seconds = max(0, int(args.dedupe_seconds))
        self._seen_packets: Dict[str, float] = {}
        self._last_prune = 0.0
        self._mqtt_ok_rc = getattr(mqtt, "MQTT_ERR_SUCCESS", 0)
        self._reconnect_min = max(1, int(args.reconnect_min_seconds))
        self._reconnect_max = max(self._reconnect_min, int(args.reconnect_max_seconds))
        self._verbose_debug = bool(args.verbose_debug)
        if self._verbose_debug:
            for ch in self.channels:
                details = ", ".join(
                    f"{cand.source}:{_channel_hash_byte(cand.secret32, cand.hash_len):02X}" for cand in ch.candidates
                )
                _vdebug(
                    True,
                    "channel config name=%s candidate_hashes=[%s]",
                    ch.name,
                    details or "none",
                )
        self.client = self._create_mqtt_client(args.client_id)
        if args.username:
            self.client.username_pw_set(args.username, args.password)

        try:
            self.client.enable_logger(logging.getLogger("paho"))
        except Exception:
            # Older paho versions may not support enable_logger.
            pass
        try:
            self.client.reconnect_delay_set(min_delay=self._reconnect_min, max_delay=self._reconnect_max)
        except Exception:
            # Older paho versions may not expose reconnect_delay_set.
            pass
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_message = self._on_message

    def _create_mqtt_client(self, client_id: str):
        # paho-mqtt v2 supports CallbackAPIVersion; v1 does not.
        if hasattr(mqtt, "CallbackAPIVersion"):
            try:
                return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
            except TypeError:
                pass
        return mqtt.Client(client_id=client_id)

    def _on_connect(self, client, userdata, flags, reason_code=0, properties=None):
        rc_value = self._reason_code_value(reason_code)
        if rc_value not in (self._mqtt_ok_rc, 0):
            logging.warning("MQTT connected with non-success reason=%s", reason_code)
        logging.info("MQTT connected rc=%s, subscribing to %s", reason_code, self.args.topic)
        sub_rc, mid = client.subscribe(self.args.topic, qos=1)
        if sub_rc != self._mqtt_ok_rc:
            logging.error("MQTT subscribe failed rc=%s topic=%s", sub_rc, self.args.topic)
        else:
            logging.debug("MQTT subscribe requested mid=%s topic=%s", mid, self.args.topic)

    def _on_disconnect(self, client, userdata, reason_code=0, properties=None):
        rc_value = self._reason_code_value(reason_code)
        if rc_value in (self._mqtt_ok_rc, 0):
            logging.info("MQTT disconnected cleanly")
        else:
            logging.warning("MQTT disconnected unexpectedly reason=%s", reason_code)

    def _on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        logging.debug("MQTT subscribed mid=%s qos=%s", mid, granted_qos)

    @staticmethod
    def _reason_code_value(reason_code) -> int:
        try:
            return int(reason_code)
        except Exception:
            value = getattr(reason_code, "value", None)
            if value is not None:
                try:
                    return int(value)
                except Exception:
                    return -1
        return -1

    def _is_connected(self) -> bool:
        if hasattr(self.client, "is_connected"):
            try:
                return bool(self.client.is_connected())
            except Exception:
                return False
        return False

    def _safe_publish(self, topic: str, payload: dict) -> None:
        try:
            info = self.client.publish(
                topic,
                json.dumps(payload, separators=(",", ":")),
                qos=1,
                retain=False,
            )
        except Exception:
            logging.exception("MQTT publish threw exception topic=%s", topic)
            return

        if info.rc != self._mqtt_ok_rc:
            logging.error("MQTT publish failed rc=%s topic=%s", info.rc, topic)

    def _connect_with_backoff(self) -> None:
        delay = self._reconnect_min
        while True:
            try:
                self.client.connect(self.args.host, self.args.port, keepalive=60)
                return
            except KeyboardInterrupt:
                raise
            except Exception:
                logging.exception(
                    "Initial MQTT connection failed to %s:%s, retry in %ss",
                    self.args.host,
                    self.args.port,
                    delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, self._reconnect_max)

    def _reconnect_with_backoff(self) -> None:
        delay = self._reconnect_min
        while not self._is_connected():
            try:
                rc = self.client.reconnect()
                if rc == self._mqtt_ok_rc:
                    return
                logging.warning("MQTT reconnect returned rc=%s, retry in %ss", rc, delay)
            except KeyboardInterrupt:
                raise
            except Exception:
                logging.exception("MQTT reconnect failed, retry in %ss", delay)
            time.sleep(delay)
            delay = min(delay * 2, self._reconnect_max)

    def _dedupe_key(self, base: dict) -> str:
        packet_hash = str(base.get("packet_hash") or "").strip()
        if packet_hash:
            return f"hash:{packet_hash}"

        decoded = base.get("decoded") or {}
        mesh_ts = decoded.get("mesh_timestamp")
        body = base.get("body") or ""
        origin_id = base.get("origin_id") or base.get("device_id") or "unknown"
        return f"fallback:{origin_id}:{mesh_ts}:{body}"

    def _is_duplicate(self, base: dict) -> bool:
        if self._dedupe_seconds <= 0:
            return False

        now = time.monotonic()
        key = self._dedupe_key(base)
        last = self._seen_packets.get(key)
        if last is not None and (now - last) < self._dedupe_seconds:
            return True

        self._seen_packets[key] = now
        # Periodic cleanup to bound memory.
        if (now - self._last_prune) >= max(5.0, self._dedupe_seconds):
            cutoff = now - self._dedupe_seconds
            self._seen_packets = {k: ts for k, ts in self._seen_packets.items() if ts >= cutoff}
            self._last_prune = now
        return False

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        _vdebug(self._verbose_debug, "mqtt rx topic=%s payload_bytes=%s", topic, len(msg.payload or b""))

        try:
            payload = json.loads(msg.payload.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            _vdebug(
                self._verbose_debug,
                "mqtt rx ignored: invalid json topic=%s payload=%r",
                topic,
                _shorten(msg.payload.decode("utf-8", errors="replace")),
            )
            return

        try:
            _vdebug(self._verbose_debug, "mqtt json parsed topic=%s keys=%s", topic, sorted(payload.keys()))
            base = _decode_packet_message(topic, payload, self.channels, verbose=self._verbose_debug)
            if not base:
                return
            if self._is_duplicate(base):
                logging.debug(
                    "duplicate tracker packet ignored hash=%s topic=%s",
                    base.get("packet_hash"),
                    topic,
                )
                return

            iata = base["iata"]
            device_id = base["device_id"]
            _vdebug(
                self._verbose_debug,
                "publishing decoded payload iata=%s device_id=%s has_tracker=%s",
                iata,
                device_id,
                "tracker" in base,
            )
            if self.args.publish_text:
                text_topic = f"{self.args.out_prefix}/{iata}/{device_id}/text"
                # Keep text topic focused on decrypted textual content only.
                text_payload = dict(base)
                text_payload.pop("tracker", None)
                self._safe_publish(text_topic, text_payload)

            if "tracker" in base:
                tracker_topic = f"{self.args.out_prefix}/{iata}/{device_id}/tracker"
                self._safe_publish(tracker_topic, base)
                t = base["tracker"]
                if "lat" in t and "lon" in t:
                    logging.info(
                        "tracker %s %s lat=%.6f lon=%.6f sats=%s",
                        iata,
                        device_id,
                        t["lat"],
                        t["lon"],
                        t.get("sats", "?"),
                    )
                else:
                    logging.info(
                        "tracker-event %s %s event=%s reason=%s",
                        iata,
                        device_id,
                        t.get("event", "?"),
                        t.get("reason", "?"),
                    )
        except Exception:
            logging.exception("Unhandled error while processing MQTT message topic=%s", topic)

    def run(self) -> None:
        logging.info("Configured channels: %s", ", ".join(ch.name for ch in self.channels))
        self._connect_with_backoff()
        try:
            while True:
                rc = self.client.loop(timeout=1.0)
                if rc not in (self._mqtt_ok_rc, 0):
                    logging.debug("MQTT loop returned rc=%s", rc)
                if not self._is_connected():
                    logging.warning("MQTT connection lost, trying to reconnect")
                    self._reconnect_with_backoff()
                    continue
                time.sleep(0.1)
        except KeyboardInterrupt:
            logging.info("Interrupted, stopping bridge")
        finally:
            try:
                self.client.disconnect()
            except Exception:
                pass


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="MeshCore MQTT tracker decoder bridge")
    p.add_argument("--host", default=os.getenv("MESHCORE_MQTT_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MESHCORE_MQTT_PORT", "1883")))
    p.add_argument("--username", default=os.getenv("MESHCORE_MQTT_USER", ""))
    p.add_argument("--password", default=os.getenv("MESHCORE_MQTT_PASS", ""))
    p.add_argument("--topic", default=os.getenv("MESHCORE_MQTT_TOPIC", "meshcore/+/+/packets"))
    p.add_argument("--out-prefix", default=os.getenv("MESHCORE_MQTT_OUT_PREFIX", "meshcore/decoded"))
    p.add_argument("--channels", default=os.getenv("MESHCORE_CHANNELS", ""))
    p.add_argument(
        "--publish-text",
        action="store_true",
        default=_parse_bool(os.getenv("MESHCORE_PUBLISH_TEXT"), False),
        help="Also publish decrypted text messages to {out-prefix}/.../text (disabled by default)",
    )
    p.add_argument(
        "--dedupe-seconds",
        type=int,
        default=int(os.getenv("MESHCORE_DEDUPE_SECONDS", "15")),
        help="Ignore duplicate packets seen within this window (default: 15, set 0 to disable)",
    )
    p.add_argument(
        "--reconnect-min-seconds",
        type=int,
        default=int(os.getenv("MESHCORE_RECONNECT_MIN_SECONDS", "2")),
        help="Minimum delay for MQTT reconnect retry backoff (default: 2)",
    )
    p.add_argument(
        "--reconnect-max-seconds",
        type=int,
        default=int(os.getenv("MESHCORE_RECONNECT_MAX_SECONDS", "60")),
        help="Maximum delay for MQTT reconnect retry backoff (default: 60)",
    )
    p.add_argument("--client-id", default=os.getenv("MESHCORE_MQTT_CLIENT_ID", "meshcore-tracker-bridge"))
    p.add_argument("--debug", action="store_true", default=_parse_bool(os.getenv("MESHCORE_DEBUG"), False))
    p.add_argument(
        "--verbose-debug",
        action="store_true",
        default=_parse_bool(os.getenv("MESHCORE_VERBOSE_DEBUG"), False),
        help="Enable very detailed debug logs for packet decode, channel/key checks, and tracker parsing",
    )
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    debug_enabled = args.debug or args.verbose_debug
    logging.basicConfig(
        level=logging.DEBUG if debug_enabled else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    app = BridgeApp(args)
    app.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
