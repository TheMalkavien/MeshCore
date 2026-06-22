#!/usr/bin/env python3
"""
Serial broadcast proxy for MeshCore companion links:
- Multi clients TCP -> single backend serial connection
- Proxy polls backend at a fixed interval; results cached locally
- Client CMD_SYNC_NEXT_MESSAGE always served from local cache (never forwarded)
- Reconnecting clients replay only missed messages from the proxy history
"""

import argparse
import asyncio
import itertools
import logging
import signal
import socket
import struct
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

try:
    import serial
    from serial import SerialException
except ImportError:  # pragma: no cover - depends on local environment
    serial = None

    class SerialException(Exception):
        pass


CMD_APP_START = 0x01
CMD_SEND_CHAN_MSG = 0x03
CMD_GET_CONTACTS = 0x04
CMD_ADD_UPDATE_CONTACT = 0x09
CMD_SYNC_NEXT_MESSAGE = 0x0A
CMD_RESET_PATH = 0x0D
CMD_REMOVE_CONTACT = 0x0F
RESP_CODE_SELF_INFO = 0x05
RESP_CODE_NO_MORE_MESSAGES = 0x0A

# Offset of the 4-byte uint32_t LE lastmod field inside a CONTACT wire frame.
# Wire frame: '>'(1) + len_LE(2) + code(1) + pubkey(32) + type(1) + flags(1)
#             + out_path_len(1) + out_path(64) + name(32) + last_advert(4)
#             + lat(4) + lon(4) + lastmod(4) = total 151 bytes
CONTACT_LASTMOD_OFFSET = 147

# Packet types that are singleton in the cache: the latest frame replaces the
# previous one (e.g. SELF_INFO / DEVICE_INFO which describe fixed node state).
CACHE_SINGLETON_TYPES = {
    0x05,  # RESP_CODE_SELF_INFO
    0x0D,  # RESP_CODE_DEVICE_INFO
    0x15,  # RESP_CODE_CUSTOM_VARS    — node config, one per device
    0x17,  # RESP_CODE_TUNING_PARAMS  — radio config, one per device
    0x19,  # RESP_CODE_AUTOADD_CONFIG — one per device
    0x1A,  # RESP_ALLOWED_REPEAT_FREQ — one per device
}

# Packet types deduplicated by pubkey (frame[4:36], 32 bytes).
# The latest frame for a given pubkey replaces the previous one.
# 0x03 = RESP_CODE_CONTACT, 0x8A = PUSH_CODE_NEW_ADVERT
CACHE_DEDUP_BY_PUBKEY_TYPES = {0x03, 0x8A}
PUBKEY_OFFSET = 4   # bytes: '>' + LSB + MSB + type
PUBKEY_SIZE   = 32

# Packet types deduplicated by a single key byte at frame[4].
# 0x12 = RESP_CODE_CHANNEL_INFO  (key = channel_idx)
CACHE_DEDUP_BY_IDX_TYPES = {0x12}
IDX_OFFSET = 4

# Packet types that must NOT be cached at all.
CACHE_BLACKLIST_TYPES = {
    0x00,  # OK              — transient command acknowledgment
    0x01,  # ERR             — transient error
    0x06,  # SENT            — transient send acknowledgment
    0x09,  # CURR_TIME       — time value, always stale when replayed
    0x0A,  # NO_MORE_MESSAGES — internal poll protocol, never forwarded
    0x0C,  # BATT_AND_STORAGE — changes constantly, stale data not useful
    0x0E,  # PRIVATE_KEY     — security-sensitive, must not be replayed
    0x02,  # CONTACTS_START  — delimiter, rebuilt from CONTACT cache on replay
    0x04,  # END_OF_CONTACTS — delimiter, rebuilt from CONTACT cache on replay
    0x0F,  # DISABLED        — transient feature-flag response
    0x0B,  # EXPORT_CONTACT  — transient response to a specific command
    0x13,  # SIGN_START      — transient signing session
    0x14,  # SIGNATURE       — transient signing result
    0x16,  # ADVERT_PATH     — per-contact routing info, stale on replay
    0x18,  # STATS           — varies by stats_type, stale on replay
    # Unsolicited push events (0x80–0x90): all transient except PUSH_NEW_ADVERT
    # (0x8A) which carries a full contact structure and is worth caching.
    0x80,  # PUSH_ADVERT                  — radio event, not persistent state
    0x81,  # PUSH_PATH_UPDATED            — routing event
    0x82,  # PUSH_SEND_CONFIRMED          — ACK event
    0x83,  # PUSH_MSG_WAITING             — tickle, proxy handles polling
    0x84,  # PUSH_RAW_DATA                — transient datagram
    0x85,  # PUSH_LOGIN_SUCCESS           — transient auth event
    0x86,  # PUSH_LOGIN_FAIL              — transient auth event
    0x87,  # PUSH_STATUS_RESPONSE         — transient status reply
    0x88,  # PUSH_LOG_RX_DATA             — debug radio log, very frequent
    0x89,  # PUSH_TRACE_DATA              — transient trace result
    0x8B,  # PUSH_TELEMETRY_RESPONSE      — transient sensor data
    0x8C,  # PUSH_BINARY_RESPONSE         — transient binary reply
    0x8D,  # PUSH_PATH_DISCOVERY_RESPONSE — transient
    0x8E,  # PUSH_CONTROL_DATA            — transient
    0x8F,  # PUSH_CONTACT_DELETED         — event (deletion handled by app state)
    0x90,  # PUSH_CONTACTS_FULL           — transient overflow notification
}

MAX_COMPANION_FRAME_SIZE = 2048
DEFAULT_HISTORY_BYTES = 512 * 1024
DEFAULT_HISTORY_FRAMES = 4096
DEFAULT_CLIENT_WRITE_TIMEOUT = 2.0
DEFAULT_CLIENT_QUEUE_FRAMES = 512
DEFAULT_POLL_INTERVAL = 1.0
DEFAULT_MAX_CLIENTS = 32
# Keep inactive client cursors for as long as the message history (86400 = 24h).
# Pruning cursors earlier than the history window causes full replays on reconnect.
DEFAULT_INACTIVE_STATE_MAX_AGE = 86400
DEFAULT_CACHE_BUCKET_MAX = 256
DEFAULT_RECONNECT_BACKOFF_MAX = 60.0

# RESP_CODE_CONTACT_MSG_RECV / CHANNEL_MSG_RECV  (v<3 : 0x07, 0x08)
# RESP_CODE_CONTACT_MSG_RECV_V3 / CHANNEL_MSG_RECV_V3  (v≥3 : 0x10, 0x11)
MESSAGE_PACKET_TYPES = {0x07, 0x08, 0x10, 0x11}

PACKET_TYPE_NAMES: dict[int, str] = {
    # Responses to client commands
    0x00: "OK",
    0x01: "ERR",
    0x02: "CONTACTS_START",
    0x03: "CONTACT",
    0x04: "END_OF_CONTACTS",
    0x05: "SELF_INFO",
    0x06: "SENT",
    0x07: "MSG_RECV",
    0x08: "CHANNEL_MSG_RECV",
    0x09: "CURR_TIME",
    0x0A: "NO_MORE_MESSAGES",
    0x0B: "EXPORT_CONTACT",
    0x0C: "BATT_AND_STORAGE",
    0x0D: "DEVICE_INFO",
    0x0E: "PRIVATE_KEY",
    0x0F: "DISABLED",
    0x10: "MSG_RECV_V3",
    0x11: "CHANNEL_MSG_RECV_V3",
    0x12: "CHANNEL_INFO",
    0x13: "SIGN_START",
    0x14: "SIGNATURE",
    0x15: "CUSTOM_VARS",
    0x16: "ADVERT_PATH",
    0x17: "TUNING_PARAMS",
    0x18: "STATS",
    0x19: "AUTOADD_CONFIG",
    0x1A: "ALLOWED_REPEAT_FREQ",
    0x1B: "CHANNEL_DATA_RECV",
    # Unsolicited push notifications (0x80–0x90)
    0x80: "PUSH_ADVERT",
    0x81: "PUSH_PATH_UPDATED",
    0x82: "PUSH_SEND_CONFIRMED",
    0x83: "PUSH_MSG_WAITING",
    0x84: "PUSH_RAW_DATA",
    0x85: "PUSH_LOGIN_SUCCESS",
    0x86: "PUSH_LOGIN_FAIL",
    0x87: "PUSH_STATUS_RESPONSE",
    0x88: "PUSH_LOG_RX_DATA",
    0x89: "PUSH_TRACE_DATA",
    0x8A: "PUSH_NEW_ADVERT",
    0x8B: "PUSH_TELEMETRY_RESPONSE",
    0x8C: "PUSH_BINARY_RESPONSE",
    0x8D: "PUSH_PATH_DISCOVERY_RESPONSE",
    0x8E: "PUSH_CONTROL_DATA",
    0x8F: "PUSH_CONTACT_DELETED",
    0x90: "PUSH_CONTACTS_FULL",
}

# How often (in poll iterations) to prune stale inactive client states.
_PRUNE_EVERY_N_POLLS = 60


def fmt_ptype(pt: Optional[int]) -> str:
    if pt is None:
        return "NONE"
    return PACKET_TYPE_NAMES.get(pt, f"0x{pt:02X}")


class CompanionStreamParser:
    def __init__(self, header_byte: int, max_frame_size: int = MAX_COMPANION_FRAME_SIZE):
        self.header_byte = header_byte
        self.max_frame_size = max_frame_size
        self.buffer = bytearray()

    def reset(self) -> None:
        self.buffer.clear()

    def feed(self, data: bytes) -> list[bytes]:
        if data:
            self.buffer.extend(data)

        frames: list[bytes] = []
        header = self.header_byte

        while self.buffer:
            if self.buffer[0] != header:
                next_header = self.buffer.find(bytes((header,)))
                if next_header < 0:
                    self.buffer.clear()
                    break
                del self.buffer[:next_header]

            if len(self.buffer) < 3:
                break

            # frame_len is always 0–65535 (unsigned); no negative check needed.
            frame_len = self.buffer[1] | (self.buffer[2] << 8)
            if frame_len > self.max_frame_size:
                del self.buffer[0]
                continue

            total_len = 3 + frame_len
            if len(self.buffer) < total_len:
                break

            frames.append(bytes(self.buffer[:total_len]))
            del self.buffer[:total_len]

        return frames


@dataclass
class BufferedFrame:
    seq: int
    data: bytes
    timestamp: float = field(default_factory=time.time)


@dataclass
class ClientState:
    client_id: str
    peer_group: str
    app_name: str = ""
    writer: Optional[asyncio.StreamWriter] = None
    # Initialized in _register_client inside the running event loop (Python 3.10+ safe).
    send_lock: Optional[asyncio.Lock] = field(default=None, init=False, repr=False)
    client_parser: CompanionStreamParser = field(
        default_factory=lambda: CompanionStreamParser(ord("<"))
    )
    awaiting_replay_after_self_info: bool = False
    replay_capture_seq: int = 0
    pending_message_frames: deque = field(default_factory=deque)
    outbound_queue: Optional[asyncio.Queue] = None
    sender_task: Optional[asyncio.Task] = None
    last_disconnect_at: float = 0.0


def get_packet_type(frame: bytes) -> Optional[int]:
    if len(frame) < 4:
        return None
    return frame[3]


def is_message_frame(frame: bytes) -> bool:
    return get_packet_type(frame) in MESSAGE_PACKET_TYPES


def build_backend_frame(packet_type: int) -> bytes:
    return bytes((ord(">"), 1, 0, packet_type))


class SerialBroadcastProxy:
    def __init__(
        self,
        listen_host: str,
        listen_port: int,
        serial_port: str,
        baudrate: int,
        reconnect_delay: float = 1.0,
        read_timeout: float = 0.2,
        write_timeout: float = 1.0,
        history_bytes: int = DEFAULT_HISTORY_BYTES,
        history_frames: int = DEFAULT_HISTORY_FRAMES,
        history_max_age: int = 86400,
        client_write_timeout: float = DEFAULT_CLIENT_WRITE_TIMEOUT,
        client_queue_frames: int = DEFAULT_CLIENT_QUEUE_FRAMES,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        max_clients: int = DEFAULT_MAX_CLIENTS,
        inactive_state_max_age: float = DEFAULT_INACTIVE_STATE_MAX_AGE,
        cache_bucket_max: int = DEFAULT_CACHE_BUCKET_MAX,
    ):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.serial_port = serial_port
        self.baudrate = baudrate
        self.reconnect_delay = reconnect_delay
        self.read_timeout = read_timeout
        self.write_timeout = write_timeout
        self.history_bytes = max(history_bytes, 0)
        self.history_frames = max(history_frames, 0)
        self.history_max_age = max(history_max_age, 0)
        self.client_write_timeout = max(client_write_timeout, 0.1)
        self.client_queue_frames = max(client_queue_frames, 1)
        self.poll_interval = max(poll_interval, 0.1)
        self.max_clients = max(max_clients, 1)
        self.inactive_state_max_age = max(inactive_state_max_age, 0.0)
        self._cache_bucket_max = max(cache_bucket_max, 1)

        self._logger = logging.getLogger(__name__)

        self.client_states: dict[str, ClientState] = {}
        self._next_client_slot = 1

        self.serial_conn: Optional["serial.Serial"] = None
        self.backend_lock = asyncio.Lock()
        self.backend_connected = asyncio.Event()
        self.backend_parser = CompanionStreamParser(ord(">"))

        self.history: deque = deque()
        self.history_size_bytes = 0
        self.next_history_seq = 1

        # Generic state cache: packet_type → list of distinct frames.
        # Singleton types (CACHE_SINGLETON_TYPES) keep only the latest frame.
        # All other non-blacklisted types accumulate one entry per distinct frame.
        # Replayed to every client immediately after SELF_INFO.
        self._state_cache: dict[int, list[bytes]] = {}
        # Parallel hash sets for O(1) exact-frame dedup in the catch-all bucket.
        self._state_cache_sets: dict[int, set[bytes]] = {}

        # Set to True while the backend is streaming a contact dump
        # (between CONTACTS_START and END_OF_CONTACTS).
        self._in_contact_dump: bool = False
        self._suppressed_dump_count: int = 0
        # Set when a client mutates contacts (add/update/reset-path): the next
        # GET_CONTACTS forces a full backend refresh to rebuild the stale cache.
        self._contacts_dirty: bool = False
        # True while an authoritative full (since=0) contact dump is rebuilding
        # the cache, so the stale 0x03 bucket is cleared on CONTACTS_START.
        self._contacts_rebuild_pending: bool = False

        self._server: Optional[asyncio.base_events.Server] = None
        self._stop = asyncio.Event()
        self._backend_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._poll_event: asyncio.Event = asyncio.Event()
        self._poll_got_message: bool = False
        # Tracked fire-and-forget tasks (echo injections); awaited on shutdown.
        self._background_tasks: set[asyncio.Task] = set()
        # Monotonically increasing counter for exponential reconnect backoff.
        self._reconnect_attempt: int = 0

    def _q_info(self, state: "ClientState") -> str:
        q = state.outbound_queue
        qsize = q.qsize() if q is not None else -1
        return (
            f"q={qsize}/{self.client_queue_frames} "
            f"pending_msgs={len(state.pending_message_frames)} "
            f"cursor={state.replay_capture_seq} "
            f"connected={'yes' if state.writer is not None else 'NO'}"
        )

    def _make_client_id(self, peer) -> str:
        if isinstance(peer, tuple) and peer:
            return str(peer[0])
        return str(peer or "unknown")

    def _connected_client_count(self) -> int:
        return sum(1 for state in self.client_states.values() if state.writer is not None)

    def _allocate_client_id(self, peer_group: str) -> str:
        client_id = f"{peer_group}#{self._next_client_slot}"
        self._next_client_slot += 1
        return client_id

    def _prune_stale_client_states(self) -> None:
        if self.inactive_state_max_age <= 0:
            return
        now = asyncio.get_running_loop().time()
        stale = [
            cid for cid, s in self.client_states.items()
            if s.writer is None and (now - s.last_disconnect_at) > self.inactive_state_max_age
        ]
        for cid in stale:
            self._logger.info("[client] pruning stale state %s", cid)
            del self.client_states[cid]

    async def start(self) -> None:
        if serial is None:
            raise RuntimeError("pyserial is required. Install it with: pip install pyserial")

        self._server = await asyncio.start_server(
            self._handle_client, host=self.listen_host, port=self.listen_port
        )
        addr = ", ".join(str(sock.getsockname()) for sock in (self._server.sockets or []))
        self._logger.info(
            "[proxy] listening on %s -> serial %s @ %d baud", addr, self.serial_port, self.baudrate
        )
        self._logger.info(
            "[proxy] history: %d frames / %d bytes | poll: %.1fs | max_clients: %d",
            self.history_frames, self.history_bytes, self.poll_interval, self.max_clients,
        )

        self._backend_task = asyncio.create_task(self._backend_manager(), name="backend-manager")
        self._poll_task = asyncio.create_task(self._poll_backend_loop(), name="poll-loop")

        try:
            async with self._server:
                await self._stop.wait()
        finally:
            self._logger.info("[proxy] stopping...")

            if self._server:
                self._server.close()
                await self._server.wait_closed()

            await self._close_all_clients()
            await self._close_backend()

            for task in (self._backend_task, self._poll_task):
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

            if self._background_tasks:
                await asyncio.gather(*self._background_tasks, return_exceptions=True)

    async def stop(self) -> None:
        if self._stop.is_set():
            return
        self._stop.set()

        if self._server:
            self._server.close()
            await self._server.wait_closed()

        await self._close_all_clients()
        await self._close_backend()

        for task in (self._backend_task, self._poll_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _poll_backend_loop(self) -> None:
        self._logger.debug("[poll] loop started")
        SYNC_FRAME = bytes((ord("<"), 1, 0, CMD_SYNC_NEXT_MESSAGE))
        poll_count = 0

        while not self._stop.is_set():
            if not self.backend_connected.is_set():
                await asyncio.sleep(0.1)
                continue

            poll_count += 1
            if poll_count % _PRUNE_EVERY_N_POLLS == 0:
                self._prune_stale_client_states()

            self._poll_event.clear()
            self._poll_got_message = False
            await self._send_to_backend(SYNC_FRAME)
            self._logger.debug("[poll] SYNC sent → backend")

            try:
                await asyncio.wait_for(self._poll_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self._logger.debug("[poll] timeout waiting for backend response")
                await asyncio.sleep(self.poll_interval)
                continue

            if not self.backend_connected.is_set():
                continue

            if self._poll_got_message:
                self._logger.debug("[poll] message received, polling again immediately")
            else:
                self._logger.debug("[poll] NO_MORE, sleeping %.1fs", self.poll_interval)
                await asyncio.sleep(self.poll_interval)

    async def _register_client(self, peer_group: str, writer: asyncio.StreamWriter) -> ClientState:
        state = ClientState(
            client_id=self._allocate_client_id(peer_group),
            peer_group=peer_group,
        )
        # Must be created inside the running event loop.
        state.send_lock = asyncio.Lock()
        self.client_states[state.client_id] = state
        state.writer = writer
        state.awaiting_replay_after_self_info = False
        state.last_disconnect_at = 0.0
        state.outbound_queue = asyncio.Queue(maxsize=self.client_queue_frames)
        self._cancel_sender_task(state)
        state.sender_task = asyncio.create_task(
            self._client_sender(state, writer),
            name=f"sender-{state.client_id}",
        )
        return state

    def _adopt_prior_state(self, state: ClientState) -> None:
        """Supersede every prior session sharing this client's app_name.

        A client is identified solely by its app_name (IP-independent), so a
        fresh APP_START is treated as the same logical client reconnecting:
        migrate the furthest cursor and all unserved pending messages from every
        other session with the same app_name, then close those sessions. This
        guarantees a single live session per app_name, which prevents a message
        from being duplicated onto a half-open (zombie) connection and then
        re-delivered as a flood of already-seen messages on the next reconnect.
        """
        if not state.app_name:
            self._logger.info(
                "[client] %s has no app_name, keeping fresh replay state", state.client_id
            )
            return

        priors = [
            other for other in self.client_states.values()
            if other is not state and other.app_name == state.app_name
        ]
        if not priors:
            self._logger.info(
                "[client] %s identified as %s, no prior session",
                state.client_id, state.app_name,
            )
            return

        # Merge cursors (furthest wins) and pending messages (union by seq, so a
        # message already queued on the old session is never delivered twice).
        merged: dict[int, BufferedFrame] = {
            bf.seq: bf for bf in state.pending_message_frames
        }
        writers_to_close: list[asyncio.StreamWriter] = []
        live = 0
        for other in priors:
            if other.replay_capture_seq > state.replay_capture_seq:
                state.replay_capture_seq = other.replay_capture_seq
            for bf in other.pending_message_frames:
                merged.setdefault(bf.seq, bf)
            if other.writer is not None:
                live += 1
                writers_to_close.append(other.writer)
            other.writer = None
            self._reset_client_runtime_state(other)
            self._cancel_sender_task(other)
            other.outbound_queue = None
            self.client_states.pop(other.client_id, None)

        state.pending_message_frames = deque(bf for _, bf in sorted(merged.items()))

        self._logger.info(
            "[client] %s identified as %s — superseded %d prior session(s)%s "
            "(cursor=%d, pending=%d)",
            state.client_id, state.app_name, len(priors),
            f" ({live} still live)" if live else "",
            state.replay_capture_seq, len(state.pending_message_frames),
        )

        for writer in writers_to_close:
            self._close_writer_background(writer)

    def _cancel_sender_task(self, state: ClientState) -> None:
        task = state.sender_task
        state.sender_task = None
        if task and not task.done():
            task.cancel()

    def _reset_client_runtime_state(self, state: ClientState) -> None:
        state.awaiting_replay_after_self_info = False

    def _on_background_task_done(self, task: asyncio.Task) -> None:
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._logger.warning("[task] background %s failed: %s", task.get_name(), exc)

    def _close_writer_background(self, writer: asyncio.StreamWriter) -> None:
        """Close a client writer without blocking the caller.

        Used from the backend broadcast path: awaiting wait_closed() on a
        half-open client there would stall delivery to every other client.
        """
        async def _closer() -> None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

        task = asyncio.create_task(_closer(), name="close-writer")
        self._background_tasks.add(task)
        task.add_done_callback(self._on_background_task_done)

    def _evict_contact_from_cache(self, pubkey: bytes) -> None:
        """Remove a contact (and any cached advert) from the state cache by pubkey."""
        end = PUBKEY_OFFSET + PUBKEY_SIZE
        for pt in (0x03, 0x8A):
            bucket = self._state_cache.get(pt)
            if not bucket:
                continue
            kept = [
                f for f in bucket
                if not (len(f) >= end and f[PUBKEY_OFFSET:end] == pubkey)
            ]
            if len(kept) != len(bucket):
                self._state_cache[pt] = kept
                self._logger.info(
                    "[cache] evicted contact pubkey=%s… from %s (now %d)",
                    pubkey[:4].hex(), fmt_ptype(pt), len(kept),
                )

    async def _client_sender(
        self, state: ClientState, writer: asyncio.StreamWriter
    ) -> None:
        queue = state.outbound_queue
        if queue is None:
            return

        self._logger.debug("[sender/%s] started", state.client_id)
        try:
            while not self._stop.is_set():
                payload = await queue.get()
                if payload is None:
                    self._logger.debug("[sender/%s] sentinel received, exiting", state.client_id)
                    break
                if state.writer is not writer:
                    self._logger.debug("[sender/%s] writer replaced, exiting", state.client_id)
                    break

                writer.write(payload)
                await asyncio.wait_for(writer.drain(), timeout=self.client_write_timeout)
        except asyncio.TimeoutError:
            self._logger.info(
                "[client] send timeout to %s, closing stalled connection", state.client_id
            )
        except asyncio.CancelledError:
            self._logger.debug("[sender/%s] cancelled", state.client_id)
        except Exception as exc:
            self._logger.debug("[sender/%s] exception: %s", state.client_id, exc)
        finally:
            if state.writer is writer:
                state.writer = None
                self._reset_client_runtime_state(state)
                state.last_disconnect_at = asyncio.get_running_loop().time()
                state.outbound_queue = None
            if state.sender_task is asyncio.current_task():
                state.sender_task = None
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def _capture_pending_history(self, state: ClientState) -> None:
        captured_messages = 0
        skipped_old = 0
        cutoff = (time.time() - self.history_max_age) if self.history_max_age > 0 else 0.0

        # history is ordered by seq; drop already-seen frames without iterating them.
        for buffered in itertools.dropwhile(
            lambda bf: bf.seq <= state.replay_capture_seq, self.history
        ):
            state.replay_capture_seq = buffered.seq

            if get_packet_type(buffered.data) not in MESSAGE_PACKET_TYPES:
                continue

            if cutoff and buffered.timestamp < cutoff:
                skipped_old += 1
                continue

            state.pending_message_frames.append(buffered)
            captured_messages += 1

        if captured_messages > 0:
            self._logger.info(
                "[client] captured %d buffered messages for %s (up to seq=%d)%s",
                captured_messages, state.client_id, state.replay_capture_seq,
                f", skipped {skipped_old} older than {self.history_max_age}s" if skipped_old else "",
            )
        else:
            self._logger.info(
                "[client] no buffered messages to capture for %s%s",
                state.client_id,
                f" ({skipped_old} too old)" if skipped_old else "",
            )

    async def _send_bytes_to_client(self, state: ClientState, payload: bytes) -> bool:
        if state.send_lock is None:
            return False
        writer_to_close = None
        async with state.send_lock:
            writer = state.writer
            queue = state.outbound_queue
            if writer is None or queue is None or self._stop.is_set():
                self._logger.debug(
                    "[send_bytes→%s] SKIP — writer=%s queue=%s stop=%s",
                    state.client_id,
                    writer is not None, queue is not None, self._stop.is_set(),
                )
                return False

            try:
                queue.put_nowait(payload)
                self._logger.debug(
                    "[send_bytes→%s] enqueued %dB — q=%d/%d",
                    state.client_id, len(payload), queue.qsize(), self.client_queue_frames,
                )
                return True
            except asyncio.QueueFull:
                self._logger.info(
                    "[client] outbound queue full for %s, closing stalled connection",
                    state.client_id,
                )
                # Mark disconnected and release the lock BEFORE the slow TCP close.
                if state.writer is writer:
                    state.writer = None
                    self._reset_client_runtime_state(state)
                    state.last_disconnect_at = asyncio.get_running_loop().time()
                    state.outbound_queue = None
                    writer_to_close = writer
                self._cancel_sender_task(state)
        if writer_to_close is not None:
            # Close in the background: this can run inside the backend broadcast
            # loop, where awaiting wait_closed() on a half-open client would stall
            # delivery to every other client until the OS TCP timeout fires.
            self._close_writer_background(writer_to_close)
        return False

    async def _serve_pending_message(self, state: ClientState) -> bool:
        """Serve the next pending message to the client, or send a local NO_MORE.

        Called whenever the client sends CMD_SYNC_NEXT_MESSAGE.  The backend is
        never consulted; the proxy poll loop keeps pending_message_frames fresh.
        """
        if state.pending_message_frames:
            buffered = state.pending_message_frames.popleft()
            self._logger.debug(
                "[serve/%s] seq=%d size=%dB remaining=%d",
                state.client_id, buffered.seq, len(buffered.data),
                len(state.pending_message_frames),
            )
            if await self._send_bytes_to_client(state, buffered.data):
                self._logger.info(
                    "[client] served message seq=%d to %s (remaining=%d)",
                    buffered.seq, state.client_id, len(state.pending_message_frames),
                )
                return True
            self._logger.debug(
                "[serve/%s] send failed — re-queuing seq=%d", state.client_id, buffered.seq
            )
            state.pending_message_frames.appendleft(buffered)
            return False
        else:
            self._logger.debug("[serve/%s] nothing pending → local NO_MORE", state.client_id)
            return await self._send_bytes_to_client(
                state, build_backend_frame(RESP_CODE_NO_MORE_MESSAGES)
            )

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        peer_group = self._make_client_id(peer)

        if self._connected_client_count() >= self.max_clients:
            self._logger.info(
                "[client] rejected %s — max_clients=%d reached", peer, self.max_clients
            )
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            return

        # Enable TCP keepalive so the OS detects half-open (zombie) connections.
        # Without this, a client that disappears without a TCP FIN/RST can leave
        # a phantom active connection for hours if no data is written to it.
        _sock = writer.transport.get_extra_info("socket")
        if _sock is not None:
            try:
                _sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                if hasattr(socket, "TCP_KEEPIDLE"):   # Linux
                    _sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
                if hasattr(socket, "TCP_KEEPINTVL"):
                    _sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
                if hasattr(socket, "TCP_KEEPCNT"):
                    _sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
            except Exception as exc:
                self._logger.debug("[client] could not set keepalive on %s: %s", peer, exc)

        state = await self._register_client(peer_group, writer)
        self._logger.info(
            "[client] connected %s as %s (clients=%d)",
            peer, state.client_id, self._connected_client_count(),
        )

        try:
            while not self._stop.is_set():
                data = await reader.read(65536)
                if not data:
                    break

                forward_frames: list[bytes] = []
                for frame in state.client_parser.feed(data):
                    if len(frame) < 4:
                        forward_frames.append(frame)
                        continue

                    command = frame[3]
                    if command == CMD_APP_START:
                        if len(frame) > 11:
                            app_name = frame[11:].decode("utf-8", errors="ignore").rstrip("\x00")
                            state.app_name = app_name
                        # Use app_name as the stable peer identity so that two
                        # different apps on the same IP are never mixed up, and
                        # a reconnecting app recovers its own state regardless of
                        # IP changes (e.g. mobile switching networks).
                        if state.app_name:
                            state.peer_group = state.app_name
                        self._adopt_prior_state(state)
                        self._capture_pending_history(state)

                        cached_self_info = (self._state_cache.get(RESP_CODE_SELF_INFO) or [None])[0]
                        if cached_self_info is not None:
                            # Cache is warm: serve the response locally without
                            # touching the backend.  Forwarding APP_START would
                            # trigger a full contact dump from the backend which
                            # blocks other client commands for several seconds.
                            self._logger.info(
                                "[client] APP_START from %s app_name=%s — served from cache",
                                state.client_id, state.app_name or "<empty>",
                            )
                            await self._send_bytes_to_client(state, cached_self_info)
                            await self._replay_state_cache(state)
                            # Do NOT append to forward_frames — backend never sees this APP_START.
                        else:
                            # Cache is cold (first ever connection): must ask the backend.
                            self._logger.info(
                                "[client] APP_START from %s app_name=%s — cache cold, forwarding",
                                state.client_id, state.app_name or "<empty>",
                            )
                            state.awaiting_replay_after_self_info = True
                            forward_frames.append(frame)
                    elif command == CMD_GET_CONTACTS:
                        cached_contacts = self._state_cache.get(0x03, [])
                        if cached_contacts and not self._contacts_dirty:
                            since: int = 0
                            if len(frame) >= 8:
                                since = struct.unpack_from("<I", frame, 4)[0]
                            filtered = [
                                f for f in cached_contacts
                                if len(f) >= CONTACT_LASTMOD_OFFSET + 4
                                and struct.unpack_from("<I", f, CONTACT_LASTMOD_OFFSET)[0] > since
                            ]
                            most_recent = max(
                                (struct.unpack_from("<I", f, CONTACT_LASTMOD_OFFSET)[0]
                                 for f in filtered),
                                default=since,
                            )
                            n = len(filtered)
                            contacts_start = bytes([ord(">"), 5, 0, 0x02]) + struct.pack("<I", n)
                            await self._send_bytes_to_client(state, contacts_start)
                            for cf in filtered:
                                await self._send_bytes_to_client(state, cf)
                            eoc = bytes([ord(">"), 5, 0, 0x04]) + struct.pack("<I", most_recent)
                            await self._send_bytes_to_client(state, eoc)
                            self._logger.debug(
                                "[cli→%s] GET_CONTACTS since=%d → %d/%d contact(s) from cache",
                                state.client_id, since, n, len(cached_contacts),
                            )
                        elif self._contacts_dirty:
                            # A contact was mutated: rebuild the cache from an
                            # authoritative full (since=0) backend dump instead of
                            # serving stale data.
                            self._contacts_dirty = False
                            self._contacts_rebuild_pending = True
                            forward_frames.append(bytes((ord("<"), 1, 0, CMD_GET_CONTACTS)))
                            self._logger.debug(
                                "[cli→%s] GET_CONTACTS — cache dirty, forcing full refresh",
                                state.client_id,
                            )
                        else:
                            self._logger.debug(
                                "[cli→%s] GET_CONTACTS — cache cold, forwarding", state.client_id
                            )
                            forward_frames.append(frame)
                    elif command == CMD_REMOVE_CONTACT and len(frame) >= PUBKEY_OFFSET + PUBKEY_SIZE:
                        # Forward the delete AND evict the contact from the cache
                        # immediately, so reconnecting clients are not re-served a
                        # ghost contact (which would reappear in the app and then
                        # fail on a second delete with ERR_NOT_FOUND).
                        pubkey = frame[PUBKEY_OFFSET:PUBKEY_OFFSET + PUBKEY_SIZE]
                        forward_frames.append(frame)
                        self._evict_contact_from_cache(pubkey)
                    elif command in (CMD_ADD_UPDATE_CONTACT, CMD_RESET_PATH):
                        # The backend mutates the contact (rename / path / flags) but
                        # does not echo a fresh CONTACT frame, so the cached copy goes
                        # stale. Mark the cache dirty to force a refresh on the next
                        # GET_CONTACTS.
                        forward_frames.append(frame)
                        self._contacts_dirty = True
                        self._logger.debug(
                            "[cli→%s] contact mutation cmd=0x%02X → cache marked dirty",
                            state.client_id, command,
                        )
                    elif command == CMD_SYNC_NEXT_MESSAGE:
                        # Always served locally from the proxy cache.
                        self._logger.debug("[cli→%s] SYNC → serving locally", state.client_id)
                        await self._serve_pending_message(state)
                    elif command == 0x1F and len(frame) >= 5:
                        # CMD_GET_CHANNEL: serve from cache if available.
                        channel_idx = frame[4]
                        cached_channels = self._state_cache.get(0x12, [])
                        cached = next(
                            (f for f in cached_channels if len(f) > IDX_OFFSET and f[IDX_OFFSET] == channel_idx),
                            None,
                        )
                        if cached is not None:
                            self._logger.debug(
                                "[cli→%s] GET_CHANNEL idx=%d → cache hit", state.client_id, channel_idx
                            )
                            await self._send_bytes_to_client(state, cached)
                        else:
                            self._logger.debug(
                                "[cli→%s] GET_CHANNEL idx=%d → cache miss, forwarding",
                                state.client_id, channel_idx,
                            )
                            forward_frames.append(frame)
                    elif command == CMD_SEND_CHAN_MSG and len(frame) >= 11:
                        # frame[3]=0x03, frame[4]=0x00, frame[5]=chan_idx,
                        # frame[6:10]=timestamp, frame[10:]=text
                        chan_idx  = frame[5]
                        text_data = frame[10:]
                        forward_frames.append(frame)
                        task = asyncio.create_task(
                            self._inject_sent_echo(chan_idx, text_data, state.client_id),
                            name=f"echo-ch{chan_idx}-{state.client_id}",
                        )
                        self._background_tasks.add(task)
                        task.add_done_callback(self._on_background_task_done)
                    else:
                        forward_frames.append(frame)

                if forward_frames:
                    payload = b"".join(forward_frames)
                    if self.backend_connected.is_set():
                        await self._send_to_backend(payload)
                    else:
                        self._logger.info(
                            "[proxy] serial backend down, dropping client->backend bytes"
                        )

        except (asyncio.CancelledError, ConnectionError):
            pass
        except Exception as exc:
            self._logger.info("[client] error %s: %s", peer, exc)
        finally:
            if state.writer is writer:
                state.writer = None
                self._reset_client_runtime_state(state)
                state.last_disconnect_at = asyncio.get_running_loop().time()
                self._cancel_sender_task(state)
                state.outbound_queue = None
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            # Drop never-identified phantom connections immediately instead of
            # keeping them until the 24h prune: they can never be re-adopted and
            # would be scanned on every backend frame.
            if not state.app_name:
                self.client_states.pop(state.client_id, None)
            self._logger.info(
                "[client] disconnected %s as %s (clients=%d)",
                peer, state.client_id, self._connected_client_count(),
            )

    async def _backend_manager(self) -> None:
        while not self._stop.is_set():
            await self._connect_backend()

            if not self.backend_connected.is_set():
                # Exponential backoff: 1s, 2s, 4s, … capped at DEFAULT_RECONNECT_BACKOFF_MAX.
                delay = min(
                    self.reconnect_delay * (2 ** min(self._reconnect_attempt, 10)),
                    DEFAULT_RECONNECT_BACKOFF_MAX,
                )
                self._reconnect_attempt += 1
                self._logger.info(
                    "[backend] retry in %.1fs (attempt %d)", delay, self._reconnect_attempt
                )
                await asyncio.sleep(delay)
                continue

            self._reconnect_attempt = 0
            try:
                while not self._stop.is_set() and self.backend_connected.is_set():
                    conn = self.serial_conn
                    if conn is None:
                        break
                    data = await asyncio.to_thread(conn.read, 65536)
                    if not data:
                        continue

                    for frame in self.backend_parser.feed(data):
                        await self._handle_backend_frame(frame)
            except asyncio.CancelledError:
                raise
            except (OSError, SerialException) as exc:
                self._logger.info("[backend] disconnected: %s", exc)
            except Exception as exc:
                self._logger.info("[backend] read error: %s", exc)
            finally:
                self.backend_connected.clear()
                await self._close_backend()

            await asyncio.sleep(self.reconnect_delay)

    async def _connect_backend(self) -> None:
        try:
            self._logger.info("[backend] opening %s @ %d ...", self.serial_port, self.baudrate)
            conn = await asyncio.to_thread(
                serial.Serial,
                self.serial_port,
                self.baudrate,
                timeout=self.read_timeout,
                write_timeout=self.write_timeout,
            )
            self.serial_conn = conn
            self.backend_parser.reset()
            self.backend_connected.set()
            self._logger.info("[backend] connected")
        except Exception as exc:
            self.backend_connected.clear()
            self.serial_conn = None
            self._logger.info("[backend] connect failed: %s", exc)

    async def _close_backend(self) -> None:
        self.backend_connected.clear()
        self.backend_parser.reset()
        # Reset contact-dump tracking so stale state cannot leak across reconnects.
        self._in_contact_dump = False
        self._suppressed_dump_count = 0
        self._contacts_rebuild_pending = False
        # Unblock the poll loop if it is waiting for a backend response.
        self._poll_event.set()
        conn = self.serial_conn
        self.serial_conn = None
        if conn:
            try:
                await asyncio.to_thread(conn.close)
            except Exception:
                pass

    async def _close_all_clients(self) -> None:
        writers = [state.writer for state in self.client_states.values() if state.writer is not None]
        for state in self.client_states.values():
            state.writer = None
            self._reset_client_runtime_state(state)
            state.last_disconnect_at = asyncio.get_running_loop().time()
            self._cancel_sender_task(state)
            state.outbound_queue = None

        for writer in writers:
            try:
                writer.close()
            except Exception:
                pass
        for writer in writers:
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _send_to_backend(self, data: bytes) -> None:
        async with self.backend_lock:
            conn = self.serial_conn
            if not conn or self._stop.is_set():
                return
            try:
                # Bound the blocking write. The explicit flush() was removed: on
                # Windows pyserial's flush() is an unbounded busy-wait that ignores
                # write_timeout, so a wedged USB-CDC device would block here forever
                # while holding backend_lock and freeze the whole proxy. write()
                # with write_timeout already hands the bytes off to the OS.
                await asyncio.wait_for(
                    asyncio.to_thread(conn.write, data),
                    timeout=self.write_timeout + 1.0,
                )
            except Exception as exc:
                self._logger.info("[backend] write error: %s", exc)
                # Tear the connection down so the read loop exits and reconnects;
                # otherwise backend_connected stays cleared while the read loop keeps
                # spinning on empty reads, permanently freezing message delivery.
                self.backend_connected.clear()
                self._poll_event.set()
                try:
                    conn.cancel_read()
                except Exception:
                    pass

    async def _inject_sent_echo(self, channel_idx: int, text_data: bytes, exclude_client_id: str) -> None:
        """Inject a synthetic CHANNEL_MSG_RECV_V3 for a message sent by a client.

        All connected clients except the sender receive the message via the
        normal PUSH_MSG_WAITING + SYNC mechanism, so both apps see sent messages.
        """
        timestamp = int(time.time())

        payload = bytes([
            0x11,           # CHANNEL_MSG_RECV_V3
            0,              # SNR = 0 (synthetic, no RF)
            0, 0,           # reserved
            channel_idx,    # target channel
            0xFF,           # plen = 255 (flood path)
            0,              # txt_type = plain text
        ]) + timestamp.to_bytes(4, "little") + text_data

        frame = bytes([ord(">"), len(payload) & 0xFF, (len(payload) >> 8) & 0xFF]) + payload
        self._logger.info(
            "[echo] injecting sent msg ch=%d text=%r excluding=%s",
            channel_idx, text_data.decode("utf-8", "ignore"), exclude_client_id,
        )

        # Pass exclude_ids instead of mutating client_states around an await —
        # avoids a race where a reconnecting client's state could be overwritten.
        # synthetic=True prevents the echo from waking the backend poll loop as if a
        # real backend message had arrived, which would cause a spurious SYNC cycle.
        await self._handle_backend_frame(
            frame, exclude_ids=frozenset({exclude_client_id}), synthetic=True
        )

    def _append_history(self, frame: bytes) -> int:
        seq = self.next_history_seq
        self.next_history_seq += 1
        self.history.append(BufferedFrame(seq=seq, data=frame))
        self.history_size_bytes += len(frame)

        evicted_count = 0
        while self.history and (
            (self.history_frames and len(self.history) > self.history_frames)
            or (self.history_bytes and self.history_size_bytes > self.history_bytes)
        ):
            evicted = self.history.popleft()
            self.history_size_bytes -= len(evicted.data)
            evicted_count += 1

        self._logger.debug(
            "[history] appended seq=%d size=%dB total=%d frames / %dB evicted=%d",
            seq, len(frame), len(self.history), self.history_size_bytes, evicted_count,
        )
        return seq

    async def _handle_backend_frame(
        self,
        frame: bytes,
        exclude_ids: Optional[frozenset] = None,
        synthetic: bool = False,
    ) -> None:
        packet_type = get_packet_type(frame)
        is_msg = is_message_frame(frame)
        history_seq: Optional[int] = None

        if is_msg:
            history_seq = self._append_history(frame)

        connected_clients = [s for s in self.client_states.values() if s.writer is not None]
        self._logger.debug(
            "[bkd←frame] type=%s size=%dB is_msg=%s seq=%s connected_clients=%d/%d",
            fmt_ptype(packet_type), len(frame), is_msg, history_seq,
            len(connected_clients), len(self.client_states),
        )

        # Update generic state cache so reconnecting clients get a full snapshot.
        if packet_type is not None and packet_type not in CACHE_BLACKLIST_TYPES and not is_msg:
            if packet_type in CACHE_SINGLETON_TYPES:
                self._state_cache[packet_type] = [frame]
                self._logger.debug("[cache] singleton updated type=%s", fmt_ptype(packet_type))
            elif packet_type in CACHE_DEDUP_BY_PUBKEY_TYPES:
                end = PUBKEY_OFFSET + PUBKEY_SIZE
                if len(frame) >= end:
                    pubkey = frame[PUBKEY_OFFSET:end]
                    bucket = self._state_cache.setdefault(packet_type, [])
                    for i, existing in enumerate(bucket):
                        if existing[PUBKEY_OFFSET:end] == pubkey:
                            if existing != frame:
                                bucket[i] = frame
                                self._logger.debug(
                                    "[cache] updated type=%s pubkey=%s… total=%d",
                                    fmt_ptype(packet_type), pubkey[:4].hex(), len(bucket),
                                )
                            break
                    else:
                        bucket.append(frame)
                        self._logger.debug(
                            "[cache] new type=%s pubkey=%s… total=%d",
                            fmt_ptype(packet_type), pubkey[:4].hex(), len(bucket),
                        )
            elif packet_type in CACHE_DEDUP_BY_IDX_TYPES:
                if len(frame) > IDX_OFFSET:
                    idx = frame[IDX_OFFSET]
                    bucket = self._state_cache.setdefault(packet_type, [])
                    for i, existing in enumerate(bucket):
                        if len(existing) > IDX_OFFSET and existing[IDX_OFFSET] == idx:
                            if existing != frame:
                                bucket[i] = frame
                                self._logger.debug(
                                    "[cache] updated type=%s idx=%d total=%d",
                                    fmt_ptype(packet_type), idx, len(bucket),
                                )
                            break
                    else:
                        bucket.append(frame)
                        self._logger.debug(
                            "[cache] new type=%s idx=%d total=%d",
                            fmt_ptype(packet_type), idx, len(bucket),
                        )
            else:
                bucket = self._state_cache.setdefault(packet_type, [])
                seen = self._state_cache_sets.setdefault(packet_type, set())
                # O(1) hash lookup via parallel set; list preserves insertion order for replay.
                if len(bucket) < self._cache_bucket_max and frame not in seen:
                    bucket.append(frame)
                    seen.add(frame)
                    self._logger.debug(
                        "[cache] collected type=%s total=%d", fmt_ptype(packet_type), len(bucket)
                    )

        # Track whether we are currently inside a backend contact dump.
        if packet_type == 0x02:  # CONTACTS_START
            self._in_contact_dump = True
            self._suppressed_dump_count = 0
            if self._contacts_rebuild_pending:
                # Authoritative full refresh: drop the stale contact cache so
                # backend-side deletions and renames are reflected after rebuild.
                self._state_cache[0x03] = []
                self._logger.debug("[cache] contact cache cleared for full rebuild")
            self._logger.debug("[cache] backend contact dump started")
        elif packet_type == 0x04:  # END_OF_CONTACTS
            self._in_contact_dump = False
            self._contacts_rebuild_pending = False
            if self._suppressed_dump_count:
                self._logger.debug(
                    "[cache] backend contact dump done: %d contact(s) received and cached",
                    self._suppressed_dump_count,
                )
                self._suppressed_dump_count = 0
        elif packet_type == 0x8F:  # PUSH_CONTACT_DELETED — backend auto-evicted a contact
            if len(frame) >= PUBKEY_OFFSET + PUBKEY_SIZE:
                self._evict_contact_from_cache(frame[PUBKEY_OFFSET:PUBKEY_OFFSET + PUBKEY_SIZE])

        # Signal the poll loop only for real backend frames, not synthetic echoes.
        if not synthetic:
            if is_msg:
                self._poll_got_message = True
                self._poll_event.set()
            elif packet_type == RESP_CODE_NO_MORE_MESSAGES:
                self._poll_got_message = False
                self._poll_event.set()

        for state in list(self.client_states.values()):
            if exclude_ids and state.client_id in exclude_ids:
                # Advance cursor even for excluded clients so they don't receive
                # the echo again as a "missed message" if they reconnect.
                if history_seq is not None and history_seq > state.replay_capture_seq:
                    state.replay_capture_seq = history_seq
                continue
            if state.writer is None:
                self._logger.debug("[bkd→%s] SKIP — not connected", state.client_id)
                continue
            await self._send_frame_to_client(state, frame, packet_type, history_seq)

    async def _replay_state_cache(self, state: ClientState) -> None:
        """Replay all cached backend state to a client that just received SELF_INFO."""
        total = 0
        self._logger.info(
            "[cache] snapshot before replay to %s: %s",
            state.client_id,
            ", ".join(f"{fmt_ptype(pt)}×{len(fs)}" for pt, fs in self._state_cache.items()),
        )
        # Replay singleton types first (SELF_INFO already sent — skip it),
        # then collections, preserving insertion order within each bucket.
        for packet_type, frames in list(self._state_cache.items()):
            if packet_type == RESP_CODE_SELF_INFO:
                continue
            if packet_type == 0x03:
                # Wrap CONTACT frames with synthetic delimiters.
                snapshot = list(frames)
                n = len(snapshot)
                contacts_start = bytes([ord(">"), 5, 0, 0x02]) + struct.pack("<I", n)
                await self._send_bytes_to_client(state, contacts_start)
                most_recent = 0
                for frame in snapshot:
                    await self._send_bytes_to_client(state, frame)
                    total += 1
                    if len(frame) >= CONTACT_LASTMOD_OFFSET + 4:
                        lm = struct.unpack_from("<I", frame, CONTACT_LASTMOD_OFFSET)[0]
                        if lm > most_recent:
                            most_recent = lm
                end_of_contacts = bytes([ord(">"), 5, 0, 0x04]) + struct.pack("<I", most_recent)
                await self._send_bytes_to_client(state, end_of_contacts)
                continue
            for frame in list(frames):
                await self._send_bytes_to_client(state, frame)
                total += 1

        n_types = sum(1 for pt in self._state_cache if pt != RESP_CODE_SELF_INFO)
        self._logger.info(
            "[client] replayed state cache to %s: %d frame(s) across %d type(s)",
            state.client_id, total, n_types,
        )
        if state.pending_message_frames:
            self._logger.info(
                "[client] %d buffered message(s) pending for %s — will serve on next SYNC",
                len(state.pending_message_frames), state.client_id,
            )
        else:
            self._logger.info("[client] no buffered messages pending for %s", state.client_id)

    async def _send_frame_to_client(
        self,
        state: ClientState,
        frame: bytes,
        packet_type: Optional[int],
        history_seq: Optional[int],
    ) -> None:
        # NO_MORE from the backend is handled by the poll loop; clients receive a
        # locally-generated NO_MORE when they SYNC and nothing is pending.
        if packet_type == RESP_CODE_NO_MORE_MESSAGES:
            return

        # PUSH_MSG_WAITING from the backend arrives before the poll loop has
        # retrieved the message.  The proxy generates its own 0x83 after
        # queueing, so suppress the backend's to avoid a spurious SYNC/NO_MORE
        # cycle on the client.
        if packet_type == 0x83:
            return

        # Message frames go into the client's pending queue and are served on the
        # client's next CMD_SYNC_NEXT_MESSAGE.
        if history_seq is not None:
            if history_seq > state.replay_capture_seq:
                state.pending_message_frames.append(BufferedFrame(seq=history_seq, data=frame))
                state.replay_capture_seq = history_seq
                self._logger.debug(
                    "[bkd→%s] QUEUED seq=%d pending=%d",
                    state.client_id, history_seq, len(state.pending_message_frames),
                )
                self._logger.info(
                    "[client] queued message seq=%d for %s (pending=%d)",
                    history_seq, state.client_id, len(state.pending_message_frames),
                )
                # Notify the client that a message is waiting so it sends
                # CMD_SYNC_NEXT_MESSAGE.  Done AFTER queueing to avoid a race
                # where the client SYNCs before the frame is in pending queue.
                await self._send_bytes_to_client(state, build_backend_frame(0x83))
            else:
                self._logger.debug(
                    "[bkd→%s] SKIP msg seq=%d — already at cursor=%d",
                    state.client_id, history_seq, state.replay_capture_seq,
                )
            return

        if self._in_contact_dump and packet_type == 0x03:
            if state is next(iter(self.client_states.values()), None):
                self._suppressed_dump_count += 1

        self._logger.debug(
            "[bkd→%s] PUSH type=%s size=%dB", state.client_id, fmt_ptype(packet_type), len(frame)
        )
        if not await self._send_bytes_to_client(state, frame):
            self._logger.debug("[bkd→%s] PUSH FAILED — client dropped", state.client_id)
            return

        if packet_type == RESP_CODE_SELF_INFO and state.awaiting_replay_after_self_info:
            state.awaiting_replay_after_self_info = False
            await self._replay_state_cache(state)


def parse_args():
    ap = argparse.ArgumentParser(
        description="Serial broadcast proxy with per-client message replay for MeshCore companion links"
    )
    ap.add_argument("--listen-host", default="0.0.0.0")
    ap.add_argument("--listen-port", type=int, required=True)
    ap.add_argument(
        "--serial-port", required=True, help="Serial device, e.g. COM7 or /dev/ttyACM0"
    )
    ap.add_argument("--baudrate", type=int, default=115200)
    ap.add_argument("--reconnect-delay", type=float, default=1.0)
    ap.add_argument("--read-timeout", type=float, default=0.2)
    ap.add_argument("--write-timeout", type=float, default=1.0)
    ap.add_argument("--history-bytes", type=int, default=DEFAULT_HISTORY_BYTES)
    ap.add_argument("--history-frames", type=int, default=DEFAULT_HISTORY_FRAMES)
    ap.add_argument(
        "--history-max-age",
        type=int,
        default=86400,
        help="Max age in seconds for replayed messages (0 = no limit, default: 86400 = 24h)",
    )
    ap.add_argument("--client-write-timeout", type=float, default=DEFAULT_CLIENT_WRITE_TIMEOUT)
    ap.add_argument("--client-queue-frames", type=int, default=DEFAULT_CLIENT_QUEUE_FRAMES)
    ap.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL,
        help="Seconds to wait between backend polls when the queue is empty (default: %(default)s)",
    )
    ap.add_argument(
        "--max-clients",
        type=int,
        default=DEFAULT_MAX_CLIENTS,
        help="Maximum simultaneous TCP connections (default: %(default)s)",
    )
    ap.add_argument(
        "--inactive-state-max-age",
        type=float,
        default=DEFAULT_INACTIVE_STATE_MAX_AGE,
        help="Seconds before an inactive client state is pruned (0 = never, default: %(default)s). "
             "Should be >= --history-max-age to avoid full replays on reconnect.",
    )
    ap.add_argument(
        "--cache-bucket-max",
        type=int,
        default=DEFAULT_CACHE_BUCKET_MAX,
        help="Max frames per catch-all cache bucket (default: %(default)s)",
    )
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--debug", action="store_true", help="Enable verbose debug logging")
    return ap.parse_args()


async def main_async():
    args = parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else (logging.WARNING if args.quiet else logging.INFO),
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )

    proxy = SerialBroadcastProxy(
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        serial_port=args.serial_port,
        baudrate=args.baudrate,
        reconnect_delay=args.reconnect_delay,
        read_timeout=args.read_timeout,
        write_timeout=args.write_timeout,
        history_bytes=args.history_bytes,
        history_frames=args.history_frames,
        history_max_age=args.history_max_age,
        client_write_timeout=args.client_write_timeout,
        client_queue_frames=args.client_queue_frames,
        poll_interval=args.poll_interval,
        max_clients=args.max_clients,
        inactive_state_max_age=args.inactive_state_max_age,
        cache_bucket_max=args.cache_bucket_max,
    )

    loop = asyncio.get_running_loop()
    _stop_task: Optional[asyncio.Task] = None

    def _handle_signal():
        nonlocal _stop_task
        if _stop_task is None or _stop_task.done():
            _stop_task = asyncio.create_task(proxy.stop(), name="signal-stop")

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    await proxy.start()


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
