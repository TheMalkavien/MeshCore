#!/usr/bin/env python3
"""
TCP broadcast proxy for MeshCore companion links:
- Multi clients -> single backend connection
- Proxy polls backend at a fixed interval; results cached locally
- Client CMD_SYNC_NEXT_MESSAGE always served from local cache (never forwarded)
- Reconnecting clients replay only missed messages from the proxy history
"""

import argparse
import asyncio
import signal
import struct
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional


CMD_APP_START = 0x01
CMD_GET_CONTACTS = 0x04
CMD_SYNC_NEXT_MESSAGE = 0x0A
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
    0x0F,  # DISABLED        — transient feature-flag response
    0x02,  # CONTACTS_START  — delimiter, rebuilt from CONTACT cache on replay
    0x04,  # END_OF_CONTACTS — delimiter, rebuilt from CONTACT cache on replay
    0x13,  # SIGN_START      — transient signing session
    0x14,  # SIGNATURE       — transient signing result
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

# RESP_CODE_CONTACT_MSG_RECV / CHANNEL_MSG_RECV  (v<3 : 0x07, 0x08)
# RESP_CODE_CONTACT_MSG_RECV_V3 / CHANNEL_MSG_RECV_V3  (v≥3 : 0x10, 0x11)
MESSAGE_PACKET_TYPES = {0x07, 0x08, 0x10, 0x11}

PACKET_TYPE_NAMES: Dict[int, str] = {
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

    def feed(self, data: bytes) -> List[bytes]:
        if data:
            self.buffer.extend(data)

        frames: List[bytes] = []
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

            frame_len = self.buffer[1] | (self.buffer[2] << 8)
            if frame_len < 0 or frame_len > self.max_frame_size:
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


@dataclass
class ClientState:
    client_id: str
    peer_group: str
    app_name: str = ""
    writer: Optional[asyncio.StreamWriter] = None
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    client_parser: CompanionStreamParser = field(
        default_factory=lambda: CompanionStreamParser(ord("<"))
    )
    awaiting_replay_after_self_info: bool = False
    replay_capture_seq: int = 0
    pending_message_frames: Deque[BufferedFrame] = field(default_factory=deque)
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


class BroadcastProxy:
    def __init__(
        self,
        listen_host: str,
        listen_port: int,
        backend_host: str,
        backend_port: int,
        backend_reconnect_delay: float = 1.0,
        history_bytes: int = DEFAULT_HISTORY_BYTES,
        history_frames: int = DEFAULT_HISTORY_FRAMES,
        client_write_timeout: float = DEFAULT_CLIENT_WRITE_TIMEOUT,
        client_queue_frames: int = DEFAULT_CLIENT_QUEUE_FRAMES,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        log: bool = True,
        debug: bool = False,
    ):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.backend_host = backend_host
        self.backend_port = backend_port
        self.backend_reconnect_delay = backend_reconnect_delay
        self.history_bytes = max(history_bytes, 0)
        self.history_frames = max(history_frames, 0)
        self.client_write_timeout = max(client_write_timeout, 0.1)
        self.client_queue_frames = max(client_queue_frames, 1)
        self.poll_interval = max(poll_interval, 0.1)
        self.log = log
        self.debug = debug

        self.client_states: Dict[str, ClientState] = {}
        self._next_client_slot = 1

        self.backend_reader: Optional[asyncio.StreamReader] = None
        self.backend_writer: Optional[asyncio.StreamWriter] = None
        self.backend_lock = asyncio.Lock()
        self.backend_connected = asyncio.Event()
        self.backend_parser = CompanionStreamParser(ord(">"))

        self.history: Deque[BufferedFrame] = deque()
        self.history_size_bytes = 0
        self.next_history_seq = 1

        # Generic state cache: packet_type → list of distinct frames.
        # Singleton types (CACHE_SINGLETON_TYPES) keep only the latest frame.
        # All other non-blacklisted types accumulate one entry per distinct frame.
        # Replayed to every client immediately after SELF_INFO.
        self._state_cache: Dict[int, List[bytes]] = {}

        # Set to True while the backend is streaming a contact dump
        # (between CONTACTS_START and END_OF_CONTACTS).  While True, contacts
        # are still cached but forwarding to all clients is suppressed — they
        # have already received the full contact list via cache replay.
        self._in_contact_dump: bool = False
        self._suppressed_dump_count: int = 0  # contacts suppressed in current dump

        self._server: Optional[asyncio.base_events.Server] = None
        self._stop = asyncio.Event()
        self._backend_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        # Event set by _handle_backend_frame to wake the poll loop after each
        # backend response (MSG or NO_MORE).
        self._poll_event: asyncio.Event = asyncio.Event()
        self._poll_got_message: bool = False

    def _p(self, msg: str) -> None:
        if self.log:
            print(msg, flush=True)

    def _d(self, msg: str) -> None:
        if self.debug:
            print(f"[DBG] {msg}", flush=True)

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

    def _find_matching_inactive_state(self, current: ClientState) -> Optional[ClientState]:
        reusable: List[ClientState] = [
            state
            for state in self.client_states.values()
            if state is not current
            and state.peer_group == current.peer_group
            and state.writer is None
            and state.app_name == current.app_name
        ]
        if not reusable:
            return None
        reusable.sort(key=lambda state: state.last_disconnect_at, reverse=True)
        return reusable[0]

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, host=self.listen_host, port=self.listen_port
        )
        addr = ", ".join(str(sock.getsockname()) for sock in (self._server.sockets or []))
        self._p(
            f"[proxy] listening on {addr} -> backend {self.backend_host}:{self.backend_port}"
        )
        self._p(
            f"[proxy] message history enabled: {self.history_frames} frames / {self.history_bytes} bytes"
        )
        self._p(f"[proxy] backend poll interval: {self.poll_interval}s")

        self._backend_task = asyncio.create_task(self._backend_manager())
        self._poll_task = asyncio.create_task(self._poll_backend_loop())

        try:
            async with self._server:
                await self._stop.wait()
        finally:
            self._p("[proxy] stopping...")

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

    async def stop(self) -> None:
        if self._stop.is_set():
            return
        self._stop.set()

        if self._server:
            self._server.close()
        await self._close_all_clients()
        await self._close_backend()

    async def _poll_backend_loop(self) -> None:
        """Continuously poll the backend for new messages.

        Drains the backend queue as fast as possible when messages are
        available, then waits poll_interval seconds before polling again.
        Client CMD_SYNC_NEXT_MESSAGE frames are never forwarded to the backend;
        they are always answered locally from pending_message_frames.
        """
        self._d("[poll] loop started")
        SYNC_FRAME = bytes((ord("<"), 1, 0, CMD_SYNC_NEXT_MESSAGE))

        while not self._stop.is_set():
            if not self.backend_connected.is_set():
                await asyncio.sleep(0.1)
                continue

            self._poll_event.clear()
            self._poll_got_message = False
            await self._send_to_backend(SYNC_FRAME)
            self._d("[poll] SYNC sent → backend")

            # Wait for _handle_backend_frame to signal us (MSG or NO_MORE)
            try:
                await asyncio.wait_for(self._poll_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self._d("[poll] timeout waiting for backend response")
                await asyncio.sleep(self.poll_interval)
                continue

            if not self.backend_connected.is_set():
                # Woken by _close_backend; restart the outer loop
                continue

            if self._poll_got_message:
                # A message was received — immediately poll again to drain the queue
                self._d("[poll] message received, polling again immediately")
            else:
                # No more messages — wait before next poll
                self._d(f"[poll] NO_MORE, sleeping {self.poll_interval}s")
                await asyncio.sleep(self.poll_interval)

    async def _register_client(self, peer_group: str, writer: asyncio.StreamWriter) -> ClientState:
        state = ClientState(
            client_id=self._allocate_client_id(peer_group),
            peer_group=peer_group,
        )
        self.client_states[state.client_id] = state
        state.writer = writer
        state.awaiting_replay_after_self_info = False
        state.last_disconnect_at = 0.0
        state.outbound_queue = asyncio.Queue(maxsize=self.client_queue_frames)
        self._cancel_sender_task(state)
        state.sender_task = asyncio.create_task(self._client_sender(state, writer))

        return state

    def _adopt_prior_state(self, state: ClientState) -> None:
        if not state.app_name:
            self._p(f"[client] {state.client_id} has no app_name, keeping fresh replay state")
            return

        previous = self._find_matching_inactive_state(state)
        if previous is None:
            self._p(
                f"[client] {state.client_id} identified as {state.peer_group} / {state.app_name}, no prior state"
            )
            return

        if previous.replay_capture_seq > state.replay_capture_seq:
            state.replay_capture_seq = previous.replay_capture_seq

        if previous.pending_message_frames:
            state.pending_message_frames = previous.pending_message_frames
            previous.pending_message_frames = deque()
            self._p(
                f"[client] {state.client_id} transferred {len(state.pending_message_frames)} "
                f"unserved messages from {previous.client_id}"
            )

        self._p(
            f"[client] {state.client_id} identified as {state.peer_group} / {state.app_name}, "
            f"adopting prior state from {previous.client_id} at seq={previous.replay_capture_seq}"
        )
        del self.client_states[previous.client_id]

    def _cancel_sender_task(self, state: ClientState) -> None:
        task = state.sender_task
        state.sender_task = None
        if task and not task.done():
            task.cancel()

    def _reset_client_runtime_state(self, state: ClientState) -> None:
        state.awaiting_replay_after_self_info = False

    async def _client_sender(
        self, state: ClientState, writer: asyncio.StreamWriter
    ) -> None:
        queue = state.outbound_queue
        if queue is None:
            return

        self._d(f"[sender/{state.client_id}] started")
        try:
            while not self._stop.is_set():
                payload = await queue.get()
                if payload is None:
                    self._d(f"[sender/{state.client_id}] sentinel received, exiting")
                    break
                if state.writer is not writer:
                    self._d(f"[sender/{state.client_id}] writer replaced, exiting")
                    break

                writer.write(payload)
                await asyncio.wait_for(writer.drain(), timeout=self.client_write_timeout)
        except asyncio.TimeoutError:
            self._p(f"[client] send timeout to {state.client_id}, closing stalled connection")
            self._d(f"[sender/{state.client_id}] TIMEOUT after {self.client_write_timeout}s")
        except asyncio.CancelledError:
            self._d(f"[sender/{state.client_id}] cancelled")
            pass
        except Exception as exc:
            self._d(f"[sender/{state.client_id}] exception: {exc}")
            pass
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
        for buffered in self.history:
            if buffered.seq <= state.replay_capture_seq:
                continue

            if get_packet_type(buffered.data) in MESSAGE_PACKET_TYPES:
                state.pending_message_frames.append(buffered)
                captured_messages += 1

            state.replay_capture_seq = buffered.seq

        if captured_messages > 0:
            self._p(
                f"[client] captured {captured_messages} buffered messages for {state.client_id} "
                f"(up to seq={state.replay_capture_seq})"
            )
        else:
            self._p(f"[client] no buffered messages to capture for {state.client_id}")

    async def _send_bytes_to_client(self, state: ClientState, payload: bytes) -> bool:
        async with state.send_lock:
            writer = state.writer
            queue = state.outbound_queue
            if writer is None or queue is None or self._stop.is_set():
                self._d(
                    f"[send_bytes→{state.client_id}] SKIP — "
                    f"writer={writer is not None} queue={queue is not None} stop={self._stop.is_set()}"
                )
                return False

            try:
                queue.put_nowait(payload)
                self._d(
                    f"[send_bytes→{state.client_id}] enqueued {len(payload)}B — "
                    f"q={queue.qsize()}/{self.client_queue_frames}"
                )
                return True
            except asyncio.QueueFull:
                self._p(
                    f"[client] outbound queue full for {state.client_id}, closing stalled connection"
                )
                if state.writer is writer:
                    state.writer = None
                    self._reset_client_runtime_state(state)
                    state.last_disconnect_at = asyncio.get_running_loop().time()
                    state.outbound_queue = None
                self._cancel_sender_task(state)
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
                return False

    async def _serve_pending_message(self, state: ClientState) -> bool:
        """Serve the next pending message to the client, or send a local NO_MORE.

        Called whenever the client sends CMD_SYNC_NEXT_MESSAGE.  The backend is
        never consulted; the proxy poll loop keeps pending_message_frames fresh.
        """
        if state.pending_message_frames:
            buffered = state.pending_message_frames.popleft()
            self._d(
                f"[serve/{state.client_id}] seq={buffered.seq} size={len(buffered.data)}B "
                f"remaining={len(state.pending_message_frames)}"
            )
            if await self._send_bytes_to_client(state, buffered.data):
                self._p(
                    f"[client] served message seq={buffered.seq} to {state.client_id} "
                    f"(remaining={len(state.pending_message_frames)})"
                )
                return True
            self._d(f"[serve/{state.client_id}] send failed — re-queuing seq={buffered.seq}")
            state.pending_message_frames.appendleft(buffered)
            return False
        else:
            self._d(f"[serve/{state.client_id}] nothing pending → local NO_MORE")
            return await self._send_bytes_to_client(
                state, build_backend_frame(RESP_CODE_NO_MORE_MESSAGES)
            )

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        peer_group = self._make_client_id(peer)
        state = await self._register_client(peer_group, writer)
        self._p(
            f"[client] connected {peer} as {state.client_id} (clients={self._connected_client_count()})"
        )

        try:
            while not self._stop.is_set():
                data = await reader.read(65536)
                if not data:
                    break

                forward_frames: List[bytes] = []
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
                            self._p(
                                f"[client] APP_START from {state.client_id} "
                                f"app_name={state.app_name or '<empty>'} — "
                                f"served from cache (no backend round-trip)"
                            )
                            await self._send_bytes_to_client(state, cached_self_info)
                            await self._replay_state_cache(state)
                            # Do NOT append to forward_frames — backend never sees this APP_START.
                        else:
                            # Cache is cold (first ever connection): must ask the backend.
                            self._p(
                                f"[client] APP_START from {state.client_id} "
                                f"app_name={state.app_name or '<empty>'} — "
                                f"cache cold, forwarding to backend"
                            )
                            state.awaiting_replay_after_self_info = True
                            forward_frames.append(frame)
                    elif command == CMD_GET_CONTACTS:
                        # Serve the contact list from the proxy cache.  The
                        # cache is kept fresh by APP_START (cold) and
                        # PUSH_NEW_ADVERT events, so no backend round-trip is
                        # needed.  Supports the optional 'since' uint32 LE
                        # filter (frame[4:8]) so incremental syncs work.
                        cached_contacts = self._state_cache.get(0x03, [])
                        if cached_contacts:
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
                            # END_OF_CONTACTS includes most_recent_lastmod so the
                            # app can use it as 'since' on the next call.
                            eoc = bytes([ord(">"), 5, 0, 0x04]) + struct.pack("<I", most_recent)
                            await self._send_bytes_to_client(state, eoc)
                            self._d(
                                f"[cli→{state.client_id}] GET_CONTACTS since={since} "
                                f"→ {n}/{len(cached_contacts)} contact(s) from cache"
                            )
                        else:
                            # Cache cold: forward to backend so cache is populated.
                            self._d(
                                f"[cli→{state.client_id}] GET_CONTACTS — cache cold, forwarding"
                            )
                            forward_frames.append(frame)
                    elif command == CMD_SYNC_NEXT_MESSAGE:
                        # Always served locally from the proxy cache.
                        # The poll loop keeps the cache fresh; client SYNCs are
                        # never forwarded to the backend.
                        self._d(f"[cli→{state.client_id}] SYNC → serving locally")
                        await self._serve_pending_message(state)
                    elif command == 0x1F and len(frame) >= 5:
                        # CMD_GET_CHANNEL: serve from cache if available,
                        # otherwise forward to backend.
                        channel_idx = frame[4]
                        cached_channels = self._state_cache.get(0x12, [])
                        cached = next(
                            (f for f in cached_channels if len(f) > IDX_OFFSET and f[IDX_OFFSET] == channel_idx),
                            None,
                        )
                        if cached is not None:
                            self._d(
                                f"[cli→{state.client_id}] GET_CHANNEL idx={channel_idx} → cache hit"
                            )
                            await self._send_bytes_to_client(state, cached)
                        else:
                            self._d(
                                f"[cli→{state.client_id}] GET_CHANNEL idx={channel_idx} → cache miss, forwarding"
                            )
                            forward_frames.append(frame)
                    else:
                        forward_frames.append(frame)

                if forward_frames:
                    payload = b"".join(forward_frames)
                    if self.backend_connected.is_set():
                        await self._send_to_backend(payload)
                    else:
                        self._p("[proxy] backend down, dropping client->backend bytes")

        except (asyncio.CancelledError, ConnectionError):
            pass
        except Exception as exc:
            self._p(f"[client] error {peer}: {exc}")
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
            self._p(
                f"[client] disconnected {peer} as {state.client_id} (clients={self._connected_client_count()})"
            )

    async def _backend_manager(self) -> None:
        while not self._stop.is_set():
            if not self.backend_connected.is_set():
                await self._connect_backend()

            if self.backend_connected.is_set() and self.backend_reader:
                try:
                    while not self._stop.is_set():
                        data = await self.backend_reader.read(65536)
                        if not data:
                            raise ConnectionError("backend closed")

                        for frame in self.backend_parser.feed(data):
                            await self._handle_backend_frame(frame)
                except (asyncio.CancelledError, ConnectionError):
                    self._p("[backend] disconnected")
                except Exception as exc:
                    self._p(f"[backend] read error: {exc}")
                finally:
                    self.backend_connected.clear()
                    await self._close_backend()

            await asyncio.sleep(self.backend_reconnect_delay)

    async def _connect_backend(self) -> None:
        try:
            self._p(f"[backend] connecting to {self.backend_host}:{self.backend_port} ...")
            reader, writer = await asyncio.open_connection(self.backend_host, self.backend_port)
            self.backend_reader, self.backend_writer = reader, writer
            self.backend_parser.reset()
            self.backend_connected.set()
            self._p("[backend] connected")
        except Exception as exc:
            self.backend_connected.clear()
            self.backend_reader, self.backend_writer = None, None
            self._p(f"[backend] connect failed: {exc}")

    async def _close_backend(self) -> None:
        self.backend_connected.clear()
        self.backend_parser.reset()
        # Unblock the poll loop if it is waiting for a backend response
        self._poll_event.set()
        if self.backend_writer:
            try:
                self.backend_writer.close()
                await self.backend_writer.wait_closed()
            except Exception:
                pass
        self.backend_reader, self.backend_writer = None, None

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
            if not self.backend_writer or self._stop.is_set():
                return
            try:
                self.backend_writer.write(data)
                await self.backend_writer.drain()
            except Exception:
                self.backend_connected.clear()

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

        self._d(
            f"[history] appended seq={seq} size={len(frame)}B "
            f"total={len(self.history)} frames / {self.history_size_bytes}B "
            f"evicted={evicted_count}"
        )
        return seq

    async def _handle_backend_frame(self, frame: bytes) -> None:
        packet_type = get_packet_type(frame)
        is_msg = is_message_frame(frame)
        history_seq: Optional[int] = None

        if is_msg:
            history_seq = self._append_history(frame)

        connected_clients = [s for s in self.client_states.values() if s.writer is not None]
        self._d(
            f"[bkd←frame] type={fmt_ptype(packet_type)} size={len(frame)}B "
            f"is_msg={is_msg} seq={history_seq} "
            f"connected_clients={len(connected_clients)}/{len(self.client_states)}"
        )

        # Update generic state cache so reconnecting clients get a full snapshot.
        if packet_type is not None and packet_type not in CACHE_BLACKLIST_TYPES and not is_msg:
            if packet_type in CACHE_SINGLETON_TYPES:
                self._state_cache[packet_type] = [frame]
                self._d(f"[cache] singleton updated type={fmt_ptype(packet_type)}")
            elif packet_type in CACHE_DEDUP_BY_PUBKEY_TYPES:
                end = PUBKEY_OFFSET + PUBKEY_SIZE
                if len(frame) >= end:
                    pubkey = frame[PUBKEY_OFFSET:end]
                    bucket = self._state_cache.setdefault(packet_type, [])
                    for i, existing in enumerate(bucket):
                        if existing[PUBKEY_OFFSET:end] == pubkey:
                            if existing != frame:
                                bucket[i] = frame
                                self._d(
                                    f"[cache] updated type={fmt_ptype(packet_type)} "
                                    f"pubkey={pubkey[:4].hex()}… total={len(bucket)}"
                                )
                            break
                    else:
                        bucket.append(frame)
                        self._d(
                            f"[cache] new type={fmt_ptype(packet_type)} "
                            f"pubkey={pubkey[:4].hex()}… total={len(bucket)}"
                        )
            elif packet_type in CACHE_DEDUP_BY_IDX_TYPES:
                if len(frame) > IDX_OFFSET:
                    idx = frame[IDX_OFFSET]
                    bucket = self._state_cache.setdefault(packet_type, [])
                    for i, existing in enumerate(bucket):
                        if len(existing) > IDX_OFFSET and existing[IDX_OFFSET] == idx:
                            if existing != frame:
                                bucket[i] = frame
                                self._d(
                                    f"[cache] updated type={fmt_ptype(packet_type)} "
                                    f"idx={idx} total={len(bucket)}"
                                )
                            break
                    else:
                        bucket.append(frame)
                        self._d(
                            f"[cache] new type={fmt_ptype(packet_type)} "
                            f"idx={idx} total={len(bucket)}"
                        )
            else:
                bucket = self._state_cache.setdefault(packet_type, [])
                if frame not in bucket:
                    bucket.append(frame)
                    self._d(
                        f"[cache] collected type={fmt_ptype(packet_type)} "
                        f"total={len(bucket)}"
                    )

        # Track whether we are currently inside a backend contact dump so that
        # _send_frame_to_client can suppress forwarding to all clients.
        if packet_type == 0x02:  # CONTACTS_START
            self._in_contact_dump = True
            self._suppressed_dump_count = 0
            self._d("[cache] backend contact dump started")
        elif packet_type == 0x04:  # END_OF_CONTACTS
            self._in_contact_dump = False
            if self._suppressed_dump_count:
                self._d(
                    f"[cache] backend contact dump done: "
                    f"{self._suppressed_dump_count} contact(s) received and cached"
                )
                self._suppressed_dump_count = 0

        # Signal the poll loop: got a message (poll again immediately) or
        # queue empty (sleep before next poll).
        if is_msg:
            self._poll_got_message = True
            self._poll_event.set()
        elif packet_type == RESP_CODE_NO_MORE_MESSAGES:
            self._poll_got_message = False
            self._poll_event.set()

        for state in list(self.client_states.values()):
            if state.writer is None:
                self._d(f"[bkd→{state.client_id}] SKIP — not connected")
                continue
            await self._send_frame_to_client(state, frame, packet_type, history_seq)

    async def _replay_state_cache(self, state: ClientState) -> None:
        """Replay all cached backend state to a client that just received SELF_INFO.

        Called once per connection, right after SELF_INFO is forwarded, so the
        client gets a full state snapshot (contacts, channels, and any other
        non-transient backend frames) without extra backend round-trips.
        Pending messages will be served on the client's subsequent
        CMD_SYNC_NEXT_MESSAGE calls.
        """
        total = 0
        # Log cache contents before replay for diagnostics.
        self._p(
            f"[cache] snapshot before replay to {state.client_id}: "
            + ", ".join(
                f"{fmt_ptype(pt)}×{len(fs)}"
                for pt, fs in self._state_cache.items()
            )
        )
        # Replay singleton types first (SELF_INFO already sent — skip it),
        # then collections, preserving insertion order within each bucket.
        contact_frames = self._state_cache.get(0x03, [])
        for packet_type, frames in list(self._state_cache.items()):
            if packet_type == RESP_CODE_SELF_INFO:
                continue  # already sent by the caller
            if packet_type == 0x03:
                # Wrap CONTACT frames with synthetic delimiters so the client
                # receives a well-formed contact dump.
                n = len(contact_frames)
                # CONTACTS_START payload: code(1) + count(4, uint32_t LE)
                # Wire frame: '>'(1) + payload_len_LE(2) + payload(5) = 8 bytes
                contacts_start = bytes([ord(">"), 5, 0, 0x02]) + struct.pack("<I", n)
                await self._send_bytes_to_client(state, contacts_start)
                for frame in frames:
                    await self._send_bytes_to_client(state, frame)
                    total += 1
                end_of_contacts = build_backend_frame(0x04)
                await self._send_bytes_to_client(state, end_of_contacts)
                continue
            for frame in frames:
                await self._send_bytes_to_client(state, frame)
                total += 1

        n_types = sum(
            1 for pt in self._state_cache if pt != RESP_CODE_SELF_INFO
        )
        self._p(
            f"[client] replayed state cache to {state.client_id}: "
            f"{total} frame(s) across {n_types} type(s)"
        )
        if state.pending_message_frames:
            self._p(
                f"[client] {len(state.pending_message_frames)} buffered message(s) pending "
                f"for {state.client_id} — will serve on next SYNC"
            )
        else:
            self._p(f"[client] no buffered messages pending for {state.client_id}")

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

        # Message frames go into the client's pending queue and are served on the
        # client's next CMD_SYNC_NEXT_MESSAGE.  This ensures every app receives
        # messages in a proper SYNC request/response context.
        if history_seq is not None:
            if history_seq > state.replay_capture_seq:
                state.pending_message_frames.append(BufferedFrame(seq=history_seq, data=frame))
                state.replay_capture_seq = history_seq
                self._d(
                    f"[bkd→{state.client_id}] QUEUED seq={history_seq} "
                    f"pending={len(state.pending_message_frames)}"
                )
                self._p(
                    f"[client] queued message seq={history_seq} for {state.client_id} "
                    f"(pending={len(state.pending_message_frames)})"
                )
                # Notify the client that a message is waiting so it sends
                # CMD_SYNC_NEXT_MESSAGE.  We do this AFTER queueing so there
                # is no race between the notification and the pending queue
                # being empty (which happened when the backend's own
                # PUSH_MSG_WAITING arrived before the poll loop retrieved
                # the message).
                await self._send_bytes_to_client(state, build_backend_frame(0x83))
            else:
                self._d(
                    f"[bkd→{state.client_id}] SKIP msg seq={history_seq} — "
                    f"already at cursor={state.replay_capture_seq}"
                )
            return

        # CMD_GET_CONTACTS is now intercepted per-client and served from the
        # proxy cache, so the backend should no longer receive contact dump
        # requests.  If a dump arrives anyway (e.g. cold-cache forwarding),
        # count it for diagnostic purposes but still forward it so the
        # requesting client gets its response.
        if self._in_contact_dump and packet_type == 0x03:
            if state is next(iter(self.client_states.values()), None):
                self._suppressed_dump_count += 1

        # Suppress CHANNEL_INFO if this client got channels via cache replay AND
        # the frame is identical to what was replayed (no actual change).
        # All other frames (SELF_INFO, CUSTOM_VARS, ACK, …) are pushed directly.
        self._d(
            f"[bkd→{state.client_id}] PUSH type={fmt_ptype(packet_type)} size={len(frame)}B"
        )
        if not await self._send_bytes_to_client(state, frame):
            self._d(f"[bkd→{state.client_id}] PUSH FAILED — client dropped")
            return

        if packet_type == RESP_CODE_SELF_INFO and state.awaiting_replay_after_self_info:
            state.awaiting_replay_after_self_info = False
            await self._replay_state_cache(state)


def parse_args():
    ap = argparse.ArgumentParser(
        description="TCP broadcast proxy with per-client message replay for MeshCore companion links"
    )
    ap.add_argument("--listen-host", default="0.0.0.0")
    ap.add_argument("--listen-port", type=int, required=True)
    ap.add_argument("--backend-host", required=True)
    ap.add_argument("--backend-port", type=int, required=True)
    ap.add_argument("--reconnect-delay", type=float, default=1.0)
    ap.add_argument("--history-bytes", type=int, default=DEFAULT_HISTORY_BYTES)
    ap.add_argument("--history-frames", type=int, default=DEFAULT_HISTORY_FRAMES)
    ap.add_argument("--client-write-timeout", type=float, default=DEFAULT_CLIENT_WRITE_TIMEOUT)
    ap.add_argument("--client-queue-frames", type=int, default=DEFAULT_CLIENT_QUEUE_FRAMES)
    ap.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL,
        help="Seconds to wait between backend polls when the queue is empty (default: %(default)s)",
    )
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--debug", action="store_true", help="Enable verbose debug logging")
    return ap.parse_args()


async def main_async():
    args = parse_args()
    proxy = BroadcastProxy(
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        backend_host=args.backend_host,
        backend_port=args.backend_port,
        backend_reconnect_delay=args.reconnect_delay,
        history_bytes=args.history_bytes,
        history_frames=args.history_frames,
        client_write_timeout=args.client_write_timeout,
        client_queue_frames=args.client_queue_frames,
        poll_interval=args.poll_interval,
        log=not args.quiet,
        debug=args.debug,
    )

    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(proxy.stop()))
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
