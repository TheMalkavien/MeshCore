#!/usr/bin/env python3
"""
Serial broadcast proxy for MeshCore companion links:
- Multi clients TCP -> single backend serial connection
- Backend serial -> broadcast to all connected TCP clients
- Backend message frames are buffered per logical client
- Reconnecting clients can replay only missed messages
"""

import argparse
import asyncio
import signal
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

try:
    import serial
    from serial import SerialException
except ImportError:  # pragma: no cover - depends on local environment
    serial = None

    class SerialException(Exception):
        pass


CMD_APP_START = 0x01
CMD_SYNC_NEXT_MESSAGE = 0x0A
RESP_CODE_SELF_INFO = 0x05
RESP_CODE_NO_MORE_MESSAGES = 0x0A

MAX_COMPANION_FRAME_SIZE = 2048
DEFAULT_HISTORY_BYTES = 512 * 1024
DEFAULT_HISTORY_FRAMES = 4096
DEFAULT_CLIENT_WRITE_TIMEOUT = 2.0
DEFAULT_CLIENT_QUEUE_FRAMES = 512

MESSAGE_PACKET_TYPES = {0x07, 0x08, 0x10, 0x11}

PACKET_TYPE_NAMES: Dict[int, str] = {
    0x01: "APP_START",
    0x02: "SEND_TXT",
    0x03: "SEND_DATA",
    0x04: "SET_CONFIG",
    0x05: "SELF_INFO",
    0x06: "CONTACT_MSG",
    0x07: "MSG_DIRECT",
    0x08: "MSG_FLOOD",
    0x09: "ACK",
    0x0A: "SYNC/NO_MORE",
    0x0B: "CHANNEL_MSG",
    0x10: "MSG_CHANNEL",
    0x11: "MSG_CHANNEL_FLOOD",
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
    serving_buffered_messages: bool = False
    outbound_queue: Optional[asyncio.Queue] = None
    sender_task: Optional[asyncio.Task] = None
    last_disconnect_at: float = 0.0
    sync_dropped: bool = False


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
        client_write_timeout: float = DEFAULT_CLIENT_WRITE_TIMEOUT,
        client_queue_frames: int = DEFAULT_CLIENT_QUEUE_FRAMES,
        log: bool = True,
        debug: bool = False,
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
        self.client_write_timeout = max(client_write_timeout, 0.1)
        self.client_queue_frames = max(client_queue_frames, 1)
        self.log = log
        self.debug = debug

        self.client_states: Dict[str, ClientState] = {}
        self._next_client_slot = 1

        self.serial_conn: Optional["serial.Serial"] = None
        self.backend_lock = asyncio.Lock()
        self.backend_connected = asyncio.Event()
        self.backend_parser = CompanionStreamParser(ord(">"))

        self.history: Deque[BufferedFrame] = deque()
        self.history_size_bytes = 0
        self.next_history_seq = 1

        self._server: Optional[asyncio.base_events.Server] = None
        self._stop = asyncio.Event()
        self._backend_task: Optional[asyncio.Task] = None
        self._sync_pending = False
        self._sync_pending_client: Optional[str] = None

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
            f"replay={state.serving_buffered_messages} "
            f"pending_msgs={len(state.pending_message_frames)} "
            f"sync_dropped={state.sync_dropped} "
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
        if serial is None:
            raise RuntimeError("pyserial is required. Install it with: pip install pyserial")

        self._server = await asyncio.start_server(
            self._handle_client, host=self.listen_host, port=self.listen_port
        )
        addr = ", ".join(str(sock.getsockname()) for sock in (self._server.sockets or []))
        self._p(
            f"[proxy] listening on {addr} -> serial {self.serial_port} @ {self.baudrate} baud"
        )
        self._p(
            f"[proxy] message history enabled: {self.history_frames} frames / {self.history_bytes} bytes"
        )

        self._backend_task = asyncio.create_task(self._backend_manager())

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

            if self._backend_task and not self._backend_task.done():
                self._backend_task.cancel()
                try:
                    await self._backend_task
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

    async def _register_client(self, peer_group: str, writer: asyncio.StreamWriter) -> ClientState:
        state = ClientState(
            client_id=self._allocate_client_id(peer_group),
            peer_group=peer_group,
        )
        self.client_states[state.client_id] = state
        state.writer = writer
        state.awaiting_replay_after_self_info = False
        state.serving_buffered_messages = False
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
            state.serving_buffered_messages = True
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
        state.serving_buffered_messages = False

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
            state.serving_buffered_messages = True
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
                self._d(f"[send_bytes→{state.client_id}] SKIP — writer={writer is not None} queue={queue is not None} stop={self._stop.is_set()}")
                return False

            try:
                queue.put_nowait(payload)
                self._d(f"[send_bytes→{state.client_id}] enqueued {len(payload)}B — q={queue.qsize()}/{self.client_queue_frames}")
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
        if not state.pending_message_frames:
            if state.serving_buffered_messages:
                state.serving_buffered_messages = False
                self._p(f"[client] buffered message replay complete for {state.client_id}")
                self._d(f"[replay/{state.client_id}] queue empty → send NO_MORE_MSGS + synthetic SYNC")
                result = await self._send_bytes_to_client(
                    state, build_backend_frame(RESP_CODE_NO_MORE_MESSAGES)
                )
                # Syncs from the client were absorbed during replay; kick the backend
                # queue with one synthetic sync so any remaining queued messages are
                # delivered (the chain continues naturally from there).
                if self.backend_connected.is_set() and not self._sync_pending:
                    self._sync_pending = True
                    self._d(f"[replay/{state.client_id}] emitting synthetic SYNC → backend")
                    await self._send_to_backend(bytes((ord("<"), 1, 0, CMD_SYNC_NEXT_MESSAGE)))
                return result
            self._d(f"[replay/{state.client_id}] _serve called but nothing pending and not serving")
            return False

        buffered = state.pending_message_frames.popleft()
        self._d(
            f"[replay/{state.client_id}] serving seq={buffered.seq} "
            f"size={len(buffered.data)}B remaining={len(state.pending_message_frames)}"
        )
        if await self._send_bytes_to_client(state, buffered.data):
            remaining = len(state.pending_message_frames)
            self._p(
                f"[client] served buffered message to {state.client_id} (remaining={remaining})"
            )
            return True

        self._d(f"[replay/{state.client_id}] send failed — re-queuing seq={buffered.seq}")
        state.pending_message_frames.appendleft(buffered)
        return False

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
                        self._p(
                            f"[client] APP_START from {state.client_id} app_name="
                            f"{state.app_name or '<empty>'}"
                        )
                        self._adopt_prior_state(state)
                        self._capture_pending_history(state)
                        state.awaiting_replay_after_self_info = True
                        forward_frames.append(frame)
                    elif command == CMD_SYNC_NEXT_MESSAGE and (
                        state.pending_message_frames or state.serving_buffered_messages
                    ):
                        self._d(
                            f"[cli→{state.client_id}] SYNC intercepted (replay) — "
                            f"pending={len(state.pending_message_frames)} serving={state.serving_buffered_messages}"
                        )
                        await self._serve_pending_message(state)
                    elif command == CMD_SYNC_NEXT_MESSAGE:
                        if not self._sync_pending:
                            self._sync_pending = True
                            self._sync_pending_client = state.client_id
                            self._d(f"[cli→{state.client_id}] SYNC forwarded → backend (sync_pending set)")
                            forward_frames.append(frame)
                        else:
                            # A sync is already in flight — its response will be
                            # broadcast to all clients. Remember that this client
                            # needs a follow-up sync so it is not left behind if
                            # the in-flight sync is consumed by another client.
                            state.sync_dropped = True
                            self._d(f"[cli→{state.client_id}] SYNC dropped (in-flight exists) — sync_dropped=True")
                    else:
                        forward_frames.append(frame)

                if forward_frames:
                    payload = b"".join(forward_frames)
                    if self.backend_connected.is_set():
                        await self._send_to_backend(payload)
                    else:
                        self._p("[proxy] serial backend down, dropping client->backend bytes")

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

            if self.backend_connected.is_set() and self.serial_conn:
                try:
                    while not self._stop.is_set():
                        data = await asyncio.to_thread(self.serial_conn.read, 65536)
                        if not data:
                            continue

                        for frame in self.backend_parser.feed(data):
                            await self._handle_backend_frame(frame)
                except asyncio.CancelledError:
                    raise
                except (OSError, SerialException) as exc:
                    self._p(f"[backend] disconnected: {exc}")
                except Exception as exc:
                    self._p(f"[backend] read error: {exc}")
                finally:
                    self.backend_connected.clear()
                    await self._close_backend()

            await asyncio.sleep(self.reconnect_delay)

    async def _connect_backend(self) -> None:
        try:
            self._p(f"[backend] opening {self.serial_port} @ {self.baudrate} ...")
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
            self._p("[backend] connected")
        except Exception as exc:
            self.backend_connected.clear()
            self.serial_conn = None
            self._p(f"[backend] connect failed: {exc}")

    async def _close_backend(self) -> None:
        self.backend_connected.clear()
        self.backend_parser.reset()
        self._sync_pending = False
        self._sync_pending_client = None
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
                await asyncio.to_thread(conn.write, data)
                await asyncio.to_thread(conn.flush)
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
        history_seq: Optional[int] = None
        is_msg = is_message_frame(frame)
        if is_msg:
            history_seq = self._append_history(frame)

        connected_clients = [s for s in self.client_states.values() if s.writer is not None]
        self._d(
            f"[bkd←frame] type={fmt_ptype(packet_type)} size={len(frame)}B "
            f"is_msg={is_msg} seq={history_seq} sync_pending={self._sync_pending} "
            f"connected_clients={len(connected_clients)}/{len(self.client_states)}"
        )

        # A message frame or no-more-messages response concludes the in-flight sync.
        if packet_type in MESSAGE_PACKET_TYPES or packet_type == RESP_CODE_NO_MORE_MESSAGES:
            if self._sync_pending:
                self._d(f"[sync] cleared sync_pending (got {fmt_ptype(packet_type)})")
            self._sync_pending = False
            self._sync_pending_client = None

        for state in list(self.client_states.values()):
            if state.writer is None:
                self._d(f"[bkd→{state.client_id}] SKIP — not connected")
                continue
            await self._send_frame_to_client(state, frame, packet_type, history_seq)

        # If any client had its sync dropped while the previous one was in flight,
        # emit one follow-up sync now so those clients are not left waiting.
        if not self._sync_pending:
            for state in self.client_states.values():
                if state.sync_dropped and state.writer is not None:
                    state.sync_dropped = False
                    self._sync_pending = True
                    self._sync_pending_client = state.client_id
                    self._p(f"[proxy] re-emitting dropped sync on behalf of {state.client_id}")
                    self._d(f"[sync] re-emit → backend on behalf of {state.client_id}")
                    await self._send_to_backend(bytes((ord("<"), 1, 0, CMD_SYNC_NEXT_MESSAGE)))
                    break

    async def _send_frame_to_client(
        self,
        state: ClientState,
        frame: bytes,
        packet_type: Optional[int],
        history_seq: Optional[int],
    ) -> None:
        # CMD_SYNC_NEXT_MESSAGE and RESP_CODE_NO_MORE_MESSAGES share value 0x0A.
        # When the backend signals end-of-queue to a client that is mid-replay,
        # suppress it: the proxy will send its own RESP_CODE_NO_MORE_MESSAGES at
        # the end of the replay buffer, preventing a premature stop of the flow.
        if packet_type == RESP_CODE_NO_MORE_MESSAGES and state.serving_buffered_messages:
            self._d(
                f"[bkd→{state.client_id}] SUPPRESS NO_MORE_MSGS — mid-replay "
                f"({len(state.pending_message_frames)} msgs left)"
            )
            return

        # Message frames are only pushed directly to the client whose SYNC triggered
        # the backend response. All other clients receive the message via their own
        # SYNC → replay path, so apps that use a strict request/response state machine
        # (e.g. meshcore-flutter) are not confused by unsolicited message frames.
        if history_seq is not None and state.client_id != self._sync_pending_client:
            if history_seq > state.replay_capture_seq:
                state.pending_message_frames.append(BufferedFrame(seq=history_seq, data=frame))
                state.replay_capture_seq = history_seq
                state.serving_buffered_messages = True
                self._d(
                    f"[bkd→{state.client_id}] DEFERRED seq={history_seq} → replay queue "
                    f"pending={len(state.pending_message_frames)}"
                )
                self._p(f"[client] deferred message seq={history_seq} for {state.client_id} (will serve on next SYNC)")
            else:
                self._d(
                    f"[bkd→{state.client_id}] SKIP msg seq={history_seq} — already at cursor={state.replay_capture_seq}"
                )
            return

        self._d(
            f"[bkd→{state.client_id}] QUEUE type={fmt_ptype(packet_type)} "
            f"size={len(frame)}B seq={history_seq} | {self._q_info(state)}"
        )

        if not await self._send_bytes_to_client(state, frame):
            self._d(f"[bkd→{state.client_id}] QUEUE FAILED — client dropped")
            return

        # Advance the per-client cursor when a live message frame is accepted
        # for delivery, so reconnect replay only covers messages missed offline.
        if history_seq is not None and history_seq > state.replay_capture_seq:
            old_cursor = state.replay_capture_seq
            state.replay_capture_seq = history_seq
            self._d(
                f"[bkd→{state.client_id}] cursor advanced {old_cursor}→{history_seq}"
            )

        if packet_type == RESP_CODE_SELF_INFO and state.awaiting_replay_after_self_info:
            state.awaiting_replay_after_self_info = False
            if state.pending_message_frames:
                self._p(
                    f"[client] waiting for client sync to replay {len(state.pending_message_frames)} buffered messages to {state.client_id}"
                )
            else:
                self._p(f"[client] no replay needed for {state.client_id} after SELF_INFO")


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
    ap.add_argument("--client-write-timeout", type=float, default=DEFAULT_CLIENT_WRITE_TIMEOUT)
    ap.add_argument("--client-queue-frames", type=int, default=DEFAULT_CLIENT_QUEUE_FRAMES)
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--debug", action="store_true", help="Enable verbose debug logging")
    return ap.parse_args()


async def main_async():
    args = parse_args()
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
        client_write_timeout=args.client_write_timeout,
        client_queue_frames=args.client_queue_frames,
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
