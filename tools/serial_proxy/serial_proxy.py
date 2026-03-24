#!/usr/bin/env python3
"""
Serial broadcast proxy for MeshCore companion links:
- Multi clients TCP -> single backend serial connection
- Backend serial -> broadcast to all connected TCP clients
- Backend activity frames are buffered per logical client
- Reconnecting clients receive buffered activity after APP_START/SELF_INFO

The serial side keeps the native MeshCore companion USB framing:
- app/client -> device: '<' + len_le16 + payload
- device -> app/client: '>' + len_le16 + payload
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
PUSH_CODE_MSG_WAITING = 0x83

MAX_COMPANION_FRAME_SIZE = 2048
DEFAULT_HISTORY_BYTES = 512 * 1024
DEFAULT_HISTORY_FRAMES = 4096
DEFAULT_REPLAY_DELAY = 1.0

MESSAGE_PACKET_TYPES = {0x07, 0x08, 0x10, 0x11}
PUSH_ACTIVITY_PACKET_TYPES = (set(range(0x80, 0x91)) - {PUSH_CODE_MSG_WAITING})
ACTIVITY_PACKET_TYPES = PUSH_ACTIVITY_PACKET_TYPES | {PUSH_CODE_MSG_WAITING} | MESSAGE_PACKET_TYPES


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
    writer: Optional[asyncio.StreamWriter] = None
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    client_parser: CompanionStreamParser = field(
        default_factory=lambda: CompanionStreamParser(ord("<"))
    )
    awaiting_replay_after_self_info: bool = False
    replay_capture_seq: int = 0
    pending_push_frames: Deque[BufferedFrame] = field(default_factory=deque)
    pending_message_frames: Deque[BufferedFrame] = field(default_factory=deque)
    serving_buffered_messages: bool = False
    replay_task: Optional[asyncio.Task] = None
    last_disconnect_at: float = 0.0


def get_packet_type(frame: bytes) -> Optional[int]:
    if len(frame) < 4:
        return None
    return frame[3]


def is_activity_frame(frame: bytes) -> bool:
    packet_type = get_packet_type(frame)
    return packet_type in ACTIVITY_PACKET_TYPES


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
        replay_delay: float = DEFAULT_REPLAY_DELAY,
        log: bool = True,
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
        self.replay_delay = max(replay_delay, 0.0)
        self.log = log

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

    def _p(self, msg: str) -> None:
        if self.log:
            print(msg, flush=True)

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

    def _find_reusable_client_state(self, peer_group: str) -> Optional[ClientState]:
        reusable: List[ClientState] = [
            state
            for state in self.client_states.values()
            if state.peer_group == peer_group and state.writer is None
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
            f"[proxy] activity history enabled: {self.history_frames} frames / {self.history_bytes} bytes"
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

    async def _register_client(self, client_id: str, writer: asyncio.StreamWriter) -> ClientState:
        state = self._find_reusable_client_state(client_id)
        if state is None:
            state = ClientState(client_id=self._allocate_client_id(client_id), peer_group=client_id)
            self.client_states[state.client_id] = state
        else:
            state.client_parser.reset()
            self._cancel_replay_task(state)

        state.writer = writer
        state.awaiting_replay_after_self_info = False
        state.serving_buffered_messages = False
        state.last_disconnect_at = 0.0

        return state

    def _cancel_replay_task(self, state: ClientState) -> None:
        task = state.replay_task
        state.replay_task = None
        if task and not task.done():
            task.cancel()

    def _capture_pending_history(self, state: ClientState) -> None:
        captured_messages = 0
        for buffered in self.history:
            if buffered.seq <= state.replay_capture_seq:
                continue

            packet_type = get_packet_type(buffered.data)
            if packet_type in MESSAGE_PACKET_TYPES:
                state.pending_message_frames.append(buffered)
                captured_messages += 1
            elif packet_type in PUSH_ACTIVITY_PACKET_TYPES:
                state.pending_push_frames.append(buffered)

            state.replay_capture_seq = buffered.seq

        if captured_messages > 0:
            state.serving_buffered_messages = True

    def _schedule_replay_flush(self, state: ClientState) -> None:
        self._cancel_replay_task(state)
        if state.writer is None:
            return
        state.replay_task = asyncio.create_task(self._flush_pending_replay_after_delay(state))

    async def _flush_pending_replay_after_delay(self, state: ClientState) -> None:
        try:
            if self.replay_delay > 0:
                await asyncio.sleep(self.replay_delay)
            await self._flush_pending_push_frames(state)
            if state.pending_message_frames:
                self._p(
                    f"[client] waiting for client sync to replay {len(state.pending_message_frames)} buffered messages to {state.client_id}"
                )
        except asyncio.CancelledError:
            pass

    async def _send_bytes_to_client(self, state: ClientState, payload: bytes) -> bool:
        async with state.send_lock:
            writer = state.writer
            if writer is None or self._stop.is_set():
                return False

            try:
                writer.write(payload)
                await writer.drain()
                return True
            except Exception:
                if state.writer is writer:
                    state.writer = None
                    state.awaiting_replay_after_self_info = False
                self._cancel_replay_task(state)
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
                return False

    async def _flush_pending_push_frames(self, state: ClientState) -> None:
        replayed = 0
        while state.pending_push_frames and state.writer is not None:
            buffered = state.pending_push_frames.popleft()
            if not await self._send_bytes_to_client(state, buffered.data):
                state.pending_push_frames.appendleft(buffered)
                return
            replayed += 1

        if replayed:
            self._p(f"[client] replayed {replayed} push activity frames to {state.client_id}")

    async def _serve_pending_message(self, state: ClientState) -> bool:
        if not state.pending_message_frames:
            if state.serving_buffered_messages:
                state.serving_buffered_messages = False
                self._p(f"[client] buffered message replay complete for {state.client_id}")
                return await self._send_bytes_to_client(
                    state, build_backend_frame(RESP_CODE_NO_MORE_MESSAGES)
                )
            return False

        buffered = state.pending_message_frames.popleft()
        if await self._send_bytes_to_client(state, buffered.data):
            remaining = len(state.pending_message_frames)
            self._p(
                f"[client] served buffered message to {state.client_id} (remaining={remaining})"
            )
            return True

        state.pending_message_frames.appendleft(buffered)
        return False

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        client_id = self._make_client_id(peer)
        state = await self._register_client(client_id, writer)
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
                        self._capture_pending_history(state)
                        state.awaiting_replay_after_self_info = True
                        self._cancel_replay_task(state)
                        forward_frames.append(frame)
                    elif command == CMD_SYNC_NEXT_MESSAGE and (
                        state.pending_message_frames or state.serving_buffered_messages
                    ):
                        await self._serve_pending_message(state)
                    else:
                        if state.awaiting_replay_after_self_info:
                            self._schedule_replay_flush(state)
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
                state.awaiting_replay_after_self_info = False
                state.serving_buffered_messages = False
                state.last_disconnect_at = asyncio.get_running_loop().time()
                self._cancel_replay_task(state)
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
            state.awaiting_replay_after_self_info = False
            state.serving_buffered_messages = False
            state.last_disconnect_at = asyncio.get_running_loop().time()

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

        while self.history and (
            (self.history_frames and len(self.history) > self.history_frames)
            or (self.history_bytes and self.history_size_bytes > self.history_bytes)
        ):
            evicted = self.history.popleft()
            self.history_size_bytes -= len(evicted.data)

        return seq

    async def _handle_backend_frame(self, frame: bytes) -> None:
        packet_type = get_packet_type(frame)
        if is_activity_frame(frame):
            self._append_history(frame)

        for state in list(self.client_states.values()):
            if state.writer is None:
                continue
            await self._send_frame_to_client(state, frame, packet_type)

    async def _send_frame_to_client(
        self,
        state: ClientState,
        frame: bytes,
        packet_type: Optional[int],
    ) -> None:
        if not await self._send_bytes_to_client(state, frame):
            return

        if packet_type == RESP_CODE_SELF_INFO and state.awaiting_replay_after_self_info:
            state.awaiting_replay_after_self_info = False
            self._schedule_replay_flush(state)


def parse_args():
    ap = argparse.ArgumentParser(
        description="Serial broadcast proxy with per-client activity replay for MeshCore companion links"
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
    ap.add_argument("--replay-delay", type=float, default=DEFAULT_REPLAY_DELAY)
    ap.add_argument("--quiet", action="store_true")
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
        replay_delay=args.replay_delay,
        log=not args.quiet,
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
