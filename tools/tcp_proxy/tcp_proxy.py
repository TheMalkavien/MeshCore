#!/usr/bin/env python3
"""
TCP broadcast proxy for MeshCore companion links:
- Multi clients -> single backend connection
- Backend -> broadcast to all connected clients
- Backend message frames are buffered per logical client
- Reconnecting clients can replay only missed messages
"""

import argparse
import asyncio
import signal
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional


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
    pending_message_frames: Deque[BufferedFrame] = field(default_factory=deque)
    serving_buffered_messages: bool = False
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
        log: bool = True,
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
        self.log = log

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
        state = self._find_reusable_client_state(peer_group)
        if state is None:
            state = ClientState(
                client_id=self._allocate_client_id(peer_group),
                peer_group=peer_group,
            )
            self.client_states[state.client_id] = state
        else:
            state.client_parser.reset()

        state.writer = writer
        state.awaiting_replay_after_self_info = False
        state.serving_buffered_messages = False
        state.last_disconnect_at = 0.0
        state.outbound_queue = asyncio.Queue(maxsize=self.client_queue_frames)
        self._cancel_sender_task(state)
        state.sender_task = asyncio.create_task(self._client_sender(state, writer))

        return state

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

        try:
            while not self._stop.is_set():
                payload = await queue.get()
                if payload is None:
                    break
                if state.writer is not writer:
                    break

                writer.write(payload)
                await asyncio.wait_for(writer.drain(), timeout=self.client_write_timeout)
        except asyncio.TimeoutError:
            self._p(f"[client] send timeout to {state.client_id}, closing stalled connection")
        except asyncio.CancelledError:
            pass
        except Exception:
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

    async def _send_bytes_to_client(self, state: ClientState, payload: bytes) -> bool:
        async with state.send_lock:
            writer = state.writer
            queue = state.outbound_queue
            if writer is None or queue is None or self._stop.is_set():
                return False

            try:
                queue.put_nowait(payload)
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
                        self._capture_pending_history(state)
                        state.awaiting_replay_after_self_info = True
                        forward_frames.append(frame)
                    elif command == CMD_SYNC_NEXT_MESSAGE and (
                        state.pending_message_frames or state.serving_buffered_messages
                    ):
                        await self._serve_pending_message(state)
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

        while self.history and (
            (self.history_frames and len(self.history) > self.history_frames)
            or (self.history_bytes and self.history_size_bytes > self.history_bytes)
        ):
            evicted = self.history.popleft()
            self.history_size_bytes -= len(evicted.data)

        return seq

    async def _handle_backend_frame(self, frame: bytes) -> None:
        packet_type = get_packet_type(frame)
        history_seq: Optional[int] = None
        if is_message_frame(frame):
            history_seq = self._append_history(frame)

        for state in list(self.client_states.values()):
            if state.writer is None:
                continue
            await self._send_frame_to_client(state, frame, packet_type, history_seq)

    async def _send_frame_to_client(
        self,
        state: ClientState,
        frame: bytes,
        packet_type: Optional[int],
        history_seq: Optional[int],
    ) -> None:
        if not await self._send_bytes_to_client(state, frame):
            return

        # Advance the per-client cursor when a live message frame is accepted
        # for delivery, so reconnect replay only covers messages missed offline.
        if history_seq is not None and history_seq > state.replay_capture_seq:
            state.replay_capture_seq = history_seq

        if packet_type == RESP_CODE_SELF_INFO and state.awaiting_replay_after_self_info:
            state.awaiting_replay_after_self_info = False
            if state.pending_message_frames:
                self._p(
                    f"[client] waiting for client sync to replay {len(state.pending_message_frames)} buffered messages to {state.client_id}"
                )


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
    ap.add_argument("--quiet", action="store_true")
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
