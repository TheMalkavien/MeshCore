#!/usr/bin/env python3
"""
TCP broadcast proxy for MeshCore companion links:
- Multi clients -> single backend connection
- Proxy polls backend at a fixed interval; results cached locally
- Client CMD_SYNC_NEXT_MESSAGE always served from local cache (never forwarded)
- Reconnecting clients replay only missed messages from the proxy history

All the shared proxy logic lives in tools/mc_proxy_core.py; this file only
provides the TCP backend transport and the CLI entrypoint.
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
from typing import Optional

# Allow running this script directly (python tools/tcp_proxy/tcp_proxy.py)
# by making the shared core importable from the parent tools/ directory.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mc_proxy_core import (  # noqa: E402
    ProxyCore,
    DEFAULT_HISTORY_BYTES,
    DEFAULT_HISTORY_FRAMES,
    DEFAULT_CLIENT_WRITE_TIMEOUT,
    DEFAULT_CLIENT_QUEUE_FRAMES,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_MAX_CLIENTS,
    DEFAULT_INACTIVE_STATE_MAX_AGE,
    DEFAULT_BACKEND_WRITE_TIMEOUT,
)


class BroadcastProxy(ProxyCore):
    """Broadcast proxy whose backend is a TCP connection."""

    _DISCONNECT_EXC = (ConnectionError,)
    _BACKEND_DOWN_MSG = "[proxy] backend down, dropping client->backend bytes"

    def __init__(
        self,
        listen_host: str,
        listen_port: int,
        backend_host: str,
        backend_port: int,
        backend_reconnect_delay: float = 1.0,
        history_bytes: int = DEFAULT_HISTORY_BYTES,
        history_frames: int = DEFAULT_HISTORY_FRAMES,
        history_max_age: int = 86400,
        client_write_timeout: float = DEFAULT_CLIENT_WRITE_TIMEOUT,
        client_queue_frames: int = DEFAULT_CLIENT_QUEUE_FRAMES,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        max_clients: int = DEFAULT_MAX_CLIENTS,
        inactive_state_max_age: float = DEFAULT_INACTIVE_STATE_MAX_AGE,
    ):
        super().__init__(
            listen_host=listen_host,
            listen_port=listen_port,
            reconnect_delay=backend_reconnect_delay,
            history_bytes=history_bytes,
            history_frames=history_frames,
            history_max_age=history_max_age,
            client_write_timeout=client_write_timeout,
            client_queue_frames=client_queue_frames,
            poll_interval=poll_interval,
            max_clients=max_clients,
            inactive_state_max_age=inactive_state_max_age,
        )
        self.backend_host = backend_host
        self.backend_port = backend_port
        self.backend_reader: Optional[asyncio.StreamReader] = None
        self.backend_writer: Optional[asyncio.StreamWriter] = None

    # --- Transport hooks ---------------------------------------------------

    def _log_listening(self, addr: str) -> None:
        self._logger.info(
            "[proxy] listening on %s -> backend %s:%d",
            addr, self.backend_host, self.backend_port,
        )

    async def _connect_backend(self) -> None:
        try:
            self._logger.info(
                "[backend] connecting to %s:%d ...", self.backend_host, self.backend_port
            )
            reader, writer = await asyncio.open_connection(self.backend_host, self.backend_port)
            self.backend_reader, self.backend_writer = reader, writer
            self.backend_parser.reset()
            self.backend_connected.set()
            self._logger.info("[backend] connected")
        except Exception as exc:
            self.backend_connected.clear()
            self.backend_reader, self.backend_writer = None, None
            self._logger.info("[backend] connect failed: %s", exc)

    async def _transport_close(self) -> None:
        writer = self.backend_writer
        self.backend_reader, self.backend_writer = None, None
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def _backend_is_readable(self) -> bool:
        return self.backend_reader is not None

    async def _transport_read(self) -> bytes:
        reader = self.backend_reader
        if reader is None:
            return b""
        data = await reader.read(65536)
        if not data:
            raise ConnectionError("backend closed")
        return data

    def _backend_is_writable(self) -> bool:
        return self.backend_writer is not None

    async def _transport_write(self, data: bytes) -> None:
        writer = self.backend_writer
        if writer is None:
            return
        writer.write(data)
        # Bound the drain: holding backend_lock across an unbounded drain()
        # would freeze the poll loop and all client forwarding if the backend
        # peer stops reading (its TCP receive window closes).
        await asyncio.wait_for(writer.drain(), timeout=DEFAULT_BACKEND_WRITE_TIMEOUT)

    def _transport_after_write_error(self) -> None:
        # Drop the wedged connection so the read loop sees EOF and reconnects.
        writer = self.backend_writer
        self.backend_reader = None
        self.backend_writer = None
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass


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

    proxy = BroadcastProxy(
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        backend_host=args.backend_host,
        backend_port=args.backend_port,
        backend_reconnect_delay=args.reconnect_delay,
        history_bytes=args.history_bytes,
        history_frames=args.history_frames,
        history_max_age=args.history_max_age,
        client_write_timeout=args.client_write_timeout,
        client_queue_frames=args.client_queue_frames,
        poll_interval=args.poll_interval,
        max_clients=args.max_clients,
        inactive_state_max_age=args.inactive_state_max_age,
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
