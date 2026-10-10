#!/usr/bin/env python3
"""
Serial broadcast proxy for MeshCore companion links:
- Multi clients TCP -> single backend serial connection
- Proxy polls backend at a fixed interval; results cached locally
- Client CMD_SYNC_NEXT_MESSAGE always served from local cache (never forwarded)
- Reconnecting clients replay only missed messages from the proxy history

All the shared proxy logic lives in tools/mc_proxy_core.py; this file only
provides the serial backend transport and the CLI entrypoint.
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
from typing import Optional

try:
    import serial
    from serial import SerialException
except ImportError:  # pragma: no cover - depends on local environment
    serial = None

    class SerialException(Exception):
        pass

# Allow running this script directly (python tools/serial_proxy/serial_proxy.py)
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
)


class SerialBroadcastProxy(ProxyCore):
    """Broadcast proxy whose backend is a serial (USB-CDC) device."""

    _DISCONNECT_EXC = (OSError, SerialException)
    _BACKEND_DOWN_MSG = "[proxy] serial backend down, dropping client->backend bytes"

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
    ):
        super().__init__(
            listen_host=listen_host,
            listen_port=listen_port,
            reconnect_delay=reconnect_delay,
            history_bytes=history_bytes,
            history_frames=history_frames,
            history_max_age=history_max_age,
            client_write_timeout=client_write_timeout,
            client_queue_frames=client_queue_frames,
            poll_interval=poll_interval,
            max_clients=max_clients,
            inactive_state_max_age=inactive_state_max_age,
        )
        self.serial_port = serial_port
        self.baudrate = baudrate
        self.read_timeout = read_timeout
        self.write_timeout = write_timeout
        self.serial_conn: Optional["serial.Serial"] = None

    # --- Transport hooks ---------------------------------------------------

    def _pre_start_check(self) -> None:
        if serial is None:
            raise RuntimeError("pyserial is required. Install it with: pip install pyserial")

    def _log_listening(self, addr: str) -> None:
        self._logger.info(
            "[proxy] listening on %s -> serial %s @ %d baud", addr, self.serial_port, self.baudrate
        )

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

    async def _transport_close(self) -> None:
        conn = self.serial_conn
        self.serial_conn = None
        if conn:
            try:
                await asyncio.to_thread(conn.close)
            except Exception:
                pass

    def _backend_is_readable(self) -> bool:
        return self.serial_conn is not None

    async def _transport_read(self) -> bytes:
        conn = self.serial_conn
        if conn is None:
            return b""
        return await asyncio.to_thread(conn.read, 65536)

    def _backend_is_writable(self) -> bool:
        return self.serial_conn is not None

    async def _transport_write(self, data: bytes) -> None:
        conn = self.serial_conn
        if conn is None:
            return
        # Bound the blocking write. The explicit flush() was removed: on
        # Windows pyserial's flush() is an unbounded busy-wait that ignores
        # write_timeout, so a wedged USB-CDC device would block here forever
        # while holding backend_lock and freeze the whole proxy. write()
        # with write_timeout already hands the bytes off to the OS.
        await asyncio.wait_for(
            asyncio.to_thread(conn.write, data),
            timeout=self.write_timeout + 1.0,
        )

    def _transport_after_write_error(self) -> None:
        conn = self.serial_conn
        if conn is not None:
            # PX-8(c): a timed-out write leaves the blocking conn.write() thread
            # stuck; cancel_write() unblocks it so the worker thread is released.
            try:
                conn.cancel_write()
            except Exception:
                pass
            # Also break the blocking read so the read loop exits and reconnects.
            try:
                conn.cancel_read()
            except Exception:
                pass


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
