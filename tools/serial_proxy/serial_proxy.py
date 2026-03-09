#!/usr/bin/env python3
"""
Serial broadcast proxy:
- Multi clients TCP -> single backend serial connection
- Backend serial -> broadcast to all TCP clients
- Clients -> backend serial (merged)
- Simple, no sessions/state
- Shutdown that actually stops on Ctrl-C

The serial side keeps the native MeshCore companion USB framing:
- app/client -> device: '<' + len_le16 + payload
- device -> app/client: '>' + len_le16 + payload
"""

import argparse
import asyncio
import signal
from typing import Optional, Set

try:
    import serial
    from serial import SerialException
except ImportError:  # pragma: no cover - depends on local environment
    serial = None

    class SerialException(Exception):
        pass


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
        log: bool = True,
    ):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.serial_port = serial_port
        self.baudrate = baudrate
        self.reconnect_delay = reconnect_delay
        self.read_timeout = read_timeout
        self.write_timeout = write_timeout
        self.log = log

        self.clients: Set[asyncio.StreamWriter] = set()

        self.serial_conn: Optional["serial.Serial"] = None
        self.backend_lock = asyncio.Lock()
        self.backend_connected = asyncio.Event()

        self._server: Optional[asyncio.base_events.Server] = None
        self._stop = asyncio.Event()
        self._backend_task: Optional[asyncio.Task] = None

    def _p(self, msg: str) -> None:
        if self.log:
            print(msg, flush=True)

    async def start(self) -> None:
        if serial is None:
            raise RuntimeError(
                "pyserial is required. Install it with: pip install pyserial"
            )

        self._server = await asyncio.start_server(
            self._handle_client, host=self.listen_host, port=self.listen_port
        )
        addr = ", ".join(str(sock.getsockname()) for sock in (self._server.sockets or []))
        self._p(
            f"[proxy] listening on {addr} -> serial {self.serial_port} @ {self.baudrate} baud"
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

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        self.clients.add(writer)
        self._p(f"[client] connected {peer} (clients={len(self.clients)})")

        try:
            while not self._stop.is_set():
                data = await reader.read(65536)
                if not data:
                    break

                if self.backend_connected.is_set():
                    await self._send_to_backend(data)
                else:
                    self._p("[proxy] serial backend down, dropping client->backend bytes")

        except (asyncio.CancelledError, ConnectionError):
            pass
        except Exception as e:
            self._p(f"[client] error {peer}: {e}")
        finally:
            self.clients.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            self._p(f"[client] disconnected {peer} (clients={len(self.clients)})")

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
                        await self._broadcast_to_clients(data)
                except asyncio.CancelledError:
                    raise
                except (OSError, SerialException) as e:
                    self._p(f"[backend] disconnected: {e}")
                except Exception as e:
                    self._p(f"[backend] read error: {e}")
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
            self.backend_connected.set()
            self._p("[backend] connected")
        except Exception as e:
            self.backend_connected.clear()
            self.serial_conn = None
            self._p(f"[backend] connect failed: {e}")

    async def _close_backend(self) -> None:
        self.backend_connected.clear()
        conn = self.serial_conn
        self.serial_conn = None
        if conn:
            try:
                await asyncio.to_thread(conn.close)
            except Exception:
                pass

    async def _close_all_clients(self) -> None:
        writers = list(self.clients)
        self.clients.clear()
        for w in writers:
            try:
                w.close()
            except Exception:
                pass
        for w in writers:
            try:
                await w.wait_closed()
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

    async def _broadcast_to_clients(self, data: bytes) -> None:
        if not self.clients:
            return

        dead = []
        for w in self.clients:
            try:
                w.write(data)
            except Exception:
                dead.append(w)

        for w in list(self.clients):
            if w in dead:
                continue
            try:
                await w.drain()
            except Exception:
                dead.append(w)

        for w in dead:
            self.clients.discard(w)
            try:
                w.close()
            except Exception:
                pass


def parse_args():
    ap = argparse.ArgumentParser(
        description="Serial broadcast proxy (multi-client TCP -> single USB companion)"
    )
    ap.add_argument("--listen-host", default="0.0.0.0")
    ap.add_argument("--listen-port", type=int, required=True)
    ap.add_argument("--serial-port", required=True, help="Serial device, e.g. COM7 or /dev/ttyACM0")
    ap.add_argument("--baudrate", type=int, default=115200)
    ap.add_argument("--reconnect-delay", type=float, default=1.0)
    ap.add_argument("--read-timeout", type=float, default=0.2)
    ap.add_argument("--write-timeout", type=float, default=1.0)
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
