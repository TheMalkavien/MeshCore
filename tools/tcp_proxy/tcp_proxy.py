#!/usr/bin/env python3
"""
TCP broadcast proxy:
- Multi clients -> single backend connection
- Backend -> broadcast to all clients
- Clients -> backend (merged)
- Simple, no sessions/state
- Shutdown that actually stops on Ctrl-C
"""

import asyncio
import argparse
import signal
from typing import Set, Optional


class BroadcastProxy:
    def __init__(
        self,
        listen_host: str,
        listen_port: int,
        backend_host: str,
        backend_port: int,
        backend_reconnect_delay: float = 1.0,
        log: bool = True,
    ):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.backend_host = backend_host
        self.backend_port = backend_port
        self.backend_reconnect_delay = backend_reconnect_delay
        self.log = log

        self.clients: Set[asyncio.StreamWriter] = set()

        self.backend_reader: Optional[asyncio.StreamReader] = None
        self.backend_writer: Optional[asyncio.StreamWriter] = None
        self.backend_lock = asyncio.Lock()
        self.backend_connected = asyncio.Event()

        self._server: Optional[asyncio.base_events.Server] = None
        self._stop = asyncio.Event()
        self._backend_task: Optional[asyncio.Task] = None

    def _p(self, msg: str) -> None:
        if self.log:
            print(msg, flush=True)

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, host=self.listen_host, port=self.listen_port
        )
        addr = ", ".join(str(sock.getsockname()) for sock in (self._server.sockets or []))
        self._p(f"[proxy] listening on {addr} -> backend {self.backend_host}:{self.backend_port}")

        self._backend_task = asyncio.create_task(self._backend_manager())

        try:
            async with self._server:
                await self._stop.wait()
        finally:
            self._p("[proxy] stopping...")

            # Stop accepting new clients immediately
            if self._server:
                self._server.close()
                await self._server.wait_closed()

            # Close everything to unblock pending reads
            await self._close_all_clients()
            await self._close_backend()

            # Cancel backend task if still running
            if self._backend_task and not self._backend_task.done():
                self._backend_task.cancel()
                try:
                    await self._backend_task
                except asyncio.CancelledError:
                    pass

    async def stop(self) -> None:
        # Make stop idempotent and aggressive:
        if self._stop.is_set():
            return
        self._stop.set()

        # Proactively close sockets to unblock tasks right now
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
                    # keep it simple: drop while backend is down
                    self._p("[proxy] backend down, dropping client->backend bytes")

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

            if self.backend_connected.is_set() and self.backend_reader:
                try:
                    while not self._stop.is_set():
                        data = await self.backend_reader.read(65536)
                        if not data:
                            raise ConnectionError("backend closed")
                        await self._broadcast_to_clients(data)
                except (asyncio.CancelledError, ConnectionError):
                    self._p("[backend] disconnected")
                except Exception as e:
                    self._p(f"[backend] read error: {e}")
                finally:
                    self.backend_connected.clear()
                    await self._close_backend()

            await asyncio.sleep(self.backend_reconnect_delay)

    async def _connect_backend(self) -> None:
        try:
            self._p(f"[backend] connecting to {self.backend_host}:{self.backend_port} ...")
            r, w = await asyncio.open_connection(self.backend_host, self.backend_port)
            self.backend_reader, self.backend_writer = r, w
            self.backend_connected.set()
            self._p("[backend] connected")
        except Exception as e:
            self.backend_connected.clear()
            self.backend_reader, self.backend_writer = None, None
            self._p(f"[backend] connect failed: {e}")

    async def _close_backend(self) -> None:
        self.backend_connected.clear()
        if self.backend_writer:
            try:
                self.backend_writer.close()
                await self.backend_writer.wait_closed()
            except Exception:
                pass
        self.backend_reader, self.backend_writer = None, None

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
            if not self.backend_writer or self._stop.is_set():
                return
            try:
                self.backend_writer.write(data)
                await self.backend_writer.drain()
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
    ap = argparse.ArgumentParser(description="TCP broadcast proxy (multi-client -> single backend)")
    ap.add_argument("--listen-host", default="0.0.0.0")
    ap.add_argument("--listen-port", type=int, required=True)
    ap.add_argument("--backend-host", required=True)
    ap.add_argument("--backend-port", type=int, required=True)
    ap.add_argument("--reconnect-delay", type=float, default=1.0)
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
        log=not args.quiet,
    )

    loop = asyncio.get_running_loop()

    # Best effort signal handling (works on Linux; may fail on some setups)
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
        # Fallback if signal handlers didn't attach for some reason
        pass


if __name__ == "__main__":
    main()
