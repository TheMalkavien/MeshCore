import asyncio
import struct
import unittest
from unittest import mock

import mc_proxy_core
from mc_proxy_core import (
    CMD_APP_START,
    CONTACT_TYPE,
    MAX_PENDING_MESSAGE_FRAMES,
    NEW_ADVERT_TYPE,
    ClientState,
    ProxyCore,
    RESP_CODE_CONTACTS_START,
    RESP_CODE_END_OF_CONTACTS,
    RESP_CODE_SELF_INFO,
)


def contact_frame(pubkey_byte: int, lastmod: int, packet_type: int = CONTACT_TYPE) -> bytes:
    frame = bytearray(151)
    frame[0] = ord(">")
    struct.pack_into("<H", frame, 1, 148)
    frame[3] = packet_type
    frame[4:36] = bytes((pubkey_byte,)) * 32
    struct.pack_into("<I", frame, 147, lastmod)
    return bytes(frame)


def contacts_start(count: int) -> bytes:
    return bytes((ord(">"), 5, 0, RESP_CODE_CONTACTS_START)) + struct.pack("<I", count)


def end_of_contacts(lastmod: int = 0) -> bytes:
    return bytes((ord(">"), 5, 0, RESP_CODE_END_OF_CONTACTS)) + struct.pack("<I", lastmod)


class MemoryProxy(ProxyCore):
    def __init__(self):
        super().__init__("127.0.0.1", 0)
        self.backend_writes: list[bytes] = []
        self.client_writes: dict[str, list[bytes]] = {}

    def _backend_is_writable(self) -> bool:
        return self.backend_connected.is_set()

    async def _transport_write(self, data: bytes) -> None:
        self.backend_writes.append(data)

    async def _send_bytes_to_client(self, state: ClientState, payload: bytes) -> bool:
        self.client_writes.setdefault(state.client_id, []).append(payload)
        return True


def connected_client(proxy: MemoryProxy, client_id: str) -> ClientState:
    state = ClientState(client_id=client_id, peer_group=client_id)
    state.writer = object()
    proxy.client_states[client_id] = state
    return state


class ContactCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_advert_is_forwarded_but_never_promoted_to_contact(self):
        proxy = MemoryProxy()
        client = connected_client(proxy, "client")
        candidate = contact_frame(0xAA, 123, NEW_ADVERT_TYPE)

        await proxy._handle_backend_frame(candidate)

        self.assertNotIn(CONTACT_TYPE, proxy._state_cache)
        self.assertFalse(proxy._contacts_dirty)
        self.assertEqual(proxy.client_writes[client.client_id], [candidate])

        stored_advert = bytes((ord(">"), 33, 0, 0x80)) + bytes((0xAA,)) * 32
        await proxy._handle_backend_frame(stored_advert)
        self.assertTrue(proxy._contacts_dirty)

    async def test_one_full_sync_serves_multiple_waiters(self):
        proxy = MemoryProxy()
        first = connected_client(proxy, "first")
        second = connected_client(proxy, "second")
        proxy.backend_connected.set()
        proxy._queue_contact_sync(first, 0)
        proxy._queue_contact_sync(second, 50)

        await proxy._start_contact_sync()
        self.assertEqual(len(proxy.backend_writes), 1)
        self.assertEqual(struct.unpack_from("<I", proxy.backend_writes[0], 4)[0], 0)

        contact = contact_frame(0x11, 100)
        await proxy._handle_backend_frame(contacts_start(1))
        await proxy._handle_backend_frame(contact)
        await proxy._handle_backend_frame(end_of_contacts(100))

        self.assertTrue(proxy._contacts_synced)
        self.assertFalse(proxy._contacts_dirty)
        self.assertEqual(len(proxy._state_cache[CONTACT_TYPE]), 1)
        self.assertEqual([frame[3] for frame in proxy.client_writes["first"]], [2, 3, 4])
        self.assertEqual([frame[3] for frame in proxy.client_writes["second"]], [2, 3, 4])

    async def test_delta_count_mismatch_falls_back_to_full_snapshot(self):
        proxy = MemoryProxy()
        client = connected_client(proxy, "client")
        old_a = contact_frame(0x21, 10)
        old_b = contact_frame(0x22, 11)
        proxy._state_cache[CONTACT_TYPE] = {old_a[4:36]: old_a, old_b[4:36]: old_b}
        proxy._contacts_synced = True
        proxy._contacts_dirty = True
        proxy._contacts_lastmod = 11
        proxy.backend_connected.set()
        proxy._queue_contact_sync(client, 0)

        await proxy._start_contact_sync()
        self.assertEqual(struct.unpack_from("<I", proxy.backend_writes[0], 4)[0], 10)

        # The backend now has only one contact. A delta cannot name the deleted
        # key, so the count mismatch must trigger a full replacement request.
        await proxy._handle_backend_frame(contacts_start(1))
        await proxy._handle_backend_frame(end_of_contacts())
        self.assertEqual(len(proxy.backend_writes), 2)
        self.assertEqual(struct.unpack_from("<I", proxy.backend_writes[1], 4)[0], 0)
        self.assertNotIn("client", proxy.client_writes)

        await proxy._handle_backend_frame(contacts_start(1))
        await proxy._handle_backend_frame(old_a)
        await proxy._handle_backend_frame(end_of_contacts(10))

        self.assertEqual(list(proxy._state_cache[CONTACT_TYPE]), [old_a[4:36]])
        self.assertFalse(proxy._contacts_dirty)
        self.assertEqual([frame[3] for frame in proxy.client_writes["client"]], [2, 3, 4])

    async def test_synchronized_empty_cache_is_served_without_backend(self):
        proxy = MemoryProxy()
        client = connected_client(proxy, "client")
        proxy._state_cache[CONTACT_TYPE] = {}
        proxy._contacts_synced = True

        await proxy._send_cached_contacts(client, 0)

        self.assertEqual(proxy.backend_writes, [])
        self.assertEqual([frame[3] for frame in proxy.client_writes["client"]], [2, 4])
        self.assertEqual(struct.unpack_from("<I", proxy.client_writes["client"][0], 4)[0], 0)


def client_frame(cmd: int, body: bytes = b"") -> bytes:
    payload = bytes((cmd,)) + body
    return b"<" + struct.pack("<H", len(payload)) + payload


def self_info_frame(name: bytes) -> bytes:
    payload = bytes((RESP_CODE_SELF_INFO,)) + name
    return b">" + struct.pack("<H", len(payload)) + payload


class FakeBackendProxy(MemoryProxy):
    """MemoryProxy with the remaining transport hooks stubbed out."""

    def __init__(self):
        super().__init__()
        self.after_write_error_calls = 0
        self.fail_writes = False

    async def _transport_write(self, data: bytes) -> None:
        if self.fail_writes:
            raise OSError("boom")
        await super()._transport_write(data)

    def _transport_after_write_error(self) -> None:
        self.after_write_error_calls += 1

    async def _transport_close(self) -> None:
        pass

    def _log_listening(self, addr: str) -> None:
        pass


async def eventually(predicate, timeout: float = 3.0) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


class NodeInfoRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def test_unchanged_self_info_is_not_pushed_to_clients(self):
        proxy = FakeBackendProxy()
        connected_client(proxy, "client")
        frame = self_info_frame(b"node-a")
        proxy._state_cache[RESP_CODE_SELF_INFO] = {None: frame}
        proxy._self_info_refresh_deadline = asyncio.get_running_loop().time() + 3

        await proxy._handle_backend_frame(frame)

        self.assertNotIn("client", proxy.client_writes)
        self.assertTrue(proxy._self_info_seen.is_set())

    async def test_changed_self_info_updates_cache_and_is_pushed(self):
        proxy = FakeBackendProxy()
        connected_client(proxy, "client")
        proxy._state_cache[RESP_CODE_SELF_INFO] = {None: self_info_frame(b"old-name")}
        proxy._self_info_refresh_deadline = asyncio.get_running_loop().time() + 3
        renamed = self_info_frame(b"new-name")

        await proxy._handle_backend_frame(renamed)

        self.assertEqual(proxy._state_cache[RESP_CODE_SELF_INFO][None], renamed)
        self.assertEqual(proxy.client_writes["client"], [renamed])

    async def test_self_info_is_relayed_while_a_client_awaits_its_own(self):
        proxy = FakeBackendProxy()
        client = connected_client(proxy, "client")
        client.awaiting_replay_after_self_info = True
        frame = self_info_frame(b"node-a")
        proxy._state_cache[RESP_CODE_SELF_INFO] = {None: frame}
        proxy._self_info_refresh_deadline = asyncio.get_running_loop().time() + 3

        await proxy._handle_backend_frame(frame)

        self.assertEqual(proxy.client_writes["client"][0], frame)

    async def test_self_info_outside_refresh_window_is_relayed(self):
        proxy = FakeBackendProxy()
        connected_client(proxy, "client")
        frame = self_info_frame(b"node-a")
        proxy._state_cache[RESP_CODE_SELF_INFO] = {None: frame}

        await proxy._handle_backend_frame(frame)

        self.assertEqual(proxy.client_writes["client"], [frame])

    async def test_config_command_triggers_immediate_refresh(self):
        proxy = FakeBackendProxy()
        proxy.backend_connected.set()
        proxy._server = await asyncio.start_server(proxy._handle_client, "127.0.0.1", 0)
        refresh_task = asyncio.create_task(proxy._node_info_refresh_loop())
        port = proxy._server.sockets[0].getsockname()[1]
        _reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            writer.write(client_frame(CMD_APP_START, b"\x03" + b"\x00" * 6 + b"app"))
            writer.write(client_frame(0x08, b"NewName"))  # CMD_SET_ADVERT_NAME
            await writer.drain()

            def refresh_sent() -> bool:
                return any(b"mc-proxy-refresh" in w for w in proxy.backend_writes)

            self.assertTrue(await eventually(refresh_sent, timeout=3.0))
            set_name_idx = next(
                i for i, w in enumerate(proxy.backend_writes) if b"NewName" in w
            )
            refresh_idx = next(
                i for i, w in enumerate(proxy.backend_writes) if b"mc-proxy-refresh" in w
            )
            self.assertLess(set_name_idx, refresh_idx)
        finally:
            writer.close()
            refresh_task.cancel()
            await asyncio.gather(refresh_task, return_exceptions=True)
            await proxy.stop()


class BackendWriteTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_to_backend_reports_success_and_failure(self):
        proxy = FakeBackendProxy()
        self.assertFalse(await proxy._send_to_backend(b"x"))  # backend down

        proxy.backend_connected.set()
        self.assertTrue(await proxy._send_to_backend(b"x"))

        proxy.fail_writes = True
        self.assertFalse(await proxy._send_to_backend(b"y"))
        self.assertFalse(proxy.backend_connected.is_set())
        self.assertEqual(proxy.after_write_error_calls, 1)


class WatchdogTests(unittest.IsolatedAsyncioTestCase):
    async def test_unresponsive_node_forces_a_reconnect(self):
        proxy = FakeBackendProxy()
        proxy.backend_connected.set()
        with mock.patch.object(mc_proxy_core, "POLL_RESPONSE_TIMEOUT", 0.05), \
                mock.patch.object(mc_proxy_core, "BACKEND_WATCHDOG_TIMEOUTS", 2):
            task = asyncio.create_task(proxy._poll_backend_loop())
            try:
                self.assertTrue(
                    await eventually(lambda: not proxy.backend_connected.is_set())
                )
                self.assertGreaterEqual(proxy.after_write_error_calls, 1)
            finally:
                proxy._stop.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


class ClientLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_shutdown_does_not_wait_for_connected_clients(self):
        proxy = FakeBackendProxy()
        proxy._server = await asyncio.start_server(proxy._handle_client, "127.0.0.1", 0)
        port = proxy._server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            await eventually(lambda: len(proxy.client_states) == 1)
            # A client that stays connected must not stall the shutdown
            # (Server.wait_closed() waits for open connections on Python 3.12+).
            await asyncio.wait_for(proxy.stop(), timeout=4.0)
            self.assertEqual(await asyncio.wait_for(reader.read(1), timeout=2.0), b"")
        finally:
            writer.close()

    async def test_client_that_never_sends_app_start_is_dropped(self):
        proxy = FakeBackendProxy()
        proxy._server = await asyncio.start_server(proxy._handle_client, "127.0.0.1", 0)
        port = proxy._server.sockets[0].getsockname()[1]
        with mock.patch.object(mc_proxy_core, "UNIDENTIFIED_CLIENT_TIMEOUT", 0.2):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            try:
                self.assertEqual(await asyncio.wait_for(reader.read(1), timeout=3.0), b"")
            finally:
                writer.close()
                await proxy.stop()


class PendingQueueTests(unittest.TestCase):
    def test_pending_message_queue_is_bounded(self):
        state = ClientState(client_id="c", peer_group="c")
        for seq in range(MAX_PENDING_MESSAGE_FRAMES + 10):
            state.pending_message_frames.append(object())
        self.assertEqual(len(state.pending_message_frames), MAX_PENDING_MESSAGE_FRAMES)


if __name__ == "__main__":
    unittest.main()
