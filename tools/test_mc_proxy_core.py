import struct
import unittest

from mc_proxy_core import (
    CONTACT_TYPE,
    NEW_ADVERT_TYPE,
    ClientState,
    ProxyCore,
    RESP_CODE_CONTACTS_START,
    RESP_CODE_END_OF_CONTACTS,
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


if __name__ == "__main__":
    unittest.main()
