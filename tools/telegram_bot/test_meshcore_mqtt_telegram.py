import hashlib
import hmac
import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path

from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers import algorithms, modes


try:
    import paho.mqtt.client  # noqa: F401
except ModuleNotFoundError:
    paho_module = types.ModuleType("paho")
    paho_mqtt_module = types.ModuleType("paho.mqtt")
    paho_client_module = types.ModuleType("paho.mqtt.client")
    paho_client_module.MQTT_ERR_SUCCESS = 0
    paho_client_module.Client = object
    paho_mqtt_module.client = paho_client_module
    paho_module.mqtt = paho_mqtt_module
    sys.modules["paho"] = paho_module
    sys.modules["paho.mqtt"] = paho_mqtt_module
    sys.modules["paho.mqtt.client"] = paho_client_module


try:
    from Crypto.Cipher import AES  # noqa: F401
except ModuleNotFoundError:
    class _AesEcbCipher:
        def __init__(self, key: bytes):
            self._cipher = Cipher(algorithms.AES(key), modes.ECB())

        def encrypt(self, data: bytes) -> bytes:
            encryptor = self._cipher.encryptor()
            return encryptor.update(data) + encryptor.finalize()

        def decrypt(self, data: bytes) -> bytes:
            decryptor = self._cipher.decryptor()
            return decryptor.update(data) + decryptor.finalize()

    class _AesCompat:
        MODE_ECB = 1

        @staticmethod
        def new(key: bytes, mode: int) -> _AesEcbCipher:
            if mode != _AesCompat.MODE_ECB:
                raise ValueError("Only ECB mode is supported by the test adapter")
            return _AesEcbCipher(key)

    AES = _AesCompat
    crypto_module = types.ModuleType("Crypto")
    crypto_cipher_module = types.ModuleType("Crypto.Cipher")
    crypto_cipher_module.AES = _AesCompat
    crypto_module.Cipher = crypto_cipher_module
    sys.modules["Crypto"] = crypto_module
    sys.modules["Crypto.Cipher"] = crypto_cipher_module


MODULE_PATH = Path(__file__).with_name("meshcore_mqtt_telegram.py")
SPEC = importlib.util.spec_from_file_location("meshcore_mqtt_telegram", MODULE_PATH)
assert SPEC and SPEC.loader
BOT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BOT)


class FakeMqttMessage:
    def __init__(self, topic: str, payload: dict):
        self.topic = topic
        self.payload = json.dumps(payload).encode("utf-8")


def make_raw_group_message(
    psk: bytes,
    timestamp: int,
    sender: str,
    message: str,
    *,
    attempt: int = 0,
) -> str:
    secret = psk + bytes(16)
    plaintext = (
        timestamp.to_bytes(4, byteorder="little")
        + bytes([attempt & 0x03])
        + f"{sender}: {message}".encode("utf-8")
    )
    plaintext += bytes((-len(plaintext)) % BOT.CIPHER_BLOCK_SIZE)
    ciphertext = AES.new(psk, AES.MODE_ECB).encrypt(plaintext)
    mac = hmac.new(secret, ciphertext, hashlib.sha256).digest()[:2]

    header = (BOT.PAYLOAD_TYPE_GRP_TXT << BOT.PH_TYPE_SHIFT) | 0x01
    path_descriptor = 0x42  # two hops, two bytes per path hash
    path = bytes.fromhex("11223344")
    channel_hash = bytes([BOT.compute_channel_hash(psk)])
    return (bytes([header, path_descriptor]) + path + channel_hash + mac + ciphertext).hex()


class MeshCoreMqttTelegramFallbackTests(unittest.TestCase):
    def setUp(self):
        self.psk = bytes.fromhex("00112233445566778899aabbccddeeff")
        mapping = BOT.ChannelMapping(
            self.psk,
            "-100123456789",
            "Public",
            channel_name="Public",
            channel_idx=0,
        )
        self.bot = BOT.MeshCoreMQTTBot(
            mqtt_host="127.0.0.1",
            mqtt_port=1883,
            mqtt_user=None,
            mqtt_password=None,
            mqtt_topic="meshcore/+/+/packets",
            mqtt_message_topic="meshcore/+/+/messages",
            telegram_token="test-token",
            channel_mappings=[mapping],
            dedup_ttl=120,
            content_dedup_ttl=5,
            stats_interval=0,
        )

    def decoded_event(self, timestamp=1784912073):
        return {
            "type": "MESSAGE",
            "direction": "rx",
            "message_type": "channel",
            "origin": "44MLK - Base",
            "channel": "Public",
            "channel_idx": 0,
            "sender": "PKT_GHOST👻",
            "message": "Message lointain",
            "timestamp": timestamp,
            "hop_count": 12,
        }

    def raw_event(self, timestamp=1784912073):
        return {
            "type": "PACKET",
            "direction": "rx",
            "packet_type": "5",
            "origin": "44MLK - Base",
            "raw": make_raw_group_message(
                self.psk,
                timestamp,
                "PKT_GHOST👻",
                "Message lointain",
                attempt=2,
            ),
            "hash": "AABBCCDD",
            "SNR": "7.5",
            "RSSI": "-91",
        }

    def test_decoded_message_is_forwarded_without_raw_packet(self):
        self.bot._handle_message(
            FakeMqttMessage("meshcore/NTE/base/messages", self.decoded_event())
        )

        route, sender, message, telegram_text = self.bot._tg_queue.get_nowait()
        self.assertEqual(route.name, "Public")
        self.assertEqual(sender, "PKT_GHOST👻")
        self.assertEqual(message, "Message lointain")
        self.assertEqual(telegram_text, "[PKT_GHOST👻] : Message lointain")
        self.assertEqual(self.bot._stats.snapshot()["decoded_fallbacks"], 1)

    def test_raw_then_decoded_copy_is_deduplicated(self):
        self.bot._handle_message(
            FakeMqttMessage("meshcore/NTE/base/packets", self.raw_event())
        )
        self.bot._handle_message(
            FakeMqttMessage("meshcore/NTE/base/messages", self.decoded_event())
        )

        self.bot._tg_queue.get_nowait()
        self.assertTrue(self.bot._tg_queue.empty())
        self.assertEqual(self.bot._stats.snapshot()["deduplicated"], 1)

    def test_decoded_then_raw_copy_is_deduplicated(self):
        self.bot._handle_message(
            FakeMqttMessage("meshcore/NTE/base/messages", self.decoded_event())
        )
        self.bot._handle_message(
            FakeMqttMessage("meshcore/NTE/base/packets", self.raw_event())
        )

        self.bot._tg_queue.get_nowait()
        self.assertTrue(self.bot._tg_queue.empty())
        self.assertEqual(self.bot._stats.snapshot()["deduplicated"], 1)

    def test_timestamp_mismatch_between_sources_is_deduplicated_by_content(self):
        self.bot._handle_message(
            FakeMqttMessage(
                "meshcore/NTE/base/messages",
                self.decoded_event(timestamp=None),
            )
        )
        self.bot._handle_message(
            FakeMqttMessage("meshcore/NTE/base/packets", self.raw_event())
        )

        self.bot._tg_queue.get_nowait()
        self.assertTrue(self.bot._tg_queue.empty())
        self.assertEqual(self.bot._stats.snapshot()["deduplicated"], 1)

    def test_two_decoded_copies_with_different_timestamps_are_deduplicated(self):
        self.bot._handle_message(
            FakeMqttMessage("meshcore/NTE/base/messages", self.decoded_event())
        )
        self.bot._handle_message(
            FakeMqttMessage(
                "meshcore/NTE/base/messages",
                self.decoded_event(timestamp=1784912074),
            )
        )

        self.bot._tg_queue.get_nowait()
        self.assertTrue(self.bot._tg_queue.empty())
        self.assertEqual(self.bot._stats.snapshot()["deduplicated"], 1)

    def test_empty_stats_packet_is_ignored(self):
        self.bot._handle_message(
            FakeMqttMessage(
                "meshcore/NTE/base/packets",
                {
                    "type": "PACKET",
                    "direction": "rx",
                    "packet_type": "",
                    "raw": "",
                },
            )
        )

        self.assertTrue(self.bot._tg_queue.empty())

    def test_wrong_decoded_channel_is_ignored(self):
        event = self.decoded_event()
        event["channel"] = "#other"
        event["channel_idx"] = 3
        self.bot._handle_message(
            FakeMqttMessage("meshcore/NTE/base/messages", event)
        )

        self.assertTrue(self.bot._tg_queue.empty())


if __name__ == "__main__":
    unittest.main()
