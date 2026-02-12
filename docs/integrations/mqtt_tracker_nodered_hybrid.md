# MeshCore MQTT tracker -> Node-RED -> Home Assistant

This setup uses an external decoder service (script running next to Node-RED).

MeshCore MQTT bridge publishes encrypted group packets on:
- `meshcore/{IATA}/{DEVICE_ID}/packets`

The decoder script decrypts group text and republishes normalized tracker JSON on:
- `meshcore/decoded/{IATA}/{DEVICE_ID}/tracker`
Optionally (if enabled), it can also publish decrypted text on:
- `meshcore/decoded/{IATA}/{DEVICE_ID}/text`

Node-RED only subscribes to decoded topics and pushes positions to Home Assistant.

## 1) Install dependencies (decoder host)

```powershell
py -m pip install -r tools/requirements-mqtt-tracker.txt
```

## 2) Run decoder as external service

```powershell
$env:MESHCORE_MQTT_HOST="127.0.0.1"
$env:MESHCORE_MQTT_PORT="1883"
$env:MESHCORE_MQTT_USER=""
$env:MESHCORE_MQTT_PASS=""
$env:MESHCORE_CHANNELS="Odyssee=ZDRjODRkNmZkYjEwMTk2MmMxMDJiNjBlMGQ4MGQ5ZjQ="
py tools/mqtt_tracker_bridge.py --debug
```

Optional bootstrap (auto-install missing deps):

```powershell
py tools/ensure_python_deps.py
py tools/mqtt_tracker_bridge.py --debug
```

## 3) Import Node-RED flow

Import:
- `docs/integrations/node_red_meshcore_tracker_flow.json`

Then update:
- MQTT broker config node (`REPLACE_MQTT_BROKER`)
- Home Assistant server in `api-call-service` node (`REPLACE_WITH_HA_SERVER_ID`)

The flow subscribes to:
- `meshcore/decoded/+/+/tracker`

And calls:
- domain: `device_tracker`
- service: `see`

## 4) Environment variables for decoder

- `MESHCORE_CHANNELS` (required)
- `MESHCORE_MQTT_HOST`
- `MESHCORE_MQTT_PORT`
- `MESHCORE_MQTT_USER`
- `MESHCORE_MQTT_PASS`
- `MESHCORE_MQTT_TOPIC` (default `meshcore/+/+/packets`)
- `MESHCORE_MQTT_OUT_PREFIX` (default `meshcore/decoded`)
- `MESHCORE_PUBLISH_TEXT` (default `0`; set `1` to publish `/text` topic)
- `MESHCORE_DEDUPE_SECONDS` (default `15`; ignore duplicate packets in this window, set `0` to disable)

## Notes

- Crypto format matches MeshCore current implementation:
  - HMAC-SHA256 truncated to 2 bytes
  - AES-128-ECB decrypt, zero-padded plaintext
- `packet_timestamp` from mesh is preserved in output payload.
- `received_at` is decoder reception time (Unix epoch seconds).
- Home Assistant `device_tracker.see` should receive top-level fields like:
  `dev_id`, `gps`, `gps_accuracy`, `source_type` (not nested custom `attributes`).
