# simple_tracker

`simple_tracker` is a GPS tracker node example built on top of `SensorMesh`.
It is intended for boards with integrated GPS such as Heltec Wireless Tracker devices.

## Behavior

1. Wake periodically (`tracker interval <seconds>`).
2. Try to acquire a GPS fix for up to `tracker timeout` seconds (max 180).
3. Send one GPS report as encrypted group text containing a JSON payload on a configured group channel (`tracker group.psk ...`).
4. Sleep between cycles when enabled (`tracker sleep on`).

If sleep is disabled (`tracker sleep off`), the node stays awake between measurements to
reduce GPS time-to-fix.

Tracker payload format (compact JSON, versioned):

`{"t":"tracker","v":1,"lat":..,"lon":..,"alt":..,"sat":..,"spd":..,"dir":..,"fix":"live|cached"}`

## Custom commands

- `tracker` or `tracker status`
- `tracker interval <30-86400>`
- `tracker timeout <30-180>`
- `tracker group`
- `tracker group.name <name>`
- `tracker group.psk <base64-psk>`
- `tracker sleep on|off`
- `tracker now` (or `tracker send`)

## Build target

- `Heltec_Wireless_Tracker_tracker`
