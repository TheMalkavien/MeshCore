# RXPS v2 integration

Based on IoTThinks/MeshCore PowerSaving-v17 at
f89446c28d8758bb09a2008a8985f8aaf30b485e (17.1.6):
https://github.com/IoTThinks/MeshCore/tree/f89446c28d8758bb09a2008a8985f8aaf30b485e

The imported RXPowerSaving calculator, CLI and radio duty-cycle implementation
are derived from that repository's MIT-licensed code. Hardware measurements in
imported comments are the upstream authors' results; no current or packet-loss
measurements were performed for this port.

## Profiles

- WisMesh Tag and T1000-E BLE/USB companions default to level 6, sender preamble 16.
- RP2040 MLK_RP2040_LOWPOWER profiles enable the same RXPS geometry; an explicit
  RXPS_DEFAULT_ENABLED=0 disables it. MCU clock/voltage and OTA policy are preserved.
- Heltec V3/V4 companion low-power defaults use level 6/preamble 16. Existing
  persisted levels, enable flags and preamble choices remain authoritative.
- General repeaters retain continuous RX unless explicitly opted in. The ESP32
  task notification waits, automatic light sleep and radio watchdog are retained.
- T1000-E tracker defaults to sleep enabled on a fresh configuration. Its radio
  remains completely off between cycles. Existing tracker configuration is
  preserved: use `tracker sleep on` if it previously saved sleep disabled.
- The LR1110 DC/DC regulator is selected at every successful begin(), including
  the tracker. Both radio families use MC_TCXO_DELAY_US=1600 by default in the
  RXPS branches; a board can override this startup margin at build time.

Preamble 16 is deliberately compatible with peers that transmit short preambles.
Only select preamble 32 after checking the transmitting peers. Levels 9 and 10
are unguarded profiles and are never selected by default.

## Controls and diagnostics

The integration's companions accept `get rxps`, `set rxps balanced`,
`set rxps level 6 preamble 16`, and `set rxps off` through their existing CLI.
Preferences are serialized with the existing JSON format; legacy binary offsets
are unchanged. `get rxps` reports desired/effective mode, errors and arm failures.

The ESP32 branch retains its previous companion custom variables and serial
syntax: `set rxps 6 16`, `set rxps off`, `get rxps`. It additionally reports the
armed periods, error code and arm failure count. Keep existing BLE/radio settings
when comparing battery life; GPS, logging and traffic affect the comparison.

Three consecutive duty-cycle arm failures stop retries until configuration or
statistics are reset. Continuous reception is used in that case. RXPS avoids
SPI probes while BUSY indicates a sleep window, stops the duty sequencer before
TX/CAD/configuration, and briefly enables continuous RX for floor calibration.
Received RSSI/SNR are cached before re-arming. LR1110 uses its own 32.768 kHz
register conversion and restores its normal LF clock when stopping RXPS.

## Validation

Builds must cover RAK_WisMesh_Tag_companion_radio_ble,
t1000e_companion_radio_ble, waveshare_rp2040_lora_repeater_lowpower,
Heltec_v3_companion_radio_ble_idf, heltec_v4_companion_radio_ble_idf,
and t1000e_tracker from the corresponding integration/ESP32/tracker branches.

The standalone C++ test tools/tests/rxps_v2_invariants.cpp checks actual SX126x
register timing, catch margin, invalid/nonfinite input, impossible geometry and
retry fallback over 7680 parameter combinations. On a native compiler:

    c++ -std=c++17 -Isrc/helpers/radiolib tools/tests/rxps_v2_invariants.cpp src/helpers/radiolib/RXPowerSaving.cpp -o rxps-tests
    ./rxps-tests

To verify the tracker timer, inspect the firmware symbols for RTC2_IRQHandler
and nrf52_sleep_timer_arm. Then test a short interval on hardware, button/USB
wakeup, radio TX/RX after wake and GPS power gating before a long battery run.

## Branches

feature/rxps-v2: reusable radio changes based on feature/core-enhancements.
feature/rxps-v2-esp32: radio adaptation based on HTv4LowPower.
feature/rxps-v2-tracker: corrections based on sensor_tracker.
review/rxps-v2-integration: resolved candidate based on patch_public.
packaging/mlk-defaults: restack registration and maintenance documentation.

The original patch_public worktree and its uncommitted changes are untouched.
Nothing has been pushed, flashed or measured on hardware.
