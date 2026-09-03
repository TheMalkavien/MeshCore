#!/usr/bin/env python3
"""Check that the web OTA tool accepts a nRF52 DFU .zip the way it should.

The .zip is the other artifact the nRF52 toolchain produces next to the .hex,
and the one anyone already has on hand for a normal DFU flash. Two things have
to hold before it can be fed to a mesh OTA transfer:

  1. the application image inside the package is byte-identical to the one
     derived from the .hex of the same build - otherwise the two entry points
     would flash different firmware;
  2. a package carrying a SoftDevice or a bootloader is refused. Those images
     are not application images; writing one at 0x26000 would brick the node.

This script builds the fixtures (including an adversarial mixed package) and
runs them through the actual reader from tools/web_ota_otg/app.js, via node.

Usage:
    python tools/tests/dfu_zip_check.py [.pio/build/<env>/firmware.zip]
"""

import json
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HARNESS = Path(__file__).with_suffix(".cjs")


def hex_to_bin(path):
    """Same reconstruction the tool does in JS, kept independent of it."""
    segs = {}
    base = 0
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line.startswith(":"):
            continue
        n = int(line[1:3], 16)
        addr = int(line[3:7], 16)
        rtype = int(line[7:9], 16)
        data = bytes.fromhex(line[9:9 + 2 * n])
        if rtype == 0:
            segs[base + addr] = data
        elif rtype == 4:
            base = int.from_bytes(data, "big") << 16
        elif rtype == 2:
            base = int.from_bytes(data, "big") << 4
    lo = min(segs)
    hi = max(k + len(v) for k, v in segs.items())
    buf = bytearray(b"\xff" * (hi - lo))
    for k, v in segs.items():
        buf[k - lo:k - lo + len(v)] = v
    return lo, bytes(buf)


def build_fixtures(zip_path, hex_path, out):
    start, raw = hex_to_bin(hex_path)
    if start != 0x26000:
        raise SystemExit("unexpected hex base 0x%X" % start)
    (out / "ref.bin").write_bytes(raw)

    src = zipfile.ZipFile(zip_path)
    names = src.namelist()
    manifest = json.loads(src.read("manifest.json"))

    # Same package, but every entry deflated instead of stored: nothing
    # guarantees a future toolchain keeps storing them.
    with zipfile.ZipFile(out / "deflated.zip", "w", zipfile.ZIP_DEFLATED) as z:
        for name in names:
            z.writestr(name, src.read(name))

    # A package that also carries a SoftDevice + bootloader image.
    mixed = json.loads(json.dumps(manifest))
    mixed["manifest"]["softdevice_bootloader"] = {
        "bin_file": "sd_bl.bin", "dat_file": "sd_bl.dat"}
    with zipfile.ZipFile(out / "mixed.zip", "w") as z:
        z.writestr("manifest.json", json.dumps(mixed))
        z.writestr("firmware.bin", src.read("firmware.bin"))
        z.writestr("sd_bl.bin", b"\x00" * 64)

    # Any old zip.
    with zipfile.ZipFile(out / "plain.zip", "w") as z:
        z.writestr("notes.txt", "rien a voir")

    # Manifest pointing at an image that is not in the package.
    broken = json.loads(json.dumps(manifest))
    broken["manifest"]["application"]["bin_file"] = "absent.bin"
    with zipfile.ZipFile(out / "broken.zip", "w") as z:
        z.writestr("manifest.json", json.dumps(broken))
        z.writestr("firmware.bin", src.read("firmware.bin"))

    return len(raw)


def main():
    if len(sys.argv) > 1:
        zip_path = Path(sys.argv[1])
    else:
        found = sorted((ROOT / ".pio" / "build").glob("*lora_ota*/firmware.zip"))
        if not found:
            print("no OTA build found under .pio/build; nothing to check")
            return 0
        zip_path = found[0]

    hex_path = zip_path.with_name("firmware.hex")
    if not hex_path.exists():
        print("%s has no firmware.hex next to it" % zip_path.parent.name)
        return 1
    if shutil.which("node") is None:
        print("node not found; skipped (the reader under test is JavaScript)")
        return 0

    print("%s: %s" % (zip_path.parent.name, zip_path.name), flush=True)
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        size = build_fixtures(zip_path, hex_path, out)
        print("  reference image from the .hex: %d bytes" % size, flush=True)
        rc = subprocess.run(
            ["node", str(HARNESS), str(zip_path), str(out / "ref.bin"),
             str(out / "deflated.zip"), str(out / "mixed.zip"),
             str(out / "plain.zip"), str(out / "broken.zip")],
            cwd=str(ROOT)).returncode
    return rc


if __name__ == "__main__":
    sys.exit(main())
