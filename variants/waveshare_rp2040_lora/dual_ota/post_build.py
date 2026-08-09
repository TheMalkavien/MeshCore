"""Create explicitly named binaries for the Waveshare dual OTA layout."""

Import("env")  # type: ignore[name-defined]  # Provided by PlatformIO/SCons.

import binascii
import json
from pathlib import Path
import struct


SHIM_ADDRESS = 0x10004000
APP_ADDRESS = 0x10007000
SHIM_SIZE = 0x3000
MANIFEST_OFFSET = 0x2FB0
PARTITION_OFFSET = 0x2FF0
FS_START = 0x1017F000
FS_END = 0x101FF000
EEPROM_START = 0x101FF000
FLASH_LENGTH = 0x0017F000
MAX_APP_SIZE = FS_START - APP_ADDRESS
INSTALL_MAGIC = b"MC2INS01"


def crc32(data):
    return binascii.crc32(data) & 0xFFFFFFFF


def validate_vectors(blob, base, upper_bound, label):
    if len(blob) < 8:
        raise ValueError(f"{label} is too short")
    stack, reset = struct.unpack_from("<II", blob)
    reset_address = reset & ~1
    if stack < 0x20000000 or stack > 0x20042000 or stack & 3:
        raise ValueError(f"{label} has invalid stack vector 0x{stack:08x}")
    if not reset & 1 or reset_address < base or reset_address >= upper_bound:
        raise ValueError(f"{label} has invalid reset vector 0x{reset:08x}")
    return stack, reset


def installer_prefix(shim, app):
    if len(shim) > MANIFEST_OFFSET:
        raise ValueError(
            f"dual OTA shim is 0x{len(shim):x} bytes; max is 0x{MANIFEST_OFFSET:x}"
        )
    validate_vectors(shim, SHIM_ADDRESS, SHIM_ADDRESS + len(shim), "shim")

    prefix = bytearray(b"\xff" * SHIM_SIZE)
    prefix[: len(shim)] = shim
    prefix[PARTITION_OFFSET:SHIM_SIZE] = struct.pack(
        "<4I", FS_START, FS_END, EEPROM_START, FLASH_LENGTH
    )

    manifest_without_crc = struct.pack(
        "<8s13I",
        INSTALL_MAGIC,
        1,  # version
        64,  # header size
        SHIM_SIZE,
        SHIM_SIZE,  # app offset in the installer file
        APP_ADDRESS,
        SHIM_ADDRESS,
        FS_START,
        FS_END,
        len(shim),
        crc32(shim),
        len(app),
        crc32(app),
        0,
    )
    if len(manifest_without_crc) != 60:
        raise AssertionError("installer manifest ABI changed")
    manifest = manifest_without_crc + struct.pack("<I", crc32(manifest_without_crc))
    prefix[MANIFEST_OFFSET : MANIFEST_OFFSET + len(manifest)] = manifest
    return bytes(prefix)


def write_json(path, values):
    path.write_text(json.dumps(values, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_artifact(target, source, env):
    mode = env.GetProjectOption("custom_dual_ota_artifact")
    app_path = Path(str(source[0]))
    shim_path = Path(str(source[1]))
    app = app_path.read_bytes()

    if len(app) > MAX_APP_SIZE or len(app) & 3:
        raise ValueError(
            f"application is 0x{len(app):x} bytes; max is 0x{MAX_APP_SIZE:x} "
            "and the serial bootloader requires a 4-byte-aligned size"
        )
    app_stack, app_reset = validate_vectors(
        app, APP_ADDRESS, APP_ADDRESS + len(app), "application"
    )
    common = {
        "app_size": len(app),
        "app_crc32": f"0x{crc32(app):08x}",
        "app_initial_stack": f"0x{app_stack:08x}",
        "app_reset_vector": f"0x{app_reset:08x}",
    }

    if mode == "migration":
        prefix = installer_prefix(shim_path.read_bytes(), app)
        stage1 = prefix + app
        stage1_path = Path(str(target[0]))
        stage1_json = Path(str(target[1]))
        stage2_path = Path(str(target[2]))
        stage2_json = Path(str(target[3]))

        if (len(prefix) != SHIM_SIZE or SHIM_SIZE % 0x1000 != 0):
            raise AssertionError("stage 2 must be exactly one 12 KiB shim prefix")
        if stage1[:SHIM_SIZE] != prefix or stage1[SHIM_SIZE:] != app:
            raise AssertionError("stage 1/stage 2 migration pair is inconsistent")

        # The historical ESP32 flasher seals the exact uploaded file size.
        # Stage 1 installs shim+app, then stage 2 uploads the identical 12 KiB
        # prefix so the final serial header covers the immutable shim only.
        stage1_path.write_bytes(stage1)
        stage2_path.write_bytes(prefix)
        pair_crc = f"0x{crc32(stage1):08x}"
        write_json(stage1_json, {
            **common,
            "artifact": stage1_path.name,
            "format": "meshcore-rp2040-dual-ota-stage1-v1",
            "load_address": f"0x{SHIM_ADDRESS:08x}",
            "size": len(stage1),
            "crc32": pair_crc,
            "historical_seal_size": len(stage1),
            "app_file_offset": SHIM_SIZE,
            "app_flash_address": f"0x{APP_ADDRESS:08x}",
            "migration_pair_crc32": pair_crc,
            "next_artifact": stage2_path.name,
        })
        write_json(stage2_json, {
            **common,
            "artifact": stage2_path.name,
            "format": "meshcore-rp2040-dual-ota-stage2-seal-v1",
            "load_address": f"0x{SHIM_ADDRESS:08x}",
            "size": len(prefix),
            "crc32": f"0x{crc32(prefix):08x}",
            "seal_size": SHIM_SIZE,
            "seal_crc32": f"0x{crc32(prefix):08x}",
            "preserves_application_from": f"0x{APP_ADDRESS:08x}",
            "migration_pair_crc32": pair_crc,
            "previous_artifact": stage1_path.name,
        })

        # Remove names produced by the superseded one-pass/modified-ESP32
        # workflow so a stale build artifact cannot be selected by mistake.
        for obsolete in (
            stage1_path.parent / "firmware-esp32-installer.bin",
            stage1_path.parent / "firmware-esp32-installer.json",
        ):
            if obsolete.exists():
                obsolete.unlink()

        print(f"Dual OTA stage 1: {stage1_path} ({len(stage1)} bytes)")
        print(f"Dual OTA stage 2: {stage2_path} ({len(prefix)} bytes)")
        return 0
    elif mode == "lora":
        output_path = Path(str(target[0]))
        metadata_path = Path(str(target[1]))
        output_path.write_bytes(app)
        metadata = {
            **common,
            "artifact": output_path.name,
            "format": "meshcore-rp2040-dual-ota-app-v1",
            "load_address": f"0x{APP_ADDRESS:08x}",
            "size": len(app),
            "crc32": f"0x{crc32(app):08x}",
        }
    else:
        raise ValueError(f"unknown custom_dual_ota_artifact={mode!r}")

    write_json(metadata_path, metadata)
    print(f"Dual OTA artifact: {output_path} ({len(app)} bytes)")
    return 0


project_dir = Path(env.subst("$PROJECT_DIR"))
build_dir = Path(env.subst("$BUILD_DIR"))
program_name = env.subst("${PROGNAME}")
mode = env.GetProjectOption("custom_dual_ota_artifact")

app_binary = build_dir / f"{program_name}.bin"
shim_binary = project_dir / "variants" / "waveshare_rp2040_lora" / "dual_ota" / "dual_ota_shim.bin"
format_header = project_dir / "variants" / "waveshare_rp2040_lora" / "dual_ota" / "dual_ota_format.h"

if mode == "migration":
    artifact_targets = [
        build_dir / "firmware-esp32-stage1.bin",
        build_dir / "firmware-esp32-stage1.json",
        build_dir / "firmware-esp32-stage2-seal.bin",
        build_dir / "firmware-esp32-stage2-seal.json",
    ]
else:
    artifact_targets = [
        build_dir / "firmware-lora.bin",
        build_dir / "firmware-lora.json",
    ]

artifact = env.Command(
    [str(path) for path in artifact_targets],
    [str(app_binary), str(shim_binary), str(format_header)],
    build_artifact,
)
env.Alias("buildprog", artifact)
