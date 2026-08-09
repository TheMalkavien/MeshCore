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
    output_path = Path(str(target[0]))
    metadata_path = Path(str(target[1]))
    app = app_path.read_bytes()

    if len(app) > MAX_APP_SIZE:
        raise ValueError(
            f"application is 0x{len(app):x} bytes; max is 0x{MAX_APP_SIZE:x}"
        )
    app_stack, app_reset = validate_vectors(
        app, APP_ADDRESS, APP_ADDRESS + len(app), "application"
    )

    if mode == "installer":
        prefix = installer_prefix(shim_path.read_bytes(), app)
        artifact = prefix + app
        metadata = {
            "format": "meshcore-rp2040-dual-ota-installer-v1",
            "load_address": f"0x{SHIM_ADDRESS:08x}",
            "seal_crc32": f"0x{crc32(prefix):08x}",
            "seal_size": SHIM_SIZE,
            "app_file_offset": SHIM_SIZE,
            "app_flash_address": f"0x{APP_ADDRESS:08x}",
        }
    elif mode == "lora":
        artifact = app
        metadata = {
            "format": "meshcore-rp2040-dual-ota-app-v1",
            "load_address": f"0x{APP_ADDRESS:08x}",
        }
    else:
        raise ValueError(f"unknown custom_dual_ota_artifact={mode!r}")

    output_path.write_bytes(artifact)
    metadata.update(
        {
            "artifact": output_path.name,
            "size": len(artifact),
            "crc32": f"0x{crc32(artifact):08x}",
            "app_size": len(app),
            "app_crc32": f"0x{crc32(app):08x}",
            "app_initial_stack": f"0x{app_stack:08x}",
            "app_reset_vector": f"0x{app_reset:08x}",
        }
    )
    write_json(metadata_path, metadata)
    print(f"Dual OTA artifact: {output_path} ({len(artifact)} bytes)")
    return 0


project_dir = Path(env.subst("$PROJECT_DIR"))
build_dir = Path(env.subst("$BUILD_DIR"))
program_name = env.subst("${PROGNAME}")
mode = env.GetProjectOption("custom_dual_ota_artifact")
artifact_stem = "firmware-esp32-installer" if mode == "installer" else "firmware-lora"

app_binary = build_dir / f"{program_name}.bin"
shim_binary = project_dir / "variants" / "waveshare_rp2040_lora" / "dual_ota" / "dual_ota_shim.bin"
format_header = project_dir / "variants" / "waveshare_rp2040_lora" / "dual_ota" / "dual_ota_format.h"
artifact_binary = build_dir / f"{artifact_stem}.bin"
artifact_json = build_dir / f"{artifact_stem}.json"

artifact = env.Command(
    [str(artifact_binary), str(artifact_json)],
    [str(app_binary), str(shim_binary), str(format_header)],
    build_artifact,
)
env.Alias("buildprog", artifact)
