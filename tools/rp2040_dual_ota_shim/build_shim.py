#!/usr/bin/env python3
"""Build and validate the immutable RP2040 dual-OTA shim."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys


SHIM_ADDRESS = 0x10004000
MANIFEST_ADDRESS = 0x10006FB0
SRAM_START = 0x20000000
SRAM_END = 0x20042000


def tool(toolchain: Path, name: str) -> str:
    candidate = toolchain / "bin" / name
    if os.name == "nt":
        candidate = candidate.with_suffix(".exe")
    if not candidate.is_file():
        raise RuntimeError(f"unable to find tool: {candidate}")
    return str(candidate)


def validate(blob: bytes) -> None:
    if len(blob) < 8:
        raise RuntimeError("shim binary is too short")
    if len(blob) > MANIFEST_ADDRESS - SHIM_ADDRESS:
        raise RuntimeError(
            f"shim is 0x{len(blob):x} bytes; maximum before manifest is "
            f"0x{MANIFEST_ADDRESS - SHIM_ADDRESS:x}"
        )

    stack, reset = struct.unpack_from("<II", blob)
    reset_address = reset & ~1
    if stack < SRAM_START or stack > SRAM_END or stack & 3:
        raise RuntimeError(f"invalid shim initial stack pointer 0x{stack:08x}")
    if not reset & 1 or not SHIM_ADDRESS <= reset_address < SHIM_ADDRESS + len(blob):
        raise RuntimeError(f"invalid shim reset vector 0x{reset:08x}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="rebuild and verify that the committed blob is byte-for-byte current",
    )
    parser.add_argument("--framework", type=Path, help="framework-arduinopico package path")
    parser.add_argument("--toolchain", type=Path, help="RP2040 ARM toolchain path")
    args = parser.parse_args()

    source_dir = Path(__file__).resolve().parent
    project_dir = source_dir.parents[1]
    output = (
        project_dir
        / "variants"
        / "waveshare_rp2040_lora"
        / "dual_ota"
        / "dual_ota_shim.bin"
    )

    core_dir = Path(os.environ.get("PLATFORMIO_CORE_DIR", Path.home() / ".platformio"))
    packages = core_dir / "packages"
    framework = (args.framework or packages / "framework-arduinopico").resolve()
    toolchain = (args.toolchain or packages / "toolchain-rp2040-earlephilhower").resolve()
    if not (framework / "pico-sdk").is_dir():
        raise RuntimeError(f"invalid Arduino-Pico framework path: {framework}")
    if not toolchain.is_dir():
        raise RuntimeError(f"invalid RP2040 toolchain path: {toolchain}")

    build_dir = project_dir / ".pio" / "dual_ota_shim"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True)

    gcc = tool(toolchain, "arm-none-eabi-gcc")
    objcopy = tool(toolchain, "arm-none-eabi-objcopy")
    framework_arg = framework.as_posix() + "/"
    platform_inc = framework / "lib" / "rp2040" / "platform_inc.txt"
    core_inc = framework / "lib" / "core_inc.txt"
    platform_def = framework / "lib" / "rp2040" / "platform_def.txt"
    littlefs = framework / "libraries" / "LittleFS" / "lib" / "littlefs"

    definitions = [
        "PICO_COPY_TO_RAM=1",
        "PICO_FLASH_SIZE_BYTES=2097152",
        "PICO_XOSC_STARTUP_DELAY_MULTIPLIER=64",
        "PICO_PRINTF_SUPPORT_FLOAT=0",
        "PICO_PRINTF_SUPPORT_LONG_LONG=0",
        "PICO_RUNTIME_INIT_AEABI_BIT_OPS=00090",
        "PICO_RUNTIME_INIT_AEABI_MEM_OPS=00091",
        "LIB_PICO_PRINTF_NONE=1",
        "LFS_READONLY=1",
        "LFS_NO_DEBUG=1",
        "LFS_NO_WARN=1",
        "LFS_NO_ERROR=1",
        "LFS_NO_ASSERT=1",
        "LFS_NO_MALLOC=1",
        "PICO_PANIC_FUNCTION=",
        "PICO_TIME_DEFAULT_ALARM_POOL_DISABLED=1",
        "PICO_NO_BINARY_INFO=1",
    ]
    common = [
        gcc,
        "-mcpu=cortex-m0plus",
        "-mthumb",
        "-mfloat-abi=soft",
        "-Os",
        "-g",
        "-ffunction-sections",
        "-fdata-sections",
        "-fno-exceptions",
        "-Wall",
        "-Werror",
        f"-iprefix{framework_arg}",
        f"@{platform_inc}",
        f"@{core_inc}",
        f"@{platform_def}",
        f"-I{source_dir}",
        f"-I{project_dir / 'variants' / 'waveshare_rp2040_lora' / 'dual_ota'}",
        f"-I{littlefs}",
        f"-I{framework / 'pico-sdk' / 'src' / 'common' / 'boot_picobin_headers' / 'include'}",
        *[f"-D{item}" for item in definitions],
    ]

    sources = [
        (source_dir / "ota.c", "ota.o", "gnu11"),
        (source_dir / "ota_lfs.c", "ota_lfs.o", "gnu11"),
        (framework / "ota" / "ota_clocks.c", "ota_clocks.o", "gnu11"),
        (littlefs / "lfs.c", "lfs.o", "gnu11"),
        (littlefs / "lfs_util.c", "lfs_util.o", "gnu11"),
    ]
    objects: list[Path] = []
    for source, object_name, standard in sources:
        object_path = build_dir / object_name
        subprocess.run(
            [*common, f"-std={standard}", "-c", str(source), "-o", str(object_path)],
            check=True,
        )
        objects.append(object_path)

    crt0 = framework / "pico-sdk" / "src" / "rp2_common" / "pico_crt0" / "crt0.S"
    crt0_object = build_dir / "crt0.o"
    subprocess.run(
        [*common, "-x", "assembler-with-cpp", "-c", str(crt0), "-o", str(crt0_object)],
        check=True,
    )
    objects.insert(0, crt0_object)

    elf = build_dir / "meshcore_dual_ota_shim.elf"
    map_file = build_dir / "meshcore_dual_ota_shim.map"
    libpico = framework / "lib" / "rp2040" / "libpico.a"
    link = [
        gcc,
        "-mcpu=cortex-m0plus",
        "-mthumb",
        "-mfloat-abi=soft",
        "-nostartfiles",
        "-Wl,--build-id=none",
        "-Wl,--gc-sections",
        "-Wl,--no-warn-rwx-segments",
        f"-Wl,--script={source_dir / 'memmap_ota_rp2040.ld'}",
        f"-Wl,-Map={map_file}",
        "-Wl,--wrap=clocks_init",
        "-Wl,--wrap=exit",
        "-Wl,--wrap=atexit",
        "-Wl,--wrap=panic_unsupported",
        "-Wl,--wrap=panic",
        "-Wl,--wrap=hard_assertion_failure",
        "-o",
        str(elf),
        "-Wl,--start-group",
        *[str(item) for item in objects],
        str(libpico),
        "-lm",
        "-lc",
        "-lgcc",
        "-Wl,--end-group",
    ]
    subprocess.run(link, check=True)

    generated = build_dir / "meshcore_dual_ota_shim.bin"
    subprocess.run([objcopy, "-O", "binary", str(elf), str(generated)], check=True)
    blob = generated.read_bytes()
    validate(blob)
    digest = hashlib.sha256(blob).hexdigest()
    if args.check:
        if not output.is_file() or output.read_bytes() != blob:
            raise RuntimeError(
                f"committed shim is stale; run {Path(__file__).name} without --check"
            )
        print(f"OK {output} size=0x{len(blob):x} sha256={digest}")
        return 0

    output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(generated, output)
    print(f"Wrote {output} size=0x{len(blob):x} sha256={digest}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
