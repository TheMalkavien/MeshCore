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


def validate_elf(
    nm_output: str, disassembly: str, data_section: bytes
) -> None:
    symbols: dict[str, list[tuple[int, str]]] = {}
    for line in nm_output.splitlines():
        fields = line.split()
        if len(fields) >= 3:
            try:
                address = int(fields[0], 16)
            except ValueError:
                continue
            symbols.setdefault(fields[-1], []).append((address, fields[-2]))

    runtime_init_symbols = symbols.get("runtime_init", [])
    if not any(symbol_type == "T" for _, symbol_type in runtime_init_symbols):
        rendered = ", ".join(item[1] for item in runtime_init_symbols) or "missing"
        raise RuntimeError(
            "runtime_init must be a strong global text symbol so that RP2040 "
            f"pre-init hooks execute; found: {rendered}"
        )
    required_symbols = (
        "runtime_run_initializers",
        "check_dual_ota",
        "__pre_init_check_dual_ota",
        "__preinit_array_start",
        "__preinit_array_end",
        "__data_start__",
        "boot_application",
        "flash_range_erase",
        "flash_range_program",
    )
    for required in required_symbols:
        if required not in symbols:
            raise RuntimeError(f"required early OTA symbol is missing: {required}")

    runtime_block = disassembly.split("<runtime_init>:", 1)
    if len(runtime_block) != 2 or "<runtime_run_initializers>" not in runtime_block[
        1
    ].split("\n\n", 1)[0]:
        raise RuntimeError("runtime_init does not call runtime_run_initializers")

    def address(name: str) -> int:
        return symbols[name][0][0]

    preinit_entry = address("__pre_init_check_dual_ota")
    preinit_start = address("__preinit_array_start")
    preinit_end = address("__preinit_array_end")
    if preinit_entry != preinit_start or preinit_entry + 4 > preinit_end:
        raise RuntimeError("dual OTA check is not the first RP2040 pre-init hook")

    data_offset = preinit_entry - address("__data_start__")
    if data_offset < 0 or data_offset + 4 > len(data_section):
        raise RuntimeError("dual OTA pre-init entry lies outside the data section")
    preinit_pointer = struct.unpack_from("<I", data_section, data_offset)[0]
    expected_pointer = address("check_dual_ota") | 1
    if preinit_pointer != expected_pointer:
        raise RuntimeError(
            "dual OTA pre-init entry does not point to check_dual_ota: "
            f"0x{preinit_pointer:08x} != 0x{expected_pointer:08x}"
        )

    for ram_function in (
        "boot_application",
        "flash_range_erase",
        "flash_range_program",
    ):
        function_address = address(ram_function)
        if not SRAM_START <= function_address < SRAM_END:
            raise RuntimeError(
                f"{ram_function} must execute from SRAM, found 0x{function_address:08x}"
            )


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
    objdump = tool(toolchain, "arm-none-eabi-objdump")
    nm = tool(toolchain, "arm-none-eabi-nm")
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
        (
            framework / "cores" / "rp2040" / "sdkoverride" / "newlib_interface.c",
            "newlib_interface.o",
            "gnu11",
        ),
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

    nm_result = subprocess.run(
        [nm, "-a", str(elf)], check=True, capture_output=True, text=True
    )
    objdump_result = subprocess.run(
        [objdump, "-d", str(elf)], check=True, capture_output=True, text=True
    )
    data_section = build_dir / "meshcore_dual_ota_shim.data.bin"
    subprocess.run(
        [objcopy, "-O", "binary", "--only-section=.data", str(elf), str(data_section)],
        check=True,
    )
    validate_elf(nm_result.stdout, objdump_result.stdout, data_section.read_bytes())

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
