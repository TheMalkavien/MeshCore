#!/usr/bin/env python3
"""Verify that the nRF52 OTA applier is genuinely self-contained in RAM.

The applier erases and rewrites the app region of flash while running. Anything
it reaches that still lives in flash - a called function, a jump veneer, a
lookup table, a string literal - reads back as 0xFF halfway through, and the
node lands in DFU with a half-written image. The compiler warns about none of
this: it will quietly move a never-written static array into .rodata (it did,
the first time this was built), and the linker will quietly route an
out-of-range call through a veneer placed in .text.

So the property is checked on the build output rather than assumed from the
source. Three passes, cheapest and most precise first:

  1. relocations: every reference made by code in .data.ramfunc must resolve to
     RAM, or to one of the staging-area symbols (flash the applier never erases)
  2. branches: no branch inside applier code may leave RAM
  3. placement: every applier symbol sits at a RAM address

Usage:
    python tools/tests/check_ramfunc.py [.pio/build/<env>/firmware.elf ...]
"""

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

TOOLCHAIN = Path(os.path.expanduser("~")) / ".platformio" / "packages" \
    / "toolchain-gccarmnoneeabi" / "bin"
NM = TOOLCHAIN / "arm-none-eabi-nm"
OBJDUMP = TOOLCHAIN / "arm-none-eabi-objdump"

RAM_LO, RAM_HI = 0x20000000, 0x20040000

# Addresses in flash that the applier may legitimately reference: the staging
# area holds the image it is reading, and the record page is what it clears at
# the end. Neither is inside the region being erased.
FLASH_ALLOWED = ("__ota_stage_start", "__ota_stage_end", "__ota_record_page")

# Symbols expected to be RAM-resident.
RAMFUNC_HINTS = (
    "inf_bits", "inf_emit", "inf_decode", "inf_construct", "inf_codes",
    "inf_stored", "inf_fixed", "inf_dynamic", "inf_blocks", "inf_zero16",
    "inf_gzip_header", "ota_inflate_gzip",
    "inf_lens", "inf_lext", "inf_dists", "inf_dext", "inf_clen_order",
    "nvmcWait", "nvmcErasePage", "nvmcWritePage",
    "applyFlushPage", "applyPut", "applyPeek", "applyRun",
)

# ota_inflate_gzip_isize deliberately stays in flash: it only reads a gzip
# footer, long before the first erase. Veneers live in flash by construction -
# they exist so flash code can reach RAM code, and pass 2 proves no RAM code
# routes through one.
FLASH_OK_SUFFIXES = ("_isize", "_veneer")

BRANCH = re.compile(
    r"^\s*([0-9a-f]+):\s+[0-9a-f ]+\s+(bl|blx|b|b\.n|b\.w|bl\.w)(?:\.\w)?\s+([0-9a-f]+)")
LABEL = re.compile(r"^([0-9a-f]{8}) <(.+)>:")
RELOC = re.compile(r"^[0-9a-f]{8}\s+(R_ARM_\w+)\s+(\S+)")


def run(cmd):
    return subprocess.run([str(c) for c in cmd], capture_output=True, text=True,
                          check=True).stdout


def symbols(elf):
    out = {}
    for line in run([NM, "-S", "--defined-only", elf]).splitlines():
        parts = line.split()
        if len(parts) == 4:
            out[parts[3]] = (int(parts[0], 16), int(parts[1], 16))
        elif len(parts) == 3:
            out[parts[2]] = (int(parts[0], 16), 0)
    return out


def check_relocations(build_dir, syms):
    """Pass 1: what does the RAM code actually reference?"""
    problems = []
    seen = 0
    objs = sorted((build_dir / "src" / "helpers" / "nrf52").glob("*.o"))
    for obj in objs:
        dump = run([OBJDUMP, "-r", obj])
        in_ramfunc = False
        for line in dump.splitlines():
            if line.startswith("RELOCATION RECORDS FOR"):
                in_ramfunc = ".data.ramfunc" in line
                continue
            if not in_ramfunc:
                continue
            m = RELOC.match(line)
            if not m:
                continue
            seen += 1
            target = m.group(2)
            if target.startswith((".data", ".bss")):
                continue                      # RAM by construction
            if target.startswith((".text", ".rodata")):
                problems.append("%s: RAM code references section %s"
                                % (obj.name, target))
                continue
            if target in FLASH_ALLOWED:
                continue
            addr = syms.get(target, (None, 0))[0]
            if addr is None:
                problems.append("%s: RAM code references unknown symbol %s"
                                % (obj.name, target))
            elif not (RAM_LO <= addr < RAM_HI):
                problems.append("%s: RAM code references %s at 0x%X (flash)"
                                % (obj.name, target, addr))
    return problems, seen, len(objs)


def check_branches_and_placement(elf, syms):
    """Passes 2 and 3."""
    problems = []
    ranges = []
    found = set()

    for name, (addr, size) in syms.items():
        if name.endswith(FLASH_OK_SUFFIXES):
            continue
        hit = next((h for h in RAMFUNC_HINTS if h in name), None)
        if hit is None:
            continue
        found.add(hit)
        if not (RAM_LO <= addr < RAM_HI):
            problems.append("%s is at 0x%X, not in RAM" % (name, addr))
        elif size:
            ranges.append((addr, addr + size, name))

    missing = [h for h in RAMFUNC_HINTS if h not in found]
    if missing:
        problems.append("symbols missing from the ELF (renamed? inlined away?): %s"
                        % ", ".join(missing))

    cur = None
    branches = 0
    for line in run([OBJDUMP, "-D", "-j", ".data", elf]).splitlines():
        m = LABEL.match(line)
        if m:
            addr = int(m.group(1), 16)
            cur = m.group(2) if any(lo <= addr < hi for lo, hi, _ in ranges) else None
            continue
        if cur is None:
            continue
        m = BRANCH.match(line)
        if m:
            branches += 1
            target = int(m.group(3), 16)
            if not (RAM_LO <= target < RAM_HI):
                problems.append("%s: %s to 0x%X leaves RAM"
                                % (cur, m.group(2), target))
    return problems, len(ranges), branches


def main():
    elves = [Path(a) for a in sys.argv[1:]]
    if not elves:
        elves = sorted((ROOT / ".pio" / "build").glob("*lora_ota*/firmware.elf"))
    if not elves:
        print("no *lora_ota* build found under .pio/build")
        return 1

    failed = False
    for elf in elves:
        syms = symbols(elf)
        if "__ota_app_start" not in syms:
            print("%s: not an OTA build (no __ota_app_start), skipped" % elf.parent.name)
            continue

        rel_problems, nrel, nobj = check_relocations(elf.parent, syms)
        br_problems, nsym, nbranch = check_branches_and_placement(elf, syms)
        problems = rel_problems + br_problems

        print("%s: app 0x%X..0x%X" % (elf.parent.name,
                                      syms["__ota_app_start"][0],
                                      syms["__ota_app_limit"][0]))
        print("   %d relocations from %d objects, %d symbols in RAM, %d branches"
              % (nrel, nobj, nsym, nbranch))
        for p in problems:
            print("   FAIL  %s" % p)
        if problems:
            failed = True
        else:
            print("   ok    the applier touches nothing in the erased region")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
