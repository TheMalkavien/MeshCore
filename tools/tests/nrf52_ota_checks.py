#!/usr/bin/env python3
"""Offline checks for the nRF52 mesh OTA decompressor and hash.

There is no host C compiler in this tree, so the C decompressor cannot be unit
tested directly. This script does the next best thing, and covers the two ways
that code realistically goes wrong:

  1. the five DEFLATE tables are extracted *from the C source itself* and
     compared against the canonical RFC 1951 values;
  2. the decoding logic is transliterated to Python, including the exact sink
     semantics of the applier (4 KiB page buffer, first page held back,
     back-references read out of already-committed output), and run over a real
     gzipped MeshCore firmware, byte-comparing the result to the original.

Usage:
    python tools/tests/nrf52_ota_checks.py [firmware.hex|firmware.bin ...]

With no argument it looks for the RAK builds under .pio/build.
"""

import gzip
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
C_SRC = ROOT / "src" / "helpers" / "nrf52" / "NRF52OTAInflate.c"

PAGE = 4096

# Canonical values from RFC 1951 sections 3.2.5 and 3.2.7.
CANON = {
    "inf_lens": [3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43,
                 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258],
    "inf_lext": [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3,
                 4, 4, 4, 4, 5, 5, 5, 5, 0],
    "inf_dists": [1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257,
                  385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289,
                  16385, 24577],
    "inf_dext": [0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8,
                 9, 9, 10, 10, 11, 11, 12, 12, 13, 13],
    "inf_clen_order": [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15],
}


def extract_tables(text):
    tables = {}
    attrs = {}
    for name in CANON:
        m = re.search(r"static int16_t %s\[\d*\]([^=]*)=\s*\{(.*?)\};" % name, text, re.S)
        if not m:
            raise AssertionError("table %s not found in %s" % (name, C_SRC.name))
        attrs[name] = m.group(1)
        tables[name] = [int(v) for v in re.findall(r"-?\d+", m.group(2))]
    return tables, attrs


def check_tables():
    text = C_SRC.read_text()
    tables, attrs = extract_tables(text)
    for name, expected in CANON.items():
        got = tables[name]
        assert got == expected, "table %s differs from RFC 1951:\n  C: %s\n  ref: %s" % (
            name, got, expected)
        # Placement is the other half of correctness here: a table left in
        # .rodata reads back as 0xFF once the applier starts erasing.
        assert "OTA_INFLATE_RAMDATA" in attrs[name], \
            "table %s is not pinned into RAM with OTA_INFLATE_RAMDATA" % name
    assert not re.search(r"static\s+const\b", re.sub(r"//.*", "", text)), \
        "a const table would land in flash, which the applier erases"
    print("tables: 5/5 match RFC 1951, all pinned into RAM")


def check_md5_constants():
    """MD5's 64 rounds are pure transcription, so derive them and compare."""
    import math

    src = (ROOT / "src" / "helpers" / "nrf52" / "NRF52OTAMD5.c").read_text()
    body = src.split("uint32_t a = state[0]")[1]
    steps = re.findall(
        r"STEP\((\w),\s*(\w),\s*(\w),\s*(\w),\s*(\w),\s*x\[(\d+)\],\s*(0x[0-9a-fA-F]+),\s*(\d+)\)",
        body)
    assert len(steps) == 64, "expected 64 MD5 steps, found %d" % len(steps)

    T = [int(math.floor(abs(math.sin(i + 1)) * (2 ** 32))) for i in range(64)]
    S = ([7, 12, 17, 22] * 4) + ([5, 9, 14, 20] * 4) \
        + ([4, 11, 16, 23] * 4) + ([6, 10, 15, 21] * 4)
    FN = ["F"] * 16 + ["G"] * 16 + ["H"] * 16 + ["I"] * 16
    ROT = [("a", "b", "c", "d"), ("d", "a", "b", "c"),
           ("c", "d", "a", "b"), ("b", "c", "d", "a")]

    def g(i):
        if i < 16:
            return i
        if i < 32:
            return (5 * i + 1) % 16
        if i < 48:
            return (3 * i + 5) % 16
        return (7 * i) % 16

    for i, (fn, a, b, c, d, xi, const, shift) in enumerate(steps):
        assert fn == FN[i], "step %d uses %s, expected %s" % (i, fn, FN[i])
        assert (a, b, c, d) == ROT[i % 4], "step %d operand rotation is wrong" % i
        assert int(xi) == g(i), "step %d reads x[%s], expected x[%d]" % (i, xi, g(i))
        assert int(const, 16) == T[i], "step %d constant is %s, expected 0x%x" % (i, const, T[i])
        assert int(shift) == S[i], "step %d shift is %s, expected %d" % (i, shift, S[i])
    print("md5: 64 steps match the RFC 1321 derivation")


# --------------------------------------------------------------------------
# Transliteration of the C decoder. Kept deliberately close to the C, control
# flow included, so a divergence shows up as a diff rather than as a subtly
# different algorithm.
# --------------------------------------------------------------------------

MAXBITS = 15
MAXLCODES = 286
MAXDCODES = 30
MAXCODES = MAXLCODES + MAXDCODES
FIXLCODES = 288


class InflateError(Exception):
    pass


class PageSink:
    """Mirrors NRF52OTAApply.cpp: bytes go out through a single page buffer,
    page 0 is held back until the very end, and back-references are served
    from whichever of the three places currently holds the byte."""

    def __init__(self, limit):
        self.limit = limit
        self.page = bytearray(PAGE)
        self.first = bytearray(PAGE)
        self.first_valid = False
        self.page_base = 0
        self.pos = 0
        self.flash = bytearray()      # app region from offset PAGE on (page 0 is held back)

    def put(self, b):
        if self.pos >= self.limit:
            return -1
        self.page[self.pos - self.page_base] = b
        self.pos += 1
        if self.pos - self.page_base == PAGE:
            self._flush_page()
        return 0

    def _flush_page(self):
        if self.page_base == 0:
            self.first[:] = self.page
            self.first_valid = True
        else:
            assert len(self.flash) == self.page_base - PAGE
            self.flash += self.page
        self.page_base += PAGE
        self.page = bytearray(PAGE)

    def peek(self, pos):
        if pos >= self.page_base:
            return self.page[pos - self.page_base]
        if pos < PAGE:
            if not self.first_valid:
                raise InflateError("peek into unwritten first page")
            return self.first[pos]
        return self.flash[pos - PAGE]

    def finish(self):
        """Flush the tail page, then commit the held-back first page."""
        tail = self.pos - self.page_base
        if tail > 0:
            for i in range(tail, PAGE):
                self.page[i] = 0xFF
            self._flush_page()
        out = bytearray(self.first if self.first_valid else b"") + self.flash
        return bytes(out[:self.pos])


class Inflater:
    def __init__(self, data, sink):
        self.inp = data
        self.incnt = 0
        self.bitbuf = 0
        self.bitcnt = 0
        self.outcnt = 0
        self.sink = sink
        self.lencnt = [0] * (MAXBITS + 1)
        self.lensym = [0] * FIXLCODES
        self.distcnt = [0] * (MAXBITS + 1)
        self.distsym = [0] * MAXDCODES
        self.lengths = [0] * MAXCODES

    def bits(self, need):
        val = self.bitbuf
        while self.bitcnt < need:
            if self.incnt >= len(self.inp):
                raise InflateError("out of input")
            val |= self.inp[self.incnt] << self.bitcnt
            self.incnt += 1
            self.bitcnt += 8
        self.bitbuf = val >> need
        self.bitcnt -= need
        return val & ((1 << need) - 1)

    def emit(self, b):
        if self.sink.put(b) < 0:
            raise InflateError("sink refused byte at %d" % self.outcnt)
        self.outcnt += 1

    def decode(self, counts, symbols):
        code = first = index = 0
        for length in range(1, MAXBITS + 1):
            code |= self.bits(1)
            count = counts[length]
            if code - count < first:
                return symbols[index + (code - first)]
            index += count
            first += count
            first <<= 1
            code <<= 1
        raise InflateError("invalid huffman code")

    @staticmethod
    def construct(counts, symbols, lengths, n):
        for i in range(MAXBITS + 1):
            counts[i] = 0
        for symbol in range(n):
            counts[lengths[symbol]] += 1
        if counts[0] == n:
            return 0
        left = 1
        for length in range(1, MAXBITS + 1):
            left <<= 1
            left -= counts[length]
            if left < 0:
                return left
        offs = [0] * (MAXBITS + 1)
        for length in range(1, MAXBITS):
            offs[length + 1] = offs[length] + counts[length]
        for symbol in range(n):
            if lengths[symbol] != 0:
                symbols[offs[lengths[symbol]]] = symbol
                offs[lengths[symbol]] += 1
        return left

    def codes(self):
        lens = CANON["inf_lens"]
        lext = CANON["inf_lext"]
        dists = CANON["inf_dists"]
        dext = CANON["inf_dext"]
        while True:
            symbol = self.decode(self.lencnt, self.lensym)
            if symbol < 256:
                self.emit(symbol)
                continue
            if symbol == 256:
                return
            symbol -= 257
            if symbol >= 29:
                raise InflateError("invalid length code")
            length = lens[symbol] + self.bits(lext[symbol])
            symbol = self.decode(self.distcnt, self.distsym)
            if symbol >= 30:
                raise InflateError("invalid distance code")
            dist = dists[symbol] + self.bits(dext[symbol])
            if dist > self.outcnt:
                raise InflateError("distance before start of output")
            for _ in range(length):
                self.emit(self.sink.peek(self.outcnt - dist))

    def stored(self):
        self.bitbuf = 0
        self.bitcnt = 0
        if self.incnt + 4 > len(self.inp):
            raise InflateError("out of input")
        length = self.inp[self.incnt] | (self.inp[self.incnt + 1] << 8)
        nlen = self.inp[self.incnt + 2] | (self.inp[self.incnt + 3] << 8)
        if (~nlen) & 0xFFFF != length:
            raise InflateError("stored length mismatch")
        self.incnt += 4
        if self.incnt + length > len(self.inp):
            raise InflateError("out of input")
        for _ in range(length):
            self.emit(self.inp[self.incnt])
            self.incnt += 1

    def fixed(self):
        for symbol in range(144):
            self.lengths[symbol] = 8
        for symbol in range(144, 256):
            self.lengths[symbol] = 9
        for symbol in range(256, 280):
            self.lengths[symbol] = 7
        for symbol in range(280, FIXLCODES):
            self.lengths[symbol] = 8
        self.construct(self.lencnt, self.lensym, self.lengths, FIXLCODES)
        for symbol in range(MAXDCODES):
            self.lengths[symbol] = 5
        self.construct(self.distcnt, self.distsym, self.lengths, MAXDCODES)

    def dynamic(self):
        order = CANON["inf_clen_order"]
        nlen = self.bits(5) + 257
        ndist = self.bits(5) + 1
        ncode = self.bits(4) + 4
        if nlen > MAXLCODES or ndist > MAXDCODES:
            raise InflateError("too many codes")
        for index in range(ncode):
            self.lengths[order[index]] = self.bits(3)
        for index in range(ncode, 19):
            self.lengths[order[index]] = 0
        if self.construct(self.lencnt, self.lensym, self.lengths, 19) != 0:
            raise InflateError("incomplete code-length code")

        index = 0
        while index < nlen + ndist:
            symbol = self.decode(self.lencnt, self.lensym)
            if symbol < 16:
                self.lengths[index] = symbol
                index += 1
                continue
            length = 0
            if symbol == 16:
                if index == 0:
                    raise InflateError("repeat with no previous length")
                length = self.lengths[index - 1]
                symbol = 3 + self.bits(2)
            elif symbol == 17:
                symbol = 3 + self.bits(3)
            else:
                symbol = 11 + self.bits(7)
            if index + symbol > nlen + ndist:
                raise InflateError("too many lengths")
            for _ in range(symbol):
                self.lengths[index] = length
                index += 1

        if self.lengths[256] == 0:
            raise InflateError("no end-of-block code")
        err = self.construct(self.lencnt, self.lensym, self.lengths, nlen)
        if err and (err < 0 or nlen != self.lencnt[0] + self.lencnt[1]):
            raise InflateError("bad literal/length code")
        for i in range(ndist):
            self.lengths[i] = self.lengths[nlen + i]
        err = self.construct(self.distcnt, self.distsym, self.lengths, ndist)
        if err and (err < 0 or ndist != self.distcnt[0] + self.distcnt[1]):
            raise InflateError("bad distance code")

    def blocks(self):
        while True:
            last = self.bits(1)
            btype = self.bits(2)
            if btype == 0:
                self.stored()
            elif btype == 1:
                self.fixed()
                self.codes()
            elif btype == 2:
                self.dynamic()
                self.codes()
            else:
                raise InflateError("invalid block type")
            if last:
                return

    def gzip_header(self):
        data = self.inp
        if len(data) < 18 or data[0] != 0x1F or data[1] != 0x8B or data[2] != 8:
            raise InflateError("bad gzip header")
        flags = data[3]
        p = 10
        if flags & 0x04:
            p += 2 + (data[p] | (data[p + 1] << 8))
        if flags & 0x08:
            while p < len(data) and data[p] != 0:
                p += 1
            p += 1
        if flags & 0x10:
            while p < len(data) and data[p] != 0:
                p += 1
            p += 1
        if flags & 0x02:
            p += 2
        if p >= len(data):
            raise InflateError("bad gzip header")
        self.incnt = p


def inflate_gzip(data, limit):
    sink = PageSink(limit)
    inf = Inflater(data, sink)
    inf.gzip_header()
    inf.blocks()
    isize = int.from_bytes(data[-4:], "little")
    return sink.finish(), inf.outcnt, isize


# --------------------------------------------------------------------------


def hex_to_bin(path):
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


def read_layout():
    """Read the flash bounds from the linker script, not from a copy of them."""
    text = (ROOT / "boards" / "nrf52840_s140_v6_lora_ota.ld").read_text()
    values = {}
    for name in ("__ota_stage_start", "__ota_stage_end", "__ota_record_page"):
        m = re.search(r"%s\s*=\s*(0x[0-9a-fA-F]+)" % name, text)
        assert m, "%s missing from the linker script" % name
        values[name] = int(m.group(1), 16)
    m = re.search(r"FLASH \(rx\)\s*:\s*ORIGIN\s*=\s*(0x[0-9a-fA-F]+),\s*LENGTH\s*=\s*"
                  r"(0x[0-9a-fA-F]+)\s*-\s*(0x[0-9a-fA-F]+)", text)
    assert m, "FLASH region missing from the linker script"
    origin = int(m.group(1), 16)
    app_size = int(m.group(2), 16) - int(m.group(3), 16)
    assert int(m.group(3), 16) == origin, "FLASH LENGTH is not relative to ORIGIN"
    stage_size = values["__ota_stage_end"] - values["__ota_stage_start"]
    assert values["__ota_stage_start"] == origin + app_size, \
        "the staging area does not start where the app region ends"
    return origin, app_size, stage_size


def check_image(path, base, app_size, stage_size):
    if path.suffix == ".hex":
        start, raw = hex_to_bin(path)
    else:
        start, raw = base, path.read_bytes()
    packed = gzip.compress(raw, 9)
    out, outcnt, isize = inflate_gzip(packed, app_size)

    assert outcnt == len(raw), "emitted %d bytes, expected %d" % (outcnt, len(raw))
    assert isize == len(raw), "gzip ISIZE %d, expected %d" % (isize, len(raw))
    assert out == raw, "decompressed image differs from the original"
    assert start == base, "image starts at 0x%X, expected 0x%X" % (start, base)

    # Both halves of the split have to hold: the image must fit the shortened
    # app region, and its gzip form must fit the staging area it is parked in.
    assert len(raw) <= app_size, \
        "image is %d bytes, app region holds %d" % (len(raw), app_size)
    assert len(packed) <= stage_size, \
        "gzip image is %d bytes, staging area holds %d" % (len(packed), stage_size)

    label = "%s/%s" % (path.parent.name, path.name)
    print("%-46s raw %d/%d (%.0f%% of app), gz %d/%d (%.0f%% of staging) -> byte-identical"
          % (label, len(raw), app_size, 100.0 * len(raw) / app_size,
             len(packed), stage_size, 100.0 * len(packed) / stage_size))


def check_edge_cases(limit):
    # Stored blocks (level 0), tiny inputs, and an input whose length is an
    # exact multiple of the page size all exercise the sink boundaries.
    cases = {
        "empty-ish": b"\x00" * 3,
        "stored (level 0)": bytes(range(256)) * 40,
        "exact page multiple": bytes((i * 7) & 0xFF for i in range(PAGE * 3)),
        "one byte over a page": bytes((i * 13) & 0xFF for i in range(PAGE + 1)),
        "highly repetitive": b"MeshCore" * 5000,
    }
    for name, raw in cases.items():
        for level in (0, 6, 9):
            packed = gzip.compress(raw, level)
            out, outcnt, isize = inflate_gzip(packed, limit)
            assert out == raw and outcnt == len(raw) == isize, \
                "%s at level %d: mismatch" % (name, level)
    print("edge cases: %d inputs x 3 compression levels, all byte-identical"
          % len(cases))


def main():
    check_tables()
    check_md5_constants()

    base, app_size, stage_size = read_layout()
    print("layout: app 0x%X + %d bytes, staging %d bytes" % (base, app_size, stage_size))
    check_edge_cases(app_size)

    args = [Path(a) for a in sys.argv[1:]]
    if not args:
        args = sorted((ROOT / ".pio" / "build").glob("*lora_ota*/firmware.hex"))
    if not args:
        print("no OTA firmware image found; skipped the real-image check")
        return 0
    for path in args:
        check_image(path, base, app_size, stage_size)
    return 0


if __name__ == "__main__":
    sys.exit(main())
