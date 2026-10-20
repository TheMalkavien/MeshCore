// DEFLATE/gzip decompressor for the nRF52 mesh OTA applier. See the header for
// why this exists instead of a library, and why nothing here is const.
//
// The Huffman decoding follows the canonical counted-code method of puff.c,
// the zlib reference decompressor: simple, table-free and small enough to
// audit line by line - which matters when a bug here bricks a node until
// someone walks up to it with a USB cable.

#include "NRF52OTAInflate.h"

// Every table below is pinned into .data.ramfunc, i.e. RAM. Merely dropping
// 'const' is not enough - GCC moves a never-written static array into .rodata
// anyway, and .rodata is in the app region that the applier erases out from
// under itself. tools/tests/check_ramfunc.py fails the build if one escapes.
static int16_t inf_lens[29] OTA_INFLATE_RAMDATA = {
  3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31,
  35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258
};
static int16_t inf_lext[29] OTA_INFLATE_RAMDATA = {
  0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2,
  3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0
};
static int16_t inf_dists[30] OTA_INFLATE_RAMDATA = {
  1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193,
  257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577
};
static int16_t inf_dext[30] OTA_INFLATE_RAMDATA = {
  0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6,
  7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13
};
static int16_t inf_clen_order[19] OTA_INFLATE_RAMDATA = {
  16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15
};

// Written through a volatile pointer so the compiler cannot rewrite the loop
// into a memset() call - that call would land in flash, which is gone.
OTA_INFLATE_RAMFUNC
static void inf_zero16(int16_t *p, int n) {
  volatile int16_t *v = p;
  while (n-- > 0) *v++ = 0;
}

OTA_INFLATE_RAMFUNC
static int inf_bits(ota_inflate_state *s, int need) {
  int32_t val = s->bitbuf;
  while (s->bitcnt < need) {
    if (s->incnt >= s->inlen) {
      s->err = OTA_INFLATE_ERR_INPUT;
      return 0;
    }
    val |= (int32_t) s->in[s->incnt++] << s->bitcnt;
    s->bitcnt += 8;
  }
  s->bitbuf = val >> need;
  s->bitcnt -= need;
  return (int)(val & ((1 << need) - 1));
}

OTA_INFLATE_RAMFUNC
static int inf_emit(ota_inflate_state *s, uint8_t b) {
  if (s->sink->put(s->sink->ctx, b) < 0) {
    s->err = OTA_INFLATE_ERR_SINK;
    return -1;
  }
  s->outcnt++;
  return 0;
}

// Decode one symbol using counts[] / symbols[] of a canonical Huffman code.
OTA_INFLATE_RAMFUNC
static int inf_decode(ota_inflate_state *s, int16_t *counts, int16_t *symbols) {
  int code = 0, first = 0, index = 0;
  for (int len = 1; len <= OTA_INFLATE_MAXBITS; len++) {
    code |= inf_bits(s, 1);
    if (s->err) return -1;
    int count = counts[len];
    if (code - count < first) return symbols[index + (code - first)];
    index += count;
    first += count;
    first <<= 1;
    code <<= 1;
  }
  s->err = OTA_INFLATE_ERR_CODE;
  return -1;
}

// Build counts[]/symbols[] from a code-length list. Returns 0 for a complete
// code, >0 for an incomplete one, <0 for an over-subscribed (invalid) one.
OTA_INFLATE_RAMFUNC
static int inf_construct(int16_t *counts, int16_t *symbols, int16_t *lengths, int n) {
  int16_t offs[OTA_INFLATE_MAXBITS + 1];

  inf_zero16(counts, OTA_INFLATE_MAXBITS + 1);
  for (int symbol = 0; symbol < n; symbol++) counts[lengths[symbol]]++;
  if (counts[0] == n) return 0;   // no codes at all: complete but unusable

  int left = 1;
  for (int len = 1; len <= OTA_INFLATE_MAXBITS; len++) {
    left <<= 1;
    left -= counts[len];
    if (left < 0) return left;    // over-subscribed
  }

  offs[1] = 0;
  for (int len = 1; len < OTA_INFLATE_MAXBITS; len++) {
    offs[len + 1] = (int16_t)(offs[len] + counts[len]);
  }
  for (int symbol = 0; symbol < n; symbol++) {
    if (lengths[symbol] != 0) symbols[offs[lengths[symbol]]++] = (int16_t) symbol;
  }
  return left;
}

// Decode one compressed block with the currently loaded length/distance codes.
OTA_INFLATE_RAMFUNC
static int inf_codes(ota_inflate_state *s) {
  for (;;) {
    int symbol = inf_decode(s, s->lencnt, s->lensym);
    if (s->err) return s->err;

    if (symbol < 256) {
      if (inf_emit(s, (uint8_t) symbol) < 0) return s->err;
      continue;
    }
    if (symbol == 256) return OTA_INFLATE_OK;   // end of block

    symbol -= 257;
    if (symbol >= 29) {
      s->err = OTA_INFLATE_ERR_CODE;
      return s->err;
    }
    int len = inf_lens[symbol] + inf_bits(s, inf_lext[symbol]);
    if (s->err) return s->err;

    symbol = inf_decode(s, s->distcnt, s->distsym);
    if (s->err) return s->err;
    if (symbol >= 30) {
      s->err = OTA_INFLATE_ERR_CODE;
      return s->err;
    }
    uint32_t dist = (uint32_t)(inf_dists[symbol] + inf_bits(s, inf_dext[symbol]));
    if (s->err) return s->err;
    if (dist > s->outcnt) {
      s->err = OTA_INFLATE_ERR_DISTANCE;
      return s->err;
    }

    // Copy through the sink: the source bytes may still be in the page buffer
    // or already committed to flash - peek() knows which.
    while (len-- > 0) {
      uint8_t b = s->sink->peek(s->sink->ctx, s->outcnt - dist);
      if (inf_emit(s, b) < 0) return s->err;
    }
  }
}

OTA_INFLATE_RAMFUNC
static int inf_stored(ota_inflate_state *s) {
  s->bitbuf = 0;
  s->bitcnt = 0;

  if (s->incnt + 4 > s->inlen) {
    s->err = OTA_INFLATE_ERR_INPUT;
    return s->err;
  }
  uint32_t len = (uint32_t) s->in[s->incnt] | ((uint32_t) s->in[s->incnt + 1] << 8);
  uint32_t nlen = (uint32_t) s->in[s->incnt + 2] | ((uint32_t) s->in[s->incnt + 3] << 8);
  if (((~nlen) & 0xFFFF) != len) {
    s->err = OTA_INFLATE_ERR_STORED;
    return s->err;
  }
  s->incnt += 4;

  if (s->incnt + len > s->inlen) {
    s->err = OTA_INFLATE_ERR_INPUT;
    return s->err;
  }
  while (len-- > 0) {
    if (inf_emit(s, s->in[s->incnt++]) < 0) return s->err;
  }
  return OTA_INFLATE_OK;
}

OTA_INFLATE_RAMFUNC
static int inf_fixed(ota_inflate_state *s) {
  int symbol = 0;
  for (; symbol < 144; symbol++) s->lengths[symbol] = 8;
  for (; symbol < 256; symbol++) s->lengths[symbol] = 9;
  for (; symbol < 280; symbol++) s->lengths[symbol] = 7;
  for (; symbol < OTA_INFLATE_FIXLCODES; symbol++) s->lengths[symbol] = 8;
  inf_construct(s->lencnt, s->lensym, s->lengths, OTA_INFLATE_FIXLCODES);

  for (symbol = 0; symbol < OTA_INFLATE_MAXDCODES; symbol++) s->lengths[symbol] = 5;
  inf_construct(s->distcnt, s->distsym, s->lengths, OTA_INFLATE_MAXDCODES);
  return OTA_INFLATE_OK;
}

OTA_INFLATE_RAMFUNC
static int inf_dynamic(ota_inflate_state *s) {
  int nlen = inf_bits(s, 5) + 257;
  int ndist = inf_bits(s, 5) + 1;
  int ncode = inf_bits(s, 4) + 4;
  if (s->err) return s->err;
  if (nlen > OTA_INFLATE_MAXLCODES || ndist > OTA_INFLATE_MAXDCODES) {
    s->err = OTA_INFLATE_ERR_LENGTHS;
    return s->err;
  }

  // Code-length code, itself Huffman-coded, in the fixed shuffled order.
  int index = 0;
  for (; index < ncode; index++) {
    s->lengths[inf_clen_order[index]] = (int16_t) inf_bits(s, 3);
    if (s->err) return s->err;
  }
  for (; index < 19; index++) s->lengths[inf_clen_order[index]] = 0;

  if (inf_construct(s->lencnt, s->lensym, s->lengths, 19) != 0) {
    s->err = OTA_INFLATE_ERR_LENGTHS;   // must be a complete code
    return s->err;
  }

  index = 0;
  while (index < nlen + ndist) {
    int symbol = inf_decode(s, s->lencnt, s->lensym);
    if (s->err) return s->err;

    if (symbol < 16) {
      s->lengths[index++] = (int16_t) symbol;
      continue;
    }

    int len = 0;
    if (symbol == 16) {
      if (index == 0) {
        s->err = OTA_INFLATE_ERR_LENGTHS;   // no previous length to repeat
        return s->err;
      }
      len = s->lengths[index - 1];
      symbol = 3 + inf_bits(s, 2);
    } else if (symbol == 17) {
      symbol = 3 + inf_bits(s, 3);
    } else {
      symbol = 11 + inf_bits(s, 7);
    }
    if (s->err) return s->err;
    if (index + symbol > nlen + ndist) {
      s->err = OTA_INFLATE_ERR_LENGTHS;
      return s->err;
    }
    while (symbol-- > 0) s->lengths[index++] = (int16_t) len;
  }

  if (s->lengths[256] == 0) {
    s->err = OTA_INFLATE_ERR_LENGTHS;   // no end-of-block code
    return s->err;
  }

  int err = inf_construct(s->lencnt, s->lensym, s->lengths, nlen);
  if (err && (err < 0 || nlen != s->lencnt[0] + s->lencnt[1])) {
    s->err = OTA_INFLATE_ERR_LENGTHS;
    return s->err;
  }
  // Distance lengths follow the literal/length lengths in the same array.
  for (int i = 0; i < ndist; i++) s->lengths[i] = s->lengths[nlen + i];
  err = inf_construct(s->distcnt, s->distsym, s->lengths, ndist);
  if (err && (err < 0 || ndist != s->distcnt[0] + s->distcnt[1])) {
    s->err = OTA_INFLATE_ERR_LENGTHS;
    return s->err;
  }
  return OTA_INFLATE_OK;
}

OTA_INFLATE_RAMFUNC
static int inf_blocks(ota_inflate_state *s) {
  int last;
  do {
    last = inf_bits(s, 1);
    int type = inf_bits(s, 2);
    if (s->err) return s->err;

    int rc;
    if (type == 0) {
      rc = inf_stored(s);
    } else if (type == 1) {
      rc = inf_fixed(s);
      if (rc == OTA_INFLATE_OK) rc = inf_codes(s);
    } else if (type == 2) {
      rc = inf_dynamic(s);
      if (rc == OTA_INFLATE_OK) rc = inf_codes(s);
    } else {
      s->err = OTA_INFLATE_ERR_BLOCK;
      rc = s->err;
    }
    if (rc != OTA_INFLATE_OK) return rc;
  } while (!last);
  return OTA_INFLATE_OK;
}

// Skip the gzip header, leaving incnt on the first deflate byte.
OTA_INFLATE_RAMFUNC
static int inf_gzip_header(ota_inflate_state *s) {
  if (s->inlen < 18) return OTA_INFLATE_ERR_GZIP;   // header + footer minimum
  if (s->in[0] != 0x1f || s->in[1] != 0x8b || s->in[2] != 8) return OTA_INFLATE_ERR_GZIP;

  uint8_t flags = s->in[3];
  uint32_t p = 10;

  if (flags & 0x04) {   // FEXTRA
    if (p + 2 > s->inlen) return OTA_INFLATE_ERR_GZIP;
    uint32_t xlen = (uint32_t) s->in[p] | ((uint32_t) s->in[p + 1] << 8);
    p += 2 + xlen;
  }
  if (flags & 0x08) {   // FNAME
    while (p < s->inlen && s->in[p] != 0) p++;
    p++;
  }
  if (flags & 0x10) {   // FCOMMENT
    while (p < s->inlen && s->in[p] != 0) p++;
    p++;
  }
  if (flags & 0x02) p += 2;   // FHCRC

  if (p >= s->inlen) return OTA_INFLATE_ERR_GZIP;
  s->incnt = p;
  return OTA_INFLATE_OK;
}

OTA_INFLATE_RAMFUNC
int ota_inflate_gzip(const uint8_t *in, uint32_t inlen, ota_inflate_sink *sink,
                     ota_inflate_state *st, uint32_t *out_len, uint32_t *isize) {
  st->in = in;
  st->inlen = inlen;
  st->incnt = 0;
  st->bitbuf = 0;
  st->bitcnt = 0;
  st->outcnt = 0;
  st->err = OTA_INFLATE_OK;
  st->sink = sink;

  int rc = inf_gzip_header(st);
  if (rc != OTA_INFLATE_OK) return rc;

  rc = inf_blocks(st);
  if (rc != OTA_INFLATE_OK) return rc;

  if (out_len) *out_len = st->outcnt;
  if (isize) {
    *isize = (uint32_t) in[inlen - 4]
           | ((uint32_t) in[inlen - 3] << 8)
           | ((uint32_t) in[inlen - 2] << 16)
           | ((uint32_t) in[inlen - 1] << 24);
  }
  return OTA_INFLATE_OK;
}

// Footer-only helper, used before the applier commits to anything. Not a
// RAMFUNC: it runs from flash, well before the first page is erased.
int ota_inflate_gzip_isize(const uint8_t *in, uint32_t inlen, uint32_t *isize) {
  if (inlen < 18 || in[0] != 0x1f || in[1] != 0x8b || in[2] != 8) return OTA_INFLATE_ERR_GZIP;
  *isize = (uint32_t) in[inlen - 4]
         | ((uint32_t) in[inlen - 3] << 8)
         | ((uint32_t) in[inlen - 2] << 16)
         | ((uint32_t) in[inlen - 1] << 24);
  return OTA_INFLATE_OK;
}
