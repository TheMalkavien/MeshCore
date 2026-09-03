#pragma once

// Minimal streaming DEFLATE/gzip decompressor for the nRF52 mesh OTA applier.
//
// Why not a library: this code runs from RAM while the app region of flash is
// being erased and rewritten (see NRF52OTAApply.cpp), so it must not touch a
// single byte of .text or .rodata. That rules out miniz/uzlib as-is and it
// rules out const lookup tables — every table below is deliberately non-const
// so the linker puts it in .data, which the startup code copies to RAM.
//
// The output is never held in RAM: bytes leave through the sink one at a time,
// and LZ77 back-references are resolved by reading them back through the sink
// (from the current page buffer, or straight out of the flash already written).
// That is what keeps the applier's RAM footprint at a couple of pages instead
// of the 32KB window a conventional inflater needs.

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Placed in .data (i.e. RAM) on the target; a no-op on the host test build.
#ifndef OTA_INFLATE_RAMFUNC
  #if defined(NRF52_PLATFORM)
    #define OTA_INFLATE_RAMFUNC __attribute__((section(".data.ramfunc"), noinline))
  #else
    #define OTA_INFLATE_RAMFUNC
  #endif
#endif

// Same, for the lookup tables - a separate section name because GCC refuses to
// mix code and data in one. Dropping 'const' is not enough on its own: GCC
// happily moves a static array it never sees written into .rodata, which is in
// the flash the applier erases. Verified by tools/tests/check_ramfunc.py.
#ifndef OTA_INFLATE_RAMDATA
  #if defined(NRF52_PLATFORM)
    #define OTA_INFLATE_RAMDATA __attribute__((section(".data.ota_tables")))
  #else
    #define OTA_INFLATE_RAMDATA
  #endif
#endif

enum {
  OTA_INFLATE_OK            =  0,
  OTA_INFLATE_ERR_INPUT     = -1,   // ran out of compressed input
  OTA_INFLATE_ERR_CODE      = -2,   // invalid Huffman code
  OTA_INFLATE_ERR_BLOCK     = -3,   // invalid block type
  OTA_INFLATE_ERR_STORED    = -4,   // stored block length mismatch
  OTA_INFLATE_ERR_LENGTHS   = -5,   // invalid code length set
  OTA_INFLATE_ERR_DISTANCE  = -6,   // back-reference before start of output
  OTA_INFLATE_ERR_SINK      = -7,   // sink refused a byte (output full / flash error)
  OTA_INFLATE_ERR_GZIP      = -8    // bad gzip header
};

#define OTA_INFLATE_MAXBITS   15
#define OTA_INFLATE_MAXLCODES 286
#define OTA_INFLATE_MAXDCODES 30
#define OTA_INFLATE_MAXCODES  (OTA_INFLATE_MAXLCODES + OTA_INFLATE_MAXDCODES)
#define OTA_INFLATE_FIXLCODES 288

typedef struct {
  void *ctx;
  // Emit the next output byte. Returns 0 on success, negative to abort.
  int (*put)(void *ctx, uint8_t byte);
  // Read back an already-emitted byte at absolute output offset 'pos'.
  uint8_t (*peek)(void *ctx, uint32_t pos);
} ota_inflate_sink;

// Work area. Caller-allocated so nothing large lands on the stack.
typedef struct {
  const uint8_t *in;
  uint32_t inlen;
  uint32_t incnt;
  int32_t  bitbuf;
  int      bitcnt;
  uint32_t outcnt;
  int      err;
  ota_inflate_sink *sink;

  int16_t lencnt[OTA_INFLATE_MAXBITS + 1];
  int16_t lensym[OTA_INFLATE_FIXLCODES];
  int16_t distcnt[OTA_INFLATE_MAXBITS + 1];
  int16_t distsym[OTA_INFLATE_MAXDCODES];
  int16_t lengths[OTA_INFLATE_MAXCODES];
} ota_inflate_state;

// Inflate a complete gzip stream. On success *out_len holds the number of
// bytes emitted and the gzip footer's ISIZE field is returned in *isize.
// Returns OTA_INFLATE_OK or a negative error code.
OTA_INFLATE_RAMFUNC
int ota_inflate_gzip(const uint8_t *in, uint32_t inlen, ota_inflate_sink *sink,
                     ota_inflate_state *st, uint32_t *out_len, uint32_t *isize);

// Read the decompressed size from a gzip footer without decompressing.
// Returns 0 on success. Safe to call from flash-resident code.
int ota_inflate_gzip_isize(const uint8_t *in, uint32_t inlen, uint32_t *isize);

#ifdef __cplusplus
}
#endif
