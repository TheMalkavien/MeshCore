// Boot-time applier for the nRF52 mesh OTA image.
//
// The nRF52 has neither a second app bank (ESP32) nor a core-provided OTA stub
// (arduino-pico), so the received image is parked, compressed, in a staging
// area carved out of flash by the linker script, and copied over the app
// region at the next boot. The copy cannot run from flash - it is erasing the
// flash it would be executing from - so everything between the first erase and
// the reset lives in .data.ramfunc, which the startup code copies to RAM.
//
// Sequence, in order of how much it costs to get wrong:
//
//   1. record page: magic + FNV check, else nothing happens at all
//   2. MD5 of the parked stream, re-hashed from flash (not trusted from RAM)
//   3. DRY RUN: the image is fully decompressed with a throwaway sink and must
//      yield exactly the byte count its gzip footer claims. This is the whole
//      safety argument of design A - a corrupt or undecodable image is thrown
//      away here, with the running firmware still intact. Only a power cut
//      during step 5 can still brick the node.
//   4. page 0 of the app region is erased and held back: from here on the
//      bootloader sees an invalid stack pointer, so any failure leaves the
//      node in USB/BLE DFU rather than boot-looping on a half-written image
//   5. decompress for real, page by page, into the app region
//   6. write the held-back page 0 (the app becomes valid), clear the record,
//      reset into the new firmware
//
// Interrupted between 6a and 6b, the node simply re-applies the same image on
// the next boot: the copy is idempotent.

#if defined(NRF52_PLATFORM) && defined(MESH_LORA_OTA)

#include "NRF52OTAApply.h"

#include "NRF52OTAInflate.h"
#include "NRF52OTALayout.h"
#include "NRF52OTAMD5.h"

#include <Arduino.h>
#include <nrf.h>
#include <nrf_sdm.h>
#include <stdlib.h>
#include <string.h>

// SoftDevice-aware flash helper from the core, used only before the point of no
// return; past it the applier drives the NVMC itself.
extern "C" bool flash_nrf5x_erase(uint32_t addr);

#define OTA_RAMFUNC __attribute__((section(".data.ramfunc"), noinline))

#define DRY_RUN_WINDOW  32768u   // max DEFLATE back-reference distance

static char _last_result[64] = "";

// ---------------------------------------------------------------------------
// Dry run: flash-resident, writes nothing. A plain 32KB ring is enough because
// no back-reference can reach further than that.
// ---------------------------------------------------------------------------

struct DryRunSink {
  uint8_t *window;
  uint32_t pos;
  uint32_t limit;
};

static int dryRunPut(void *ctx, uint8_t b) {
  DryRunSink *s = (DryRunSink *) ctx;
  if (s->pos >= s->limit) return -1;
  s->window[s->pos & (DRY_RUN_WINDOW - 1)] = b;
  s->pos++;
  return 0;
}

static uint8_t dryRunPeek(void *ctx, uint32_t pos) {
  DryRunSink *s = (DryRunSink *) ctx;
  return s->window[pos & (DRY_RUN_WINDOW - 1)];
}

// ---------------------------------------------------------------------------
// Real apply: everything below runs from RAM.
// ---------------------------------------------------------------------------

struct ApplySink {
  uint8_t *page;        // current output page
  uint8_t *first;       // page 0, held back until the very end
  uint32_t page_base;   // app-relative offset of page[0]
  uint32_t pos;         // total bytes emitted
  uint32_t app_start;
  uint32_t limit;
  int failed;
};

OTA_RAMFUNC
static void nvmcWait() {
  while (NRF_NVMC->READY == NVMC_READY_READY_Busy) {
    // The CPU stalls on flash access here anyway; this loop runs from RAM.
  }
}

OTA_RAMFUNC
static void nvmcErasePage(uint32_t addr) {
  NRF_NVMC->CONFIG = NVMC_CONFIG_WEN_Een;
  nvmcWait();
  NRF_NVMC->ERASEPAGE = addr;
  nvmcWait();
  NRF_NVMC->CONFIG = NVMC_CONFIG_WEN_Ren;
  nvmcWait();
}

// src must be word-aligned and hold exactly NRF52_OTA_PAGE_SIZE bytes.
OTA_RAMFUNC
static void nvmcWritePage(uint32_t addr, const uint8_t *src) {
  const uint32_t *words = (const uint32_t *) src;
  volatile uint32_t *dst = (volatile uint32_t *) addr;

  NRF_NVMC->CONFIG = NVMC_CONFIG_WEN_Wen;
  nvmcWait();
  for (uint32_t i = 0; i < NRF52_OTA_PAGE_SIZE / 4; i++) {
    dst[i] = words[i];
    nvmcWait();
  }
  NRF_NVMC->CONFIG = NVMC_CONFIG_WEN_Ren;
  nvmcWait();
}

// Commit the current page. Page 0 is copied aside instead: writing it last is
// what keeps a failed apply recoverable through the bootloader.
OTA_RAMFUNC
static void applyFlushPage(ApplySink *s) {
  if (s->page_base == 0) {
    // Byte loop through a volatile pointer: a memcpy() call would resolve to
    // libc in flash, which no longer exists at this point.
    volatile uint8_t *dst = s->first;
    const uint8_t *src = s->page;
    for (uint32_t i = 0; i < NRF52_OTA_PAGE_SIZE; i++) dst[i] = src[i];
  } else {
    uint32_t addr = s->app_start + s->page_base;
    nvmcErasePage(addr);
    nvmcWritePage(addr, s->page);
  }
  s->page_base += NRF52_OTA_PAGE_SIZE;
}

OTA_RAMFUNC
static int applyPut(void *ctx, uint8_t b) {
  ApplySink *s = (ApplySink *) ctx;
  if (s->pos >= s->limit) {
    s->failed = 1;
    return -1;
  }
  s->page[s->pos - s->page_base] = b;
  s->pos++;
  if (s->pos - s->page_base == NRF52_OTA_PAGE_SIZE) applyFlushPage(s);
  return 0;
}

// A back-reference can point into the page still being filled, into the
// held-back first page, or into flash that has already been committed.
OTA_RAMFUNC
static uint8_t applyPeek(void *ctx, uint32_t pos) {
  ApplySink *s = (ApplySink *) ctx;
  if (pos >= s->page_base) return s->page[pos - s->page_base];
  if (pos < NRF52_OTA_PAGE_SIZE) return s->first[pos];
  return *(volatile uint8_t *)(s->app_start + pos);
}

// Point of no return: erases and rewrites the app region, then resets. Never
// returns, whatever happens - there is no flash left to return to.
OTA_RAMFUNC
__attribute__((noreturn))
static void applyRun(const uint8_t *gz, uint32_t gz_len,
                     ota_inflate_state *st, ota_inflate_sink *sink,
                     ApplySink *s, uint32_t record_addr) {
  // Invalidate the app before touching anything else, so an interrupted apply
  // leaves the bootloader in DFU mode instead of jumping into rubble.
  nvmcErasePage(s->app_start);

  uint32_t out_len = 0;
  uint32_t isize = 0;
  int rc = ota_inflate_gzip(gz, gz_len, sink, st, &out_len, &isize);

  if (rc == OTA_INFLATE_OK && !s->failed && out_len == isize) {
    // Flush the tail page, padded like erased flash.
    uint32_t tail = s->pos - s->page_base;
    if (tail > 0) {
      volatile uint8_t *pad = s->page;
      for (uint32_t i = tail; i < NRF52_OTA_PAGE_SIZE; i++) pad[i] = 0xFF;
      applyFlushPage(s);
    }
    // The app becomes valid on this write.
    nvmcWritePage(s->app_start, s->first);
    // Disarm: the update is done, do not apply it again on the next boot.
    nvmcErasePage(record_addr);
  } else {
    // The dry run already proved this image decodes, so getting here means a
    // flash fault. Leave page 0 erased: DFU is the only honest outcome.
    nvmcErasePage(record_addr);
  }

  NVIC_SystemReset();
  while (1) { }
}

// ---------------------------------------------------------------------------

static bool readRecord(NRF52OTARecord *rec) {
  const NRF52OTARecord *flash_rec = (const NRF52OTARecord *) nrf52OtaRecordPage();
  *rec = *flash_rec;

  if (rec->magic != NRF52_OTA_RECORD_MAGIC) return false;
  if (rec->check != nrf52OtaRecordCheck(rec)) return false;
  if (rec->stream_len == 0 || rec->stream_len > nrf52OtaStageSize()) return false;
  if (rec->image_size == 0 || rec->image_size > nrf52OtaAppSize()) return false;
  return true;
}

static bool verifyStreamMD5(const NRF52OTARecord *rec) {
  if ((rec->flags & NRF52_OTA_FLAG_HAS_MD5) == 0) {
    // No MD5 was supplied for this transfer. The gzip stream still has to
    // decode and match its own footer, which the dry run checks.
    return true;
  }

  ota_md5_ctx md5;
  ota_md5_init(&md5);

  const uint8_t *p = (const uint8_t *) nrf52OtaStageStart();
  uint32_t left = rec->stream_len;
  while (left > 0) {
    uint32_t n = left > 4096 ? 4096 : left;
    ota_md5_update(&md5, p, n);
    p += n;
    left -= n;
  }

  uint8_t digest[16];
  ota_md5_final(&md5, digest);
  return memcmp(digest, rec->md5, 16) == 0;
}

// Shared by the boot path and the 'ota dryrun' command.
static bool runDryRun(const NRF52OTARecord *rec, char reply[], size_t reply_len) {
  uint8_t *window = (uint8_t *) malloc(DRY_RUN_WINDOW);
  ota_inflate_state *st = (ota_inflate_state *) malloc(sizeof(ota_inflate_state));
  if (window == NULL || st == NULL) {
    free(window);
    free(st);
    snprintf(reply, reply_len, "no RAM for dry run");
    return false;
  }

  DryRunSink dry = { window, 0, nrf52OtaAppSize() };
  ota_inflate_sink sink = { &dry, dryRunPut, dryRunPeek };

  uint32_t out_len = 0, isize = 0;
  int rc = ota_inflate_gzip((const uint8_t *) nrf52OtaStageStart(), rec->stream_len,
                            &sink, st, &out_len, &isize);
  free(window);
  free(st);

  if (rc != OTA_INFLATE_OK) {
    snprintf(reply, reply_len, "inflate failed (%d)", rc);
    return false;
  }
  if (out_len != isize) {
    snprintf(reply, reply_len, "size mismatch %lu/%lu",
             (unsigned long) out_len, (unsigned long) isize);
    return false;
  }
  if (out_len != rec->image_size) {
    snprintf(reply, reply_len, "record size mismatch %lu/%lu",
             (unsigned long) out_len, (unsigned long) rec->image_size);
    return false;
  }
  if (out_len > nrf52OtaAppSize()) {
    snprintf(reply, reply_len, "image too big (%lu)", (unsigned long) out_len);
    return false;
  }
  snprintf(reply, reply_len, "ok, %lu bytes", (unsigned long) out_len);
  return true;
}

bool nrf52OtaDryRun(char reply[], size_t reply_len) {
  NRF52OTARecord rec;
  if (!readRecord(&rec)) {
    snprintf(reply, reply_len, "no staged image");
    return false;
  }
  if (!verifyStreamMD5(&rec)) {
    snprintf(reply, reply_len, "MD5 mismatch");
    return false;
  }
  return runDryRun(&rec, reply, reply_len);
}

const char *nrf52OtaLastApplyResult() {
  return _last_result;
}

// Clear the record page so a rejected image is not retried on every boot.
static void disarmRecord() {
  // Safe to use the SoftDevice-aware path here: nothing has been erased yet.
  flash_nrf5x_erase(nrf52OtaRecordPage());
}

void nrf52OtaApplyPending() {
  NRF52OTARecord rec;
  if (!readRecord(&rec)) return;   // nothing armed: the common case, stay silent

  Serial.println("[OTA] staged image found, verifying...");

  char cause[48];
  if (!verifyStreamMD5(&rec)) {
    strcpy(cause, "MD5 mismatch");
  } else if (!runDryRun(&rec, cause, sizeof(cause))) {
    // cause already filled
  } else {
    Serial.printf("[OTA] verified (%lu -> %lu bytes), applying - do not power off\n",
                  (unsigned long) rec.stream_len, (unsigned long) rec.image_size);
    Serial.flush();

    uint8_t *page = (uint8_t *) malloc(NRF52_OTA_PAGE_SIZE);
    uint8_t *first = (uint8_t *) malloc(NRF52_OTA_PAGE_SIZE);
    ota_inflate_state *st = (ota_inflate_state *) malloc(sizeof(ota_inflate_state));
    if (page == NULL || first == NULL || st == NULL) {
      free(page);
      free(first);
      free(st);
      strcpy(cause, "no RAM for apply");
    } else {
      ApplySink s;
      s.page = page;
      s.first = first;
      s.page_base = 0;
      s.pos = 0;
      s.app_start = nrf52OtaAppStart();
      s.limit = nrf52OtaAppSize();
      s.failed = 0;

      ota_inflate_sink sink = { &s, applyPut, applyPeek };

      // From here on nothing may touch flash but us: no SoftDevice, no
      // interrupt handler (every vector points into the region being erased).
      sd_softdevice_disable();
      __disable_irq();

      applyRun((const uint8_t *) nrf52OtaStageStart(), rec.stream_len,
               st, &sink, &s, nrf52OtaRecordPage());
      // not reached
    }
  }

  snprintf(_last_result, sizeof(_last_result), "rejected: %s", cause);
  Serial.printf("[OTA] staged image %s\n", _last_result);
  disarmRecord();
}

#endif
