#pragma once

// Flash layout shared by the nRF52 mesh OTA receiver and its applier.
//
// The bounds themselves live in boards/nrf52840_s140_v6_lora_ota.ld so the
// linker refuses to build an app that overlaps the staging area; this header
// only imports the symbols it defines. Nothing here may hardcode an address.

#if defined(NRF52_PLATFORM) && defined(MESH_LORA_OTA)

#include <stdint.h>

extern "C" {
  extern uint32_t __ota_app_start[];      // start of the app region (0x26000)
  extern uint32_t __ota_app_limit[];      // first byte past the app region
  extern uint32_t __ota_stage_start[];    // start of the received gzip stream
  extern uint32_t __ota_stage_end[];      // first byte past it
  extern uint32_t __ota_record_page[];    // one page: the pending-update record
}

#define NRF52_OTA_PAGE_SIZE   4096

static inline uint32_t nrf52OtaAppStart()    { return (uint32_t) __ota_app_start; }
static inline uint32_t nrf52OtaAppLimit()    { return (uint32_t) __ota_app_limit; }
static inline uint32_t nrf52OtaAppSize()     { return nrf52OtaAppLimit() - nrf52OtaAppStart(); }
static inline uint32_t nrf52OtaStageStart()  { return (uint32_t) __ota_stage_start; }
static inline uint32_t nrf52OtaStageEnd()    { return (uint32_t) __ota_stage_end; }
static inline uint32_t nrf52OtaStageSize()   { return nrf52OtaStageEnd() - nrf52OtaStageStart(); }
static inline uint32_t nrf52OtaRecordPage()  { return (uint32_t) __ota_record_page; }

// Written to the record page once an image is fully received and verified.
// Read back at boot by the applier. Kept to one flash page so arming and
// disarming an update is a single erase.
#define NRF52_OTA_RECORD_MAGIC  0x3141544FUL   // 'OTA1' little-endian

// The MD5 is optional in the OTA protocol ('ota begin <size>' without one is
// legal), so whether md5[] means anything has to be recorded explicitly -
// otherwise the boot-time check would compare a real digest against 16 zero
// bytes and throw away a perfectly good image.
#define NRF52_OTA_FLAG_HAS_MD5  0x00000001UL

struct NRF52OTARecord {
  uint32_t magic;
  uint32_t stream_len;    // bytes of gzip stream parked at __ota_stage_start
  uint32_t image_size;    // decompressed size, from the gzip footer
  uint8_t  md5[16];       // MD5 of the gzip stream, as sent by the client
  uint32_t flags;
  uint32_t check;         // FNV-1a over the 32 bytes above
};

// FNV-1a: a table-free integrity check on the record itself, so a half-written
// or stale page can never be mistaken for an armed update.
static inline uint32_t nrf52OtaRecordCheck(const NRF52OTARecord *rec) {
  const uint8_t *p = (const uint8_t *) rec;
  uint32_t h = 2166136261UL;
  for (uint32_t i = 0; i < sizeof(NRF52OTARecord) - 4; i++) {
    h ^= p[i];
    h *= 16777619UL;
  }
  return h;
}

#endif
