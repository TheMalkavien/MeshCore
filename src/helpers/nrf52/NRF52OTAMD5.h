#pragma once

// Small MD5 for the nRF52 mesh OTA path. The ESP32 and RP2040 backends get MD5
// from their platform (MD5Builder / the Updater library); the Adafruit nRF52
// core ships none, and the OTA protocol's 'begin' carries an MD5 of the stream,
// so we bring our own.
//
// Used in two places: hashing the stream as it arrives, and re-hashing it out
// of flash at boot before the applier commits to erasing anything. Plain
// flash-resident code - it never runs while the app region is being rewritten.

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  uint32_t state[4];
  uint32_t count[2];    // message length in bits, low word first
  uint8_t  buffer[64];
} ota_md5_ctx;

void ota_md5_init(ota_md5_ctx *ctx);
void ota_md5_update(ota_md5_ctx *ctx, const uint8_t *data, size_t len);
void ota_md5_final(ota_md5_ctx *ctx, uint8_t digest[16]);

#ifdef __cplusplus
}
#endif
