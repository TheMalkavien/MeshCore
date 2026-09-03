#pragma once

// Boot-time applier for the nRF52 mesh OTA image (design A: no bootloader
// change, linker change only). See NRF52OTAApply.cpp for the full sequence.

#if defined(NRF52_PLATFORM) && defined(MESH_LORA_OTA)

#include <stddef.h>
#include <stdint.h>

// Call this as the very first thing at boot, before the SoftDevice is enabled
// and before any peripheral is brought up. If a verified image is parked in
// the staging area it is decompressed over the app region and the chip resets
// into it - this call does not return in that case.
//
// It returns normally (and the node keeps running its current firmware) when
// there is nothing to apply, or when the staged image failed verification.
void nrf52OtaApplyPending();

// Human-readable outcome of the last applyPending() attempt, for 'ota status'.
// "" when nothing was staged.
const char *nrf52OtaLastApplyResult();

// Decompress the staged image without writing anything, to prove it decodes
// and yields exactly the size its gzip footer claims. Run automatically before
// every apply; also reachable from the CLI as 'ota dryrun' so an image can be
// validated on the bench before the reboot that commits it.
// Returns true on success; on failure 'reply' holds a short cause.
bool nrf52OtaDryRun(char reply[], size_t reply_len);

#endif
