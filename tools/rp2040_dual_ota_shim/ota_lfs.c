/*
 * Read-only LittleFS access for the MeshCore RP2040 dual OTA shim.
 * Derived from Arduino-Pico ota/ota_lfs.c (LGPL-2.1-or-later).
 */

#include "ota_lfs.h"

#include <string.h>

#include "hardware/flash.h"
#include "hardware/sync.h"
#include "lfs.h"

static lfs_t lfs;
static struct lfs_config lfs_cfg;
static uint8_t *flash_start;
static uint32_t flash_block_size;

static int flash_read(const struct lfs_config *cfg, lfs_block_t block,
                      lfs_off_t off, void *dst, lfs_size_t size) {
  (void)cfg;
  memcpy(dst, flash_start + (block * flash_block_size) + off, size);
  return 0;
}

static int flash_program(const struct lfs_config *cfg, lfs_block_t block,
                         lfs_off_t off, const void *buffer, lfs_size_t size) {
  (void)cfg;
  uint8_t *address = flash_start + (block * flash_block_size) + off;
  uint32_t irq_state = save_and_disable_interrupts();
  flash_range_program((uintptr_t)address - XIP_BASE, buffer, size);
  restore_interrupts(irq_state);
  return 0;
}

static int flash_erase(const struct lfs_config *cfg, lfs_block_t block) {
  (void)cfg;
  uint8_t *address = flash_start + (block * flash_block_size);
  uint32_t irq_state = save_and_disable_interrupts();
  flash_range_erase((uintptr_t)address - XIP_BASE, flash_block_size);
  restore_interrupts(irq_state);
  return 0;
}

static int flash_sync(const struct lfs_config *cfg) {
  (void)cfg;
  return 0;
}

static uint8_t read_buffer[256] __attribute__((section(".globals")));
static uint8_t program_buffer[256] __attribute__((section(".globals")));
static uint8_t lookahead_buffer[256] __attribute__((section(".globals")));

bool lfsMount(uint8_t *start, uint32_t block_size, uint32_t size) {
  flash_start = start;
  flash_block_size = block_size;

  memset(&lfs, 0, sizeof(lfs));
  memset(&lfs_cfg, 0, sizeof(lfs_cfg));
  lfs_cfg.read = flash_read;
  lfs_cfg.prog = flash_program;
  lfs_cfg.erase = flash_erase;
  lfs_cfg.sync = flash_sync;
  lfs_cfg.read_size = 256;
  lfs_cfg.prog_size = 256;
  lfs_cfg.block_size = block_size;
  lfs_cfg.block_count = block_size ? size / block_size : 0;
  lfs_cfg.block_cycles = 16;
  lfs_cfg.cache_size = 256;
  lfs_cfg.lookahead_size = 256;
  lfs_cfg.read_buffer = read_buffer;
  lfs_cfg.prog_buffer = program_buffer;
  lfs_cfg.lookahead_buffer = lookahead_buffer;
  lfs_cfg.name_max = 0;
  lfs_cfg.file_max = 0;
  lfs_cfg.attr_max = 0;
  return lfs_mount(&lfs, &lfs_cfg) >= 0;
}

static uint8_t command_buffer[256] __attribute__((section(".globals")));
static struct lfs_file_config command_cfg = {command_buffer, NULL, 0};

bool lfsReadCommand(MLKDualOTACommand *command) {
  lfs_file_t file;
  if (lfs_file_opencfg(&lfs, &file, MLK_DUAL_OTA_COMMAND_FILE,
                       LFS_O_RDONLY, &command_cfg) < 0) {
    return false;
  }

  lfs_ssize_t count = lfs_file_read(&lfs, &file, command, sizeof(*command));
  lfs_file_close(&lfs, &file);
  return count == (lfs_ssize_t)sizeof(*command);
}

static lfs_file_t firmware_file;
static bool firmware_open;
static uint8_t file_cache[256] __attribute__((section(".globals")));
static struct lfs_file_config file_cfg = {file_cache, NULL, 0};
static uint8_t flash_buffer[4096] __attribute__((section(".globals")));

bool lfsOpen(const char *filename) {
  if (firmware_open) {
    lfsClose();
  }
  if (lfs_file_opencfg(&lfs, &firmware_file, filename,
                       LFS_O_RDONLY, &file_cfg) < 0) {
    return false;
  }
  firmware_open = true;
  return true;
}

bool lfsSeek(uint32_t offset) {
  if (!firmware_open) {
    return false;
  }
  return lfs_file_seek(&lfs, &firmware_file, (lfs_soff_t)offset,
                       LFS_SEEK_SET) == (lfs_soff_t)offset;
}

uint8_t *lfsRead(uint32_t len) {
  if (!firmware_open || len > sizeof(flash_buffer)) {
    return NULL;
  }
  lfs_ssize_t count = lfs_file_read(&lfs, &firmware_file, flash_buffer, len);
  return count == (lfs_ssize_t)len ? flash_buffer : NULL;
}

void lfsClose(void) {
  if (!firmware_open) {
    return;
  }
  lfs_file_close(&lfs, &firmware_file);
  firmware_open = false;
}
