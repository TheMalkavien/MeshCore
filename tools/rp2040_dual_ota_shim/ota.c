/*
 * Immutable OTA shim for MeshCore's Waveshare RP2040 UART + LoRa layout.
 * Derived from Arduino-Pico ota/ota.c (LGPL-2.1-or-later).
 *
 * The whole program is copied to SRAM before check_dual_ota() runs. This is
 * essential: flash erase/program stalls XIP on the RP2040.
 */

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "dual_ota_format.h"
#include "hardware/flash.h"
#include "hardware/gpio.h"
#include "hardware/structs/scb.h"
#include "hardware/structs/watchdog.h"
#include "hardware/sync.h"
#include "hardware/watchdog.h"
#include "pico/runtime_init.h"
#include "pico/time.h"

#include "ota_lfs.h"

#define UART_BOOTLOADER_ENTRY_PIN   22u
#define UART_BOOTLOADER_ENTRY_MAGIC 0xb105f00du
#define RP2040_SRAM_START           0x20000000u
#define RP2040_SRAM_END             0x20042000u

static const uint32_t *const fs_start_word =
    (const uint32_t *)(MLK_DUAL_OTA_APP_ADDRESS - 0x10u);
static const uint32_t *const fs_end_word =
    (const uint32_t *)(MLK_DUAL_OTA_APP_ADDRESS - 0x0cu);

static MLKDualOTACommand command __attribute__((section(".globals")));
static uint32_t command_block;

static uint32_t crc32_update(uint32_t crc, const void *source, uint32_t length) {
  const uint8_t *data = (const uint8_t *)source;
  for (uint32_t i = 0; i < length; ++i) {
    crc ^= data[i];
    for (uint32_t bit = 0; bit < 8; ++bit) {
      crc = (crc & 1u) ? ((crc >> 1) ^ 0xedb88320u) : (crc >> 1);
    }
  }
  return crc;
}

static uint32_t crc32(const void *source, uint32_t length) {
  return ~crc32_update(0xffffffffu, source, length);
}

static bool serial_header_finalized(void) {
  const MLKDualOTAUARTHeaderPrefix *header =
      (const MLKDualOTAUARTHeaderPrefix *)MLK_DUAL_OTA_UART_HEADER_ADDRESS;
  return header->vtor == MLK_DUAL_OTA_SHIM_ADDRESS &&
         header->size == MLK_DUAL_OTA_SHIM_SIZE &&
         header->crc32 ==
             crc32((const void *)MLK_DUAL_OTA_SHIM_ADDRESS,
                   MLK_DUAL_OTA_SHIM_SIZE);
}

static bool valid_app_vectors(const uint32_t vectors[2], uint32_t image_length) {
  uint32_t stack = vectors[0];
  uint32_t reset = vectors[1];
  uint32_t reset_address = reset & ~1u;
  uint32_t image_end = MLK_DUAL_OTA_APP_ADDRESS + image_length;

  if ((stack & 0x3u) != 0u || stack < RP2040_SRAM_START ||
      stack > RP2040_SRAM_END) {
    return false;
  }
  if ((reset & 1u) == 0u || reset_address < MLK_DUAL_OTA_APP_ADDRESS ||
      reset_address >= image_end || image_end > MLK_DUAL_OTA_FS_START) {
    return false;
  }
  return true;
}

static bool command_filename_ok(void) {
  static const char expected[] = MLK_DUAL_OTA_STAGED_FILE;
  if (sizeof(expected) > sizeof(command.filename)) {
    return false;
  }
  if (memcmp(command.filename, expected, sizeof(expected)) != 0) {
    return false;
  }
  return true;
}

static bool command_ok(void) {
  static const uint8_t magic[8] = MLK_DUAL_OTA_COMMAND_MAGIC_BYTES;
  if (memcmp(command.magic, magic, sizeof(magic)) != 0 ||
      command.version != MLK_DUAL_OTA_COMMAND_VERSION ||
      command.header_size != sizeof(command) ||
      command.target_address != MLK_DUAL_OTA_APP_ADDRESS ||
      command.file_offset != 0u || command.image_length < 8u ||
      command.image_length > MLK_DUAL_OTA_MAX_APP_SIZE ||
      !command_filename_ok()) {
    return false;
  }

  uint32_t expected_crc = crc32(&command,
                                offsetof(MLKDualOTACommand, command_crc32));
  return expected_crc == command.command_crc32;
}

static bool has_dual_ota(void) {
  // Stage 1 is bootable so that migration can be completed remotely, but its
  // UART header still covers shim+application. Ignore every staged LoRa
  // command until stage 2 has sealed exactly the immutable shim prefix.
  if (!serial_header_finalized()) {
    return false;
  }

  uint32_t fs_start = *fs_start_word;
  uint32_t fs_end = *fs_end_word;
  if (fs_start != MLK_DUAL_OTA_FS_START ||
      fs_end != MLK_DUAL_OTA_FS_END || fs_start >= fs_end) {
    return false;
  }
  if (!lfsMount((uint8_t *)fs_start, FLASH_SECTOR_SIZE, fs_end - fs_start)) {
    return false;
  }
  if (!lfsReadCommand(&command, &command_block)) {
    return false;
  }

  // The successful OTA path erases the command data block, while LittleFS
  // metadata can still describe the file until the application mounts the
  // filesystem again. Reject that erased/stale record here, before the rest
  // of the shim runtime is initialized and we hand control to the app.
  return command_ok();
}

static bool verify_staged_image(void) {
  if (!command_ok() || !lfsOpen(command.filename) ||
      !lfsSeek(command.file_offset)) {
    lfsClose();
    return false;
  }

  uint32_t remaining = command.image_length;
  uint32_t crc = 0xffffffffu;
  uint32_t vectors[2] = {0u, 0u};
  bool first = true;

  while (remaining != 0u) {
    uint32_t length = remaining < FLASH_SECTOR_SIZE ? remaining
                                                    : FLASH_SECTOR_SIZE;
    uint8_t *data = lfsRead(length);
    if (data == NULL) {
      lfsClose();
      return false;
    }
    if (first) {
      memcpy(vectors, data, sizeof(vectors));
      first = false;
    }
    crc = crc32_update(crc, data, length);
    remaining -= length;
  }
  lfsClose();

  return valid_app_vectors(vectors, command.image_length) &&
         (~crc == command.image_crc32);
}

static bool program_staged_image(void) {
  if (!lfsOpen(command.filename) || !lfsSeek(command.file_offset)) {
    lfsClose();
    return false;
  }

  uint32_t remaining = command.image_length;
  uint32_t destination = command.target_address;

  while (remaining != 0u) {
    uint32_t length = remaining < FLASH_SECTOR_SIZE ? remaining
                                                    : FLASH_SECTOR_SIZE;
    uint8_t *data = lfsRead(length);
    if (data == NULL) {
      lfsClose();
      return false;
    }
    if (length < FLASH_SECTOR_SIZE) {
      memset(data + length, 0xff, FLASH_SECTOR_SIZE - length);
    }

    if (memcmp(data, (const void *)destination, FLASH_SECTOR_SIZE) != 0) {
      uint32_t irq_state = save_and_disable_interrupts();
      flash_range_erase(destination - XIP_BASE, FLASH_SECTOR_SIZE);
      flash_range_program(destination - XIP_BASE, data, FLASH_SECTOR_SIZE);
      restore_interrupts(irq_state);
    }

    destination += FLASH_SECTOR_SIZE;
    remaining -= length;
  }
  lfsClose();

  const uint32_t *vectors = (const uint32_t *)MLK_DUAL_OTA_APP_ADDRESS;
  return valid_app_vectors(vectors, command.image_length) &&
         crc32(vectors, command.image_length) == command.image_crc32;
}

static void clear_bootloader_magic(void) {
  watchdog_hw->scratch[5] = 0u;
  watchdog_hw->scratch[6] = 0u;
}

__attribute__((noreturn)) static void enter_uart_recovery(void) {
  // Wake the companion ESP32 before asking the serial bootloader to remain
  // active. This mirrors MeshCore's "start esp32ota" pulse.
  gpio_init(UART_BOOTLOADER_ENTRY_PIN);
  gpio_set_dir(UART_BOOTLOADER_ENTRY_PIN, GPIO_OUT);
  gpio_put(UART_BOOTLOADER_ENTRY_PIN, 1);
  sleep_ms(500);
  gpio_put(UART_BOOTLOADER_ENTRY_PIN, 0);

  watchdog_hw->scratch[5] = UART_BOOTLOADER_ENTRY_MAGIC;
  watchdog_hw->scratch[6] = ~UART_BOOTLOADER_ENTRY_MAGIC;
  watchdog_reboot(0, 0, 10);
  while (true) {
    tight_loop_contents();
  }
}

#pragma GCC push_options
#pragma GCC optimize("O0")
__attribute__((noreturn)) static void boot_application(void) {
  const uint32_t *vectors = (const uint32_t *)MLK_DUAL_OTA_APP_ADDRESS;
  if (!valid_app_vectors(vectors,
                         MLK_DUAL_OTA_FS_START - MLK_DUAL_OTA_APP_ADDRESS)) {
    enter_uart_recovery();
  }

  clear_bootloader_magic();
  scb_hw->vtor = MLK_DUAL_OTA_APP_ADDRESS;

  register uint32_t *stack_pointer asm("sp");
  register uint32_t initial_stack = vectors[0];
  register void (*reset_handler)(void) = (void (*)(void))vectors[1];
  stack_pointer = (uint32_t *)initial_stack;
  reset_handler();
  (void)stack_pointer;
  __builtin_unreachable();
}
#pragma GCC pop_options

static void perform_dual_ota(void) {
  // The command is retained until both the source and programmed image pass
  // CRC/vector validation. A power cut during a sector write therefore causes
  // the immutable shim to retry on the next boot.
  if (!command_ok()) {
    return;
  }
  // A valid, atomically committed command proves that the running application
  // had completely verified the staged file before rebooting. If that source
  // can no longer be verified, or if a programmed sector cannot be read back,
  // never jump into a potentially half-written application.
  if (!verify_staged_image() || !program_staged_image()) {
    enter_uart_recovery();
  }

  lfsEraseBlock(command_block);
  clear_bootloader_magic();
  watchdog_reboot(0, 0, 100);
  while (true) {
    tight_loop_contents();
  }
}

static void check_dual_ota(void) {
  if (!has_dual_ota()) {
    boot_application();
  }
}
PICO_RUNTIME_INIT_FUNC_RUNTIME(check_dual_ota, "00099");

int main(int argc, char **argv) {
  (void)argc;
  (void)argv;
  perform_dual_ota();
  boot_application();
}

int __wrap_atexit(void (*function)(void)) {
  (void)function;
  return 0;
}

void __wrap_exit(int status) {
  (void)status;
  while (true) {
    tight_loop_contents();
  }
}

void __wrap_panic(const char *message) {
  (void)message;
  while (true) {
    tight_loop_contents();
  }
}

void __wrap_panic_unsupported(void) {
  while (true) {
    tight_loop_contents();
  }
}

void __wrap_hard_assertion_failure(void) {
  while (true) {
    tight_loop_contents();
  }
}
