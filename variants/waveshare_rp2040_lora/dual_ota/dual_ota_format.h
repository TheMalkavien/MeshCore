#pragma once

#include <stddef.h>
#include <stdint.h>

// Flash layout used by the Waveshare RP2040 UART + LoRa dual OTA target.
// All addresses are absolute XIP addresses unless explicitly named as offsets.
#define MLK_DUAL_OTA_XIP_BASE             0x10000000u
#define MLK_DUAL_OTA_UART_HEADER_ADDRESS  0x10003000u
#define MLK_DUAL_OTA_SHIM_ADDRESS         0x10004000u
#define MLK_DUAL_OTA_APP_ADDRESS          0x10007000u
#define MLK_DUAL_OTA_SHIM_SIZE            0x00003000u
#define MLK_DUAL_OTA_APP_FILE_OFFSET      0x00003000u

#define MLK_DUAL_OTA_FS_START             0x1017f000u
#define MLK_DUAL_OTA_FS_END               0x101ff000u
#define MLK_DUAL_OTA_EEPROM_START         0x101ff000u
#define MLK_DUAL_OTA_FLASH_LENGTH         0x0017f000u
#define MLK_DUAL_OTA_MAX_APP_SIZE         (MLK_DUAL_OTA_FS_START - MLK_DUAL_OTA_APP_ADDRESS)

// The migration-pair manifest occupies the 64 bytes immediately before the
// four Arduino-Pico partition words. Both are part of the immutable 12 KiB
// shim prefix covered by the serial bootloader header. The historical ESP32
// flasher does not interpret this manifest; it identifies matching stage 1
// and stage 2 artifacts produced by the same build.
#define MLK_DUAL_OTA_INSTALL_MANIFEST_OFFSET 0x00002fb0u
#define MLK_DUAL_OTA_PARTITION_OFFSET        0x00002ff0u
#define MLK_DUAL_OTA_INSTALL_MANIFEST_VERSION 1u
#define MLK_DUAL_OTA_INSTALL_MAGIC_BYTES \
  { 'M', 'C', '2', 'I', 'N', 'S', '0', '1' }

typedef struct {
  uint8_t magic[8];
  uint32_t version;
  uint32_t header_size;
  uint32_t seal_size;
  uint32_t app_file_offset;
  uint32_t app_flash_address;
  uint32_t shim_flash_address;
  uint32_t fs_start;
  uint32_t fs_end;
  uint32_t shim_length;
  uint32_t shim_crc32;
  uint32_t app_length;
  uint32_t app_crc32;
  uint32_t reserved;
  uint32_t header_crc32;
} MLKDualOTAInstallManifest;

// First three words of rp2040-serial-bootloader's 256-byte image header at
// 0x10003000. During migration stage 1, size covers shim+application. Stage 2
// is final only when it seals exactly the immutable 12 KiB shim prefix.
typedef struct {
  uint32_t vtor;
  uint32_t size;
  uint32_t crc32;
} MLKDualOTAUARTHeaderPrefix;

// The custom LittleFS command is intentionally incompatible with PicoOTA's
// stock otacommand.bin. A stale command from the old layout can therefore
// never request an erase below the application boundary.
#define MLK_DUAL_OTA_COMMAND_VERSION 1u
#define MLK_DUAL_OTA_COMMAND_FILE    "mcota2.cmd"
#define MLK_DUAL_OTA_COMMAND_TEMP    "mcota2.tmp"
#define MLK_DUAL_OTA_STAGED_FILE     "firmware.bin"
#define MLK_DUAL_OTA_COMMAND_MAGIC_BYTES \
  { 'M', 'C', '2', 'O', 'T', 'A', '0', '1' }

typedef struct {
  uint8_t magic[8];
  uint32_t version;
  uint32_t header_size;
  uint32_t target_address;
  uint32_t file_offset;
  uint32_t image_length;
  uint32_t image_crc32;
  char filename[64];
  // Keep this record larger than LittleFS's 256-byte inline-file threshold.
  // The shim deliberately erases the command's dedicated data block after a
  // successful update, following Arduino-Pico's 656-byte OTACmdPage design.
  uint32_t reserved[139];
  uint32_t command_crc32;
} MLKDualOTACommand;

#if defined(__cplusplus)
static_assert(sizeof(MLKDualOTAInstallManifest) == 64,
              "dual OTA installer manifest ABI changed");
static_assert(offsetof(MLKDualOTAInstallManifest, header_crc32) == 60,
              "dual OTA installer CRC offset changed");
static_assert(sizeof(MLKDualOTAUARTHeaderPrefix) == 12,
              "UART bootloader header prefix ABI changed");
static_assert(sizeof(MLKDualOTACommand) == 656,
              "dual OTA command ABI changed");
static_assert(offsetof(MLKDualOTACommand, command_crc32) == 652,
              "dual OTA command CRC offset changed");
#else
_Static_assert(sizeof(MLKDualOTAInstallManifest) == 64,
               "dual OTA installer manifest ABI changed");
_Static_assert(offsetof(MLKDualOTAInstallManifest, header_crc32) == 60,
               "dual OTA installer CRC offset changed");
_Static_assert(sizeof(MLKDualOTAUARTHeaderPrefix) == 12,
               "UART bootloader header prefix ABI changed");
_Static_assert(sizeof(MLKDualOTACommand) == 656,
               "dual OTA command ABI changed");
_Static_assert(offsetof(MLKDualOTACommand, command_crc32) == 652,
               "dual OTA command CRC offset changed");
#endif
