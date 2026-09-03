/* Linker script for nRF52840 targets that carry mesh OTA over LoRa.

   Same as nrf52840_s140_v6.ld, except the app region is shortened to reserve
   a staging area for the OTA image. Design A (see NRF52OTA.cpp): the running
   app cannot rewrite the flash it executes from, so the incoming firmware is
   parked in the staging area and copied over the app region at the next boot
   by a RAM-resident applier.

   Layout (nRF52840, S140 v6/v7, Adafruit/RAK bootloader):

     0x00000..0x26000  MBR + SoftDevice        (untouched)
     0x26000..0x9A000  app                     475136 B - rewritten by the applier
     0x9A000..0xEC000  OTA staging (gzip)      335872 B - holds the received stream
     0xEC000..0xED000  OTA pending record      one page - magic/size/MD5
     0xED000..0xF4000  UserData LittleFS       (untouched, InternalFS)
     0xF4000..         bootloader              (untouched, UF2/DFU recovery)

   The split is balanced so that an app filling its region still compresses
   into the staging area: MeshCore nRF52 images gzip to about 68% of their raw
   size, and 475136 * 0.68 = 323k < 335872. Grow one bound and you must shrink
   the other.  */

SEARCH_DIR(.)
GROUP(-lgcc -lc -lnosys)

MEMORY
{
  FLASH (rx)     : ORIGIN = 0x26000, LENGTH = 0x9A000 - 0x26000

  /* SRAM required by Softdevice depend on
   * - Attribute Table Size (Number of Services and Characteristics)
   * - Vendor UUID count
   * - Max ATT MTU
   * - Concurrent connection peripheral + central + secure links
   * - Event Len, HVN queue, Write CMD queue
   */
  RAM (rwx) :  ORIGIN = 0x20006000, LENGTH = 0x20040000 - 0x20006000
}

/* Consumed by NRF52OTA.cpp: keeping the bounds here means the code and the
   linker can never disagree about where staging lives. */
__ota_app_start    = ORIGIN(FLASH);
__ota_app_limit    = ORIGIN(FLASH) + LENGTH(FLASH);
__ota_stage_start  = 0x9A000;
__ota_stage_end    = 0xEC000;
__ota_record_page  = 0xEC000;

ASSERT(__ota_stage_start >= __ota_app_limit, "OTA staging area overlaps the app region")

SECTIONS
{
  . = ALIGN(4);
  .svc_data :
  {
    PROVIDE(__start_svc_data = .);
    KEEP(*(.svc_data))
    PROVIDE(__stop_svc_data = .);
  } > RAM

  .fs_data :
  {
    PROVIDE(__start_fs_data = .);
    KEEP(*(.fs_data))
    PROVIDE(__stop_fs_data = .);
  } > RAM
} INSERT AFTER .data;

INCLUDE "nrf52_common.ld"
