/*
 * RP2040 application linker script for the Waveshare dual OTA layout.
 *
 * 0x10000000..0x10002fff  UART bootloader
 * 0x10003000..0x10003fff  UART image header
 * 0x10004000..0x10006fff  immutable LoRa OTA shim
 * 0x10007000..             this application
 *
 * The Arduino-Pico builder always supplies boot2.o and ota.o. They are raw,
 * prelinked images for the standard layout and must never enter this binary.
 */

MEMORY
{
    FLASH(rx) : ORIGIN = 0x10007000, LENGTH = 0x1017f000 - 0x10007000
    RAM(rwx) : ORIGIN = 0x20000000, LENGTH = 256k
    SCRATCH_X(rwx) : ORIGIN = 0x20040000, LENGTH = 4k
    SCRATCH_Y(rwx) : ORIGIN = 0x20041000, LENGTH = 4k
}

PROVIDE(_EEPROM_start = 0x101ff000);
PROVIDE(_FS_start      = 0x1017f000);
PROVIDE(_FS_end        = 0x101ff000);

ENTRY(_entry_point)

SECTIONS
{
    .flash_begin : {
        __flash_binary_start = .;
    } > FLASH

    .text : {
        __logical_binary_start = .;
        KEEP(*(.vectors))
        KEEP(*(.binary_info_header))
        __binary_info_header_end = .;
        KEEP(*(.embedded_block))
        __embedded_block_end = .;
        KEEP(*(.reset))
        *(.init)
        *(EXCLUDE_FILE(*libgcc.a: *libc.a:*lib_a-mem*.o *libm.a: *interp.c.obj *divider.S.obj *PicoDVI.cpp.o *dvi.c.o) .text*)
        *(.fini)

        *crtbegin.o(.ctors)
        *crtbegin?.o(.ctors)
        *(EXCLUDE_FILE(*crtend?.o *crtend.o) .ctors)
        *(SORT(.ctors.*))
        *(.ctors)
        *crtbegin.o(.dtors)
        *crtbegin?.o(.dtors)
        *(EXCLUDE_FILE(*crtend?.o *crtend.o) .dtors)
        *(SORT(.dtors.*))
        *(.dtors)

        . = ALIGN(4);
        PROVIDE_HIDDEN(__preinit_array_start = .);
        KEEP(*(SORT(.preinit_array.*)))
        KEEP(*(.preinit_array))
        PROVIDE_HIDDEN(__preinit_array_end = .);

        . = ALIGN(4);
        PROVIDE_HIDDEN(__init_array_start = .);
        KEEP(*(SORT(.init_array.*)))
        KEEP(*(.init_array))
        PROVIDE_HIDDEN(__init_array_end = .);

        . = ALIGN(4);
        PROVIDE_HIDDEN(__fini_array_start = .);
        *(SORT(.fini_array.*))
        *(.fini_array)
        PROVIDE_HIDDEN(__fini_array_end = .);

        *(.eh_frame*)
        . = ALIGN(4);
    } > FLASH

    .rodata : {
        *(EXCLUDE_FILE(*libgcc.a: *libc.a:*lib_a-mem*.o *libm.a:) .rodata* .big_const*)
        . = ALIGN(4);
        *(SORT_BY_ALIGNMENT(SORT_BY_NAME(.flashdata*)))
        . = ALIGN(4);
    } > FLASH

    .ARM.extab : {
        *(.ARM.extab* .gnu.linkonce.armextab.*)
    } > FLASH

    __exidx_start = .;
    .ARM.exidx : {
        *(.ARM.exidx* .gnu.linkonce.armexidx.*)
    } > FLASH
    __exidx_end = .;

    . = ALIGN(4);

    .ram_vector_table (NOLOAD) : {
        *(.ram_vector_table)
    } > RAM

    .data : {
        __data_start__ = .;
        *(vtable)
        *(.time_critical*)
        *(.text*)
        . = ALIGN(4);
        *(.rodata*)
        . = ALIGN(4);
        *(.data*)
        . = ALIGN(4);
        *(.after_data.*)
        . = ALIGN(4);
        PROVIDE_HIDDEN(__mutex_array_start = .);
        KEEP(*(SORT(.mutex_array.*)))
        KEEP(*(.mutex_array))
        PROVIDE_HIDDEN(__mutex_array_end = .);
        . = ALIGN(4);
        *(.jcr)
        . = ALIGN(4);
    } > RAM AT> FLASH

    .tdata : {
        . = ALIGN(4);
        *(.tdata .tdata.* .gnu.linkonce.td.*)
        __tdata_end = .;
    } > RAM AT> FLASH
    PROVIDE(__data_end__ = .);

    __etext = LOADADDR(.data);

    .tbss (NOLOAD) : {
        . = ALIGN(4);
        __bss_start__ = .;
        __tls_base = .;
        *(.tbss .tbss.* .gnu.linkonce.tb.*)
        *(.tcommon)
        __tls_end = .;
    } > RAM

    .bss (NOLOAD) : {
        . = ALIGN(4);
        __tbss_end = .;
        *(SORT_BY_ALIGNMENT(SORT_BY_NAME(.bss*)))
        *(COMMON)
        . = ALIGN(4);
        __bss_end__ = .;
    } > RAM

    .uninitialized_ram MAX(0x20003000, .) (NOLOAD) : {
        *(.uninitialized_ram*)
    } > RAM

    .uninitialized_data MAX(0x20003000, .) (NOLOAD) : {
        *(.uninitialized_data*)
    } > RAM

    .heap (NOLOAD) : {
        __end__ = .;
        end = __end__;
        KEEP(*(.heap*))
        . = ORIGIN(RAM) + LENGTH(RAM);
        __HeapLimit = .;
    } > RAM

    .scratch_x : {
        __scratch_x_start__ = .;
        *(.scratch_x.*)
        . = ALIGN(4);
        __scratch_x_end__ = .;
    } > SCRATCH_X AT> FLASH
    __scratch_x_source__ = LOADADDR(.scratch_x);

    .scratch_y : {
        __scratch_y_start__ = .;
        *(.scratch_y.*)
        . = ALIGN(4);
        __scratch_y_end__ = .;
    } > SCRATCH_Y AT> FLASH
    __scratch_y_source__ = LOADADDR(.scratch_y);

    .stack1_dummy (NOLOAD) : {
        *(.stack1*)
    } > SCRATCH_X

    .stack_dummy (NOLOAD) : {
        KEEP(*(.stack*))
    } > SCRATCH_Y

    .flash_end : {
        KEEP(*(.embedded_end_block*))
        PROVIDE(__flash_binary_end = .);
    } > FLASH

    /DISCARD/ : {
        *(.boot2)
        *(.OTA)
    }

    __StackLimit = ORIGIN(RAM) + LENGTH(RAM);
    __StackOneTop = ORIGIN(SCRATCH_X) + LENGTH(SCRATCH_X);
    __StackTop = ORIGIN(SCRATCH_Y) + LENGTH(SCRATCH_Y);
    __StackOneBottom = __StackOneTop - SIZEOF(.stack1_dummy);
    __StackBottom = __StackTop - SIZEOF(.stack_dummy);
    PROVIDE(__stack = __StackTop);

    PROVIDE(__heap_start = __end__);
    PROVIDE(__heap_end = __HeapLimit);
    PROVIDE(__tls_align = MAX(ALIGNOF(.tdata), ALIGNOF(.tbss)));
    PROVIDE(__tls_size_align = (__tls_size + __tls_align - 1) & ~(__tls_align - 1));
    PROVIDE(__arm32_tls_tcb_offset = MAX(8, __tls_align));
    PROVIDE(_end = __end__);
    PROVIDE(__llvm_libc_heap_limit = __HeapLimit);

    ASSERT(__StackLimit >= __HeapLimit, "region RAM overflowed")
    ASSERT(__binary_info_header_end - __logical_binary_start <= 256,
           "Binary info must be in first 256 bytes of the binary")
    ASSERT(__logical_binary_start == 0x10007000,
           "dual OTA application vectors moved from 0x10007000")
    ASSERT(__flash_binary_end <= 0x1017f000,
           "dual OTA application overlaps LittleFS")
}
