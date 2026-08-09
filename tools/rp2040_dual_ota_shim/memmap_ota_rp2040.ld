/*
 * Arduino-Pico OTA RAM-copy linker script, relocated for MeshCore's UART
 * bootloader layout. Derived from Arduino-Pico memmap_ota_rp2040.ld.
 */

MEMORY
{
    FLASH(rx) :      ORIGIN = 0x10004000, LENGTH = 0x2fb0
    RAM(rwx) :        ORIGIN = 0x20000000, LENGTH = 212k
    GLOBALS(rwx) :    ORIGIN = 0x20035000, LENGTH = 44k
    SCRATCH_X(rwx) :  ORIGIN = 0x20040000, LENGTH = 4k
    SCRATCH_Y(rwx) :  ORIGIN = 0x20041000, LENGTH = 4k
}

ENTRY(_entry_point)

SECTIONS
{
    .globals (NOLOAD) : {
        *(.globals*)
    } > GLOBALS

    .flash_begin : {
        __flash_binary_start = .;
    } > FLASH

    .flashtext : {
        __logical_binary_start = .;
        KEEP(*(.vectors))
        KEEP(*(.binary_info_header))
        __binary_info_header_end = .;
        KEEP(*(.reset))
    } > FLASH

    .rodata : {
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

    .ram_vector_table (COPY) : {
        *(.ram_vector_table)
    } > RAM

    .text : {
        __ram_text_start__ = .;
        *(.init)
        *(.text*)
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
        *(.eh_frame*)
        . = ALIGN(4);
        __ram_text_end__ = .;
    } > RAM AT> FLASH
    __ram_text_source__ = LOADADDR(.text);

    .data : {
        __data_start__ = .;
        *(vtable)
        *(.time_critical*)
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
        *(.jcr)
        . = ALIGN(4);
        __data_end__ = .;
    } > RAM AT> FLASH
    __etext = LOADADDR(.data);

    .uninitialized_data (COPY) : {
        . = ALIGN(4);
        *(.uninitialized_data*)
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

    .bss : {
        . = ALIGN(4);
        __bss_start__ = .;
        *(SORT_BY_ALIGNMENT(SORT_BY_NAME(.bss*)))
        *(COMMON)
        . = ALIGN(4);
        __bss_end__ = .;
    } > RAM

    .heap (COPY) : {
        __end__ = .;
        end = __end__;
        *(.heap*)
        __HeapLimit = .;
    } > RAM

    .stack1_dummy (COPY) : {
        *(.stack1*)
    } > SCRATCH_X

    .stack_dummy (COPY) : {
        KEEP(*(.stack*))
    } > SCRATCH_Y

    .flash_end : {
        __flash_binary_end = .;
    } > FLASH

    __StackLimit = ORIGIN(RAM) + LENGTH(RAM);
    __StackOneTop = ORIGIN(SCRATCH_X) + LENGTH(SCRATCH_X);
    __StackTop = ORIGIN(SCRATCH_Y) + LENGTH(SCRATCH_Y);
    __StackOneBottom = __StackOneTop - SIZEOF(.stack1_dummy);
    __StackBottom = __StackTop - SIZEOF(.stack_dummy);
    PROVIDE(__stack = __StackTop);

    ASSERT(__StackLimit >= __HeapLimit, "region RAM overflowed")
    ASSERT(__binary_info_header_end - __logical_binary_start <= 256,
           "Binary info must be in first 256 bytes of the binary")
    ASSERT(__logical_binary_start == 0x10004000,
           "dual OTA shim vectors moved from 0x10004000")
    ASSERT(__flash_binary_end <= 0x10006fb0,
           "dual OTA shim overlaps the installer manifest")
}
