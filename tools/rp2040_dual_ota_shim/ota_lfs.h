#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "dual_ota_format.h"

bool lfsMount(uint8_t *start, uint32_t block_size, uint32_t size);
bool lfsReadCommand(MLKDualOTACommand *command, uint32_t *block_to_erase);
bool lfsOpen(const char *filename);
bool lfsSeek(uint32_t offset);
uint8_t *lfsRead(uint32_t len);
void lfsClose(void);
void lfsEraseBlock(uint32_t block_to_erase);
