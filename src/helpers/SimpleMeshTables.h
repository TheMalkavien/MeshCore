#pragma once

#include <Mesh.h>

#ifdef ESP32
  #include <FS.h>
#endif

#define MAX_PACKET_HASHES  (128+32)

class SimpleMeshTables : public mesh::MeshTables {
  uint8_t _hashes[MAX_PACKET_HASHES*MAX_HASH_SIZE];
  uint8_t _hash0[MAX_PACKET_HASHES];   // first byte of each stored hash, for a cheap reject before memcmp
  int _next_idx;
  uint32_t _direct_dups, _flood_dups;

public:
  SimpleMeshTables() {
    memset(_hashes, 0, sizeof(_hashes));
    memset(_hash0, 0, sizeof(_hash0));
    _next_idx = 0;
    _direct_dups = _flood_dups = 0;
  }

#ifdef ESP32
  void restoreFrom(File f) {
    f.read(_hashes, sizeof(_hashes));
    f.read((uint8_t *) &_next_idx, sizeof(_next_idx));
    for (int i = 0; i < MAX_PACKET_HASHES; i++) _hash0[i] = _hashes[i * MAX_HASH_SIZE];  // rebuild reject cache
  }
  void saveTo(File f) {
    f.write(_hashes, sizeof(_hashes));
    f.write((const uint8_t *) &_next_idx, sizeof(_next_idx));
  }
#endif

  bool wasSeen(const mesh::Packet* packet) override {
    uint8_t hash[MAX_HASH_SIZE];
    packet->calculatePacketHash(hash);

    const uint8_t* sp = _hashes;
    for (int i = 0; i < MAX_PACKET_HASHES; i++, sp += MAX_HASH_SIZE) {
      if (_hash0[i] != hash[0]) continue;   // quick reject (equivalent to memcmp failing on byte 0)
      if (memcmp(hash, sp, MAX_HASH_SIZE) == 0) {
        if (packet->isRouteDirect()) {
          _direct_dups++;
        } else {
          _flood_dups++;
        }
        return true;
      }
    }
    return false;
  }

  void markSeen(const mesh::Packet* packet) override {
    uint8_t hash[MAX_HASH_SIZE];
    packet->calculatePacketHash(hash);
    memcpy(&_hashes[_next_idx * MAX_HASH_SIZE], hash, MAX_HASH_SIZE);
    _hash0[_next_idx] = hash[0];
    _next_idx = (_next_idx + 1) % MAX_PACKET_HASHES;
  }

  void clear(const mesh::Packet* packet) override {
    uint8_t hash[MAX_HASH_SIZE];
    packet->calculatePacketHash(hash);

    uint8_t* sp = _hashes;
    for (int i = 0; i < MAX_PACKET_HASHES; i++, sp += MAX_HASH_SIZE) {
      if (_hash0[i] != hash[0]) continue;   // quick reject (kept in sync with _hashes)
      if (memcmp(hash, sp, MAX_HASH_SIZE) == 0) {
        memset(sp, 0, MAX_HASH_SIZE);
        _hash0[i] = 0;
        break;
      }
    }
  }

  uint32_t getNumDirectDups() const { return _direct_dups; }
  uint32_t getNumFloodDups() const { return _flood_dups; }

  void resetStats() { _direct_dups = _flood_dups = 0; }
};
