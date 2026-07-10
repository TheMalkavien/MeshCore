#pragma once

#include <Mesh.h>

#ifdef WITH_ESPNOW_BRIDGE
#define WITH_BRIDGE
#endif

#ifdef WITH_BRIDGE
/*
 * Thin wrapper around the bridge implementations (src/helpers/bridges).
 *
 * The bridge headers pull in helpers/CommonCLI.h, whose NodePrefs struct clashes
 * with the companion's own NodePrefs (NodePrefs.h). So the bridge headers must
 * only ever be included from BridgeAdapter.cpp, never from companion headers.
 *
 * The companion has no CLI to edit the bridge settings, so they are fixed at
 * build time (see BridgeAdapter.cpp): ESPNOW_BRIDGE_SECRET, ESPNOW_BRIDGE_CHANNEL,
 * ESPNOW_BRIDGE_DELAY. Defaults match the simple_repeater bridge defaults so a
 * companion and a repeater bridge interoperate out of the box.
 */
namespace companion_bridge {
  void begin(mesh::PacketManager* mgr, mesh::RTCClock* rtc);
  void loop();
  void sendPacket(mesh::Packet* pkt);
  bool isRunning();
}
#endif
