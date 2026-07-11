#include "BridgeAdapter.h"

#include <string.h>

#ifdef WITH_ESPNOW_BRIDGE

// NOTE: this pulls in the CommonCLI NodePrefs (NOT the companion one), which is
// why this file must not include any companion header defining NodePrefs.
#include <helpers/bridges/ESPNowBridge.h>

#ifndef ESPNOW_BRIDGE_CHANNEL
  #define ESPNOW_BRIDGE_CHANNEL 1        // 1-14, ignored when ESPNOW_BRIDGE_SHARED_WIFI (AP decides)
#endif
#ifndef ESPNOW_BRIDGE_SECRET
  #define ESPNOW_BRIDGE_SECRET "LVSITANOS"   // must match the other bridges on the network
#endif
#ifndef ESPNOW_BRIDGE_DELAY
  #define ESPNOW_BRIDGE_DELAY 500        // milliseconds, inbound queueing delay
#endif

static NodePrefs bridge_prefs;   // CommonCLI NodePrefs, only the bridge_* fields are used
static ESPNowBridge* espnow_bridge = NULL;

void companion_bridge::begin(mesh::PacketManager* mgr, mesh::RTCClock* rtc) {
  memset(&bridge_prefs, 0, sizeof(bridge_prefs));
  bridge_prefs.bridge_enabled = 1;
  bridge_prefs.bridge_delay = ESPNOW_BRIDGE_DELAY;
  bridge_prefs.bridge_channel = ESPNOW_BRIDGE_CHANNEL;
  strncpy(bridge_prefs.bridge_secret, ESPNOW_BRIDGE_SECRET, sizeof(bridge_prefs.bridge_secret) - 1);

  espnow_bridge = new ESPNowBridge(&bridge_prefs, mgr, rtc);
  espnow_bridge->begin();
}

void companion_bridge::loop() {
  if (espnow_bridge) espnow_bridge->loop();
}

void companion_bridge::sendPacket(mesh::Packet* pkt) {
  if (espnow_bridge) espnow_bridge->sendPacket(pkt);
}

bool companion_bridge::isRunning() {
  return espnow_bridge && espnow_bridge->isRunning();
}

#endif
