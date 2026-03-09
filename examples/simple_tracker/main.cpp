#include <Arduino.h>

#include "TrackerMesh.h"
#include "tracker_peripherals.h"

#ifdef DISPLAY_CLASS
  #include "../simple_sensor/UITask.h"
  static UITask ui_task(display);
#endif

StdRNG fast_rng;
SimpleMeshTables tables;
TrackerMesh the_mesh(board, radio_driver, *new ArduinoMillis(), fast_rng, rtc_clock, tables);

static char command[160];

void halt() {
  while (1) ;
}

static FILESYSTEM* beginFilesystemAndIdentity() {
  FILESYSTEM* fs = NULL;
#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
  InternalFS.begin();
  fs = &InternalFS;
  IdentityStore store(InternalFS, "");
#elif defined(ESP32)
  SPIFFS.begin(true);
  fs = &SPIFFS;
  IdentityStore store(SPIFFS, "/identity");
#elif defined(RP2040_PLATFORM)
  LittleFS.begin();
  fs = &LittleFS;
  IdentityStore store(LittleFS, "/identity");
  store.begin();
#else
  #error "need to define filesystem"
#endif

  if (!store.load("_main", the_mesh.self_id)) {
    MESH_DEBUG_PRINTLN("Generating new keypair");
    TRACKER_DBG("identity not found, generating");
    the_mesh.self_id = radio_new_identity();
    int count = 0;
    while (count < 10 && (the_mesh.self_id.pub_key[0] == 0x00 || the_mesh.self_id.pub_key[0] == 0xFF)) {
      the_mesh.self_id = radio_new_identity();
      count++;
    }
    store.save("_main", the_mesh.self_id);
    TRACKER_DBG("identity generated and saved");
  } else {
    TRACKER_DBG("identity loaded from storage");
  }

  return fs;
}

static void beginDisplay() {
#ifdef DISPLAY_CLASS
  if (display.begin()) {
    display.startFrame();
    display.print("Please wait...");
    display.endFrame();
    TRACKER_DBG("display initialized");
  } else {
    TRACKER_DBG("display init failed");
  }
#endif
}

static void processSerialCommands() {
  int len = strlen(command);
  bool got_line = false;
  while (Serial.available() && len < sizeof(command) - 1) {
    char c = Serial.read();
    if (c == '\r' || c == '\n') {
      if (len > 0) {
        got_line = true;
      }
      continue;
    }
    if (c >= 32 && c <= 126) {
      command[len++] = c;
      command[len] = 0;
    } else {
      TRACKER_DBG("ignored non-printable serial byte: 0x%02X", (unsigned char)c);
    }
  }

  if (len == sizeof(command) - 1) {
    TRACKER_DBG("cmd auto-submit: buffer full");
    got_line = true;
  }
  if (!got_line || len == 0) {
    return;
  }

  char reply[240];
  TRACKER_DBG("cmd: %s", command);
  the_mesh.handleCommand(0, command, reply);
  if (reply[0]) {
    TRACKER_DBG("reply: %s", reply);
  }
  command[0] = 0;
}

void setup() {
  Serial.begin(115200);
  delay(1000);
  TRACKER_DBG("setup begin");

  board.begin();
  TRACKER_DBG("board.begin done");
  trackerPeripheralsBegin();

  beginDisplay();

  if (!radio_init()) {
    TRACKER_DBG("radio_init failed, halting");
    halt();
  }
  TRACKER_DBG("radio_init done");

  fast_rng.begin(radio_get_rng_seed());
  TRACKER_DBG("rng seeded");

  FILESYSTEM* fs = beginFilesystemAndIdentity();

  Serial.print("Tracker ID: ");
  mesh::Utils::printHex(Serial, the_mesh.self_id.pub_key, PUB_KEY_SIZE);
  Serial.println();

  command[0] = 0;

  sensors.begin();
  TRACKER_DBG("sensors.begin done");

  the_mesh.begin(fs);
  TRACKER_DBG("mesh.begin done");

#ifdef DISPLAY_CLASS
  ui_task.begin(the_mesh.getNodePrefs(), FIRMWARE_BUILD_DATE, FIRMWARE_VERSION);
  TRACKER_DBG("ui_task.begin done");
#endif

#if ENABLE_ADVERT_ON_BOOT == 1
  the_mesh.sendSelfAdvertisement(16000, false);
  TRACKER_DBG("boot advertisement sent");
#endif
  TRACKER_DBG("setup complete");
}

void loop() {
  processSerialCommands();

  the_mesh.loop();
  sensors.loop();
#ifdef DISPLAY_CLASS
  ui_task.loop();
#endif
  trackerPeripheralsLoop();
  if (trackerPeripheralsConsumeImmediateCycleRequest()) {
    the_mesh.queueImmediateCycle("button");
  }
  rtc_clock.tick();
}
