#include "TrackerMesh.h"

#include "tracker_peripherals.h"

bool TrackerMesh::handleCustomCommand(uint32_t sender_timestamp, char* command, char* reply) {
  (void)sender_timestamp;
  return handleTrackerStatusCommand(command, reply) ||
         handleTrackerConfigCommand(command, reply) ||
         handleTrackerGroupCommand(command, reply) ||
         handleTrackerActionCommand(command, reply);
}

bool TrackerMesh::handleTrackerStatusCommand(char* command, char* reply) {
  if (strcmp(command, "tracker") == 0 || strcmp(command, "tracker status") == 0) {
    sprintf(reply,
      "tracker: interval=%lus timeout=%lus sleep=%s fix=%s min_sats=%u min_age=%us group=%s extpower=%s",
      (unsigned long)_interval_secs,
      (unsigned long)_gps_timeout_secs,
      _sleep_enabled ? "on" : "off",
      _require_live_fix ? "live" : "cached",
      (unsigned)_min_sats,
      (unsigned)_min_live_fix_age_secs,
      _group_name,
      _ignore_external_power ? "ignore" : "pause");
    return true;
  }

  if (strcmp(command, "tracker gps") == 0) {
    formatGPSStatus(reply, 240);
    return true;
  }

  if (strcmp(command, "tracker group") == 0) {
    sprintf(reply, "group=%s psk=%s (%s) hash=%02X%02X%02X%02X",
      _group_name,
      _group_psk,
      _group_psk_is_default ? "default" : "custom",
      _group_channel.hash[0],
      _group_channel.hash[1],
      _group_channel.hash[2],
      _group_channel.hash[3]);
    return true;
  }

  if (strcmp(command, "tracker extpower") == 0) {
    sprintf(reply, "extpower: mode=%s usb=%s paused=%s",
      _ignore_external_power ? "ignore" : "pause",
      trackerPeripheralsIsExternalPowered() ? "on" : "off",
      _external_power_paused ? "yes" : "no");
    return true;
  }

  return false;
}

bool TrackerMesh::handleTrackerConfigCommand(char* command, char* reply) {
  if (memcmp(command, "tracker interval ", 17) == 0) {
    uint32_t secs = atoi(&command[17]);
    if (secs < 30 || secs > 86400) {
      strcpy(reply, "Error: interval range is 30-86400 seconds");
    } else {
      _interval_secs = secs;
      persistTrackerConfig();
      strcpy(reply, "OK");
    }
    return true;
  }

  if (memcmp(command, "tracker timeout ", 16) == 0) {
    uint32_t secs = atoi(&command[16]);
    if (secs < 30 || secs > TRACKER_GPS_TIMEOUT_MAX_SECS) {
      snprintf(reply, 80, "Error: timeout range is 30-%u seconds", (unsigned)TRACKER_GPS_TIMEOUT_MAX_SECS);
    } else {
      _gps_timeout_secs = secs;
      persistTrackerConfig();
      strcpy(reply, "OK");
    }
    return true;
  }

  if (memcmp(command, "tracker sleep ", 14) == 0) {
    if (memcmp(&command[14], "on", 2) == 0) {
      _sleep_enabled = true;
    } else if (memcmp(&command[14], "off", 3) == 0) {
      _sleep_enabled = false;
    } else {
      strcpy(reply, "Error: use tracker sleep on|off");
      return true;
    }

    _sleep_waiting_for_queue_drain = false;
    _sleep_drain_started_millis = 0;
    persistTrackerConfig();
    strcpy(reply, "OK");
    return true;
  }

  if (memcmp(command, "tracker fix ", 12) == 0) {
    if (memcmp(&command[12], "live", 4) == 0) {
      _require_live_fix = true;
    } else if (memcmp(&command[12], "cached", 6) == 0) {
      _require_live_fix = false;
    } else {
      strcpy(reply, "Error: use tracker fix live|cached");
      return true;
    }

    persistTrackerConfig();
    strcpy(reply, "OK");
    return true;
  }

  if (memcmp(command, "tracker sats ", 13) == 0) {
    int sats = atoi(&command[13]);
    if (sats < 0 || sats > 30) {
      strcpy(reply, "Error: sats range is 0-30");
    } else {
      _min_sats = (uint8_t)sats;
      persistTrackerConfig();
      strcpy(reply, "OK");
    }
    return true;
  }

  if (memcmp(command, "tracker fix.age ", 16) == 0) {
    int secs = atoi(&command[16]);
    if (secs < 0 || secs > 3600) {
      strcpy(reply, "Error: fix.age range is 0-3600 seconds");
    } else {
      _min_live_fix_age_secs = (uint16_t)secs;
      persistTrackerConfig();
      strcpy(reply, "OK");
    }
    return true;
  }

  if (memcmp(command, "tracker extpower ignore ", 24) == 0) {
    if (memcmp(&command[24], "on", 2) == 0) {
      _ignore_external_power = true;
      _external_power_paused = false;
      queueImmediateCycle("external-power-ignore");
      strcpy(reply, "OK");
    } else if (memcmp(&command[24], "off", 3) == 0) {
      _ignore_external_power = false;
      strcpy(reply, "OK");
    } else {
      strcpy(reply, "Error: use tracker extpower ignore on|off");
    }
    return true;
  }

  return false;
}

bool TrackerMesh::handleTrackerGroupCommand(char* command, char* reply) {
  if (memcmp(command, "tracker group.name ", 19) == 0) {
    if (strlen(&command[19]) == 0) {
      strcpy(reply, "Error: group name cannot be empty");
    } else {
      StrHelper::strncpy(_group_name, &command[19], sizeof(_group_name));
      if (_group_psk_is_default && strcmp(_group_name, "Public") != 0) {
        _group_ready = false;
        strcpy(reply, "OK (set tracker group.psk <base64-psk>)");
      } else {
        if (_group_psk_is_default) {
          configureGroupChannel(_group_psk);
        }
        strcpy(reply, "OK");
      }
      persistTrackerConfig();
    }
    return true;
  }

  if (memcmp(command, "tracker group.psk ", 18) == 0) {
    char normalized_b64[GROUP_PSK_SIZE];
    if (configureGroupChannel(&command[18], normalized_b64, sizeof(normalized_b64))) {
      StrHelper::strncpy(_group_psk, normalized_b64, sizeof(_group_psk));
      _group_psk_is_default = false;
      persistTrackerConfig();
      strcpy(reply, "OK");
    } else {
      strcpy(reply, "Error: invalid group key (base64 or hex 16/32 bytes)");
    }
    return true;
  }

  return false;
}

bool TrackerMesh::handleTrackerActionCommand(char* command, char* reply) {
  if (strcmp(command, "tracker send") == 0 || strcmp(command, "tracker now") == 0) {
    queueImmediateCycle("cmd");
    strcpy(reply, "queued");
    return true;
  }
  return false;
}
