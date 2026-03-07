const MAX_MESH_CMD_TEXT_LEN = 160;
const REPEATER_CMD_TOKEN_LEN = 5; // "0000|"
const OTA_WRITE_CMD_PREFIX = "ota write ";
const OTA_BINARY_REQ_TYPE = 0x70;
const OTA_BIN_OP = {
  START: 1,
  BEGIN: 2,
  WRITE: 3,
  END: 4,
  STATUS: 5,
  ABORT: 6,
};
const OTA_BINARY_MAX_CHUNK = 132;
const CMD_SET_RADIO_PARAMS = 11;

const PACKET_TYPE = {
  OK: 0,
  ERROR: 1,
  CONTACT_START: 2,
  CONTACT: 3,
  CONTACT_END: 4,
  SELF_INFO: 5,
  MSG_SENT: 6,
  CONTACT_MSG_RECV: 7,
  NO_MORE_MSGS: 10,
  DEVICE_INFO: 13,
  CONTACT_MSG_RECV_V3: 16,
  MESSAGES_WAITING: 0x83,
  LOGIN_SUCCESS: 0x85,
  LOGIN_FAILED: 0x86,
  BINARY_RESPONSE: 0x8c,
};

const CONTACT_TYPE = {
  COMPANION: 1,
  REPEATER: 2,
  ROOM_SERVER: 3,
  SENSOR: 4,
};

const WEBUSB_REQ_SET_LINE_CODING = 0x20;
const WEBUSB_REQ_SET_CONTROL_LINE_STATE = 0x22;
const WEBUSB_CDC_CLASS_CONTROL = 0x02;
const WEBUSB_CDC_CLASS_DATA = 0x0a;
const WEBUSB_DEFAULT_PACKET_SIZE = 64;
const MESHCORE_BLE_SERVICE_UUID = "6e400001-b5a3-f393-e0a9-e50e24dcca9e";
const MESHCORE_BLE_RX_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e";
const MESHCORE_BLE_TX_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e";
const WEBUSB_DEVICE_FILTERS = [
  { vendorId: 0x2e8a }, // Raspberry Pi / RP2040
  { vendorId: 0x10c4 }, // Silicon Labs CP210x
  { vendorId: 0x1a86 }, // QinHeng CH34x
  { classCode: WEBUSB_CDC_CLASS_CONTROL },
  { classCode: WEBUSB_CDC_CLASS_DATA },
];

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: false });

function nowTime() {
  const d = new Date();
  return d.toLocaleTimeString("fr-FR", { hour12: false });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeHex(value) {
  return String(value || "").replace(/[^0-9a-fA-F]/g, "").toLowerCase();
}

function hexToBytes(hex) {
  const clean = normalizeHex(hex);
  if (clean.length % 2 !== 0) {
    throw new Error("hex invalide (longueur impaire)");
  }
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i += 1) {
    out[i] = Number.parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function bytesToHex(bytes) {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

function parseU32LE(bytes, offset) {
  return (
    (bytes[offset] || 0) |
    ((bytes[offset + 1] || 0) << 8) |
    ((bytes[offset + 2] || 0) << 16) |
    ((bytes[offset + 3] || 0) << 24)
  ) >>> 0;
}

function u32ToBytesLE(value) {
  const v = value >>> 0;
  return new Uint8Array([v & 0xff, (v >> 8) & 0xff, (v >> 16) & 0xff, (v >> 24) & 0xff]);
}

function concatBytes(...parts) {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

function isWebSerialPortLike(port) {
  return Boolean(
    port
    && typeof port.open === "function"
    && port.readable
    && port.writable
  );
}

function isWebUsbDeviceLike(device) {
  return Boolean(
    device
    && typeof device.open === "function"
    && typeof device.transferIn === "function"
    && typeof device.transferOut === "function"
  );
}

function isWebBleDeviceLike(device) {
  return Boolean(
    device
    && device.gatt
    && typeof device.gatt.connect === "function"
  );
}

function getUsbBrowserPreference() {
  const ua = String(navigator.userAgent || "").toLowerCase();
  const platform = String(navigator.platform || "").toLowerCase();
  if (ua.includes("android")) return "webusb";
  if (platform.includes("win") || platform.includes("mac") || platform.includes("linux")) return "webserial";
  return "auto";
}

function configurationHasBulkPair(cfg) {
  for (const iface of cfg?.interfaces || []) {
    for (const alt of iface.alternates || []) {
      const inEp = (alt.endpoints || []).some((ep) => ep.type === "bulk" && ep.direction === "in");
      const outEp = (alt.endpoints || []).some((ep) => ep.type === "bulk" && ep.direction === "out");
      if (inEp && outEp) return true;
    }
  }
  return false;
}

function listWebUsbBulkCandidates(device) {
  const cfg = device?.configuration;
  if (!cfg || !Array.isArray(cfg.interfaces)) return [];

  const controlIfaces = [];
  for (const iface of cfg.interfaces) {
    for (const alt of iface.alternates || []) {
      if (alt.interfaceClass === WEBUSB_CDC_CLASS_CONTROL) {
        controlIfaces.push(iface.interfaceNumber);
        break;
      }
    }
  }

  const candidates = [];
  for (const iface of cfg.interfaces) {
    for (const alt of iface.alternates || []) {
      const inEp = (alt.endpoints || []).find((ep) => ep.type === "bulk" && ep.direction === "in");
      const outEp = (alt.endpoints || []).find((ep) => ep.type === "bulk" && ep.direction === "out");
      if (!inEp || !outEp) continue;

      let controlInterfaceNumber = iface.interfaceNumber;
      if (alt.interfaceClass === WEBUSB_CDC_CLASS_DATA) {
        const prev = iface.interfaceNumber - 1;
        if (controlIfaces.includes(prev)) {
          controlInterfaceNumber = prev;
        } else if (controlIfaces.length > 0) {
          controlInterfaceNumber = controlIfaces[0];
        }
      } else if (controlIfaces.length > 0) {
        controlInterfaceNumber = controlIfaces[0];
      }

      candidates.push({
        interfaceNumber: iface.interfaceNumber,
        alternateSetting: alt.alternateSetting || 0,
        inEndpoint: inEp.endpointNumber,
        outEndpoint: outEp.endpointNumber,
        inPacketSize: Number(inEp.packetSize || WEBUSB_DEFAULT_PACKET_SIZE),
        outPacketSize: Number(outEp.packetSize || WEBUSB_DEFAULT_PACKET_SIZE),
        classCode: alt.interfaceClass,
        controlInterfaceNumber,
      });
    }
  }

  candidates.sort((a, b) => {
    const rank = (c) => {
      if (c.classCode === WEBUSB_CDC_CLASS_DATA) return 0;
      if (c.classCode === 0xff) return 1; // vendor specific
      return 2;
    };
    return rank(a) - rank(b);
  });
  return candidates;
}

function formatWebUsbCandidateLabel(candidate) {
  return `if${candidate.interfaceNumber}/alt${candidate.alternateSetting}/cls0x${Number(candidate.classCode).toString(16)} in${candidate.inEndpoint} out${candidate.outEndpoint}`;
}

function selectPreferredWebUsbCandidates(candidates) {
  const cdcData = candidates.filter((c) => c.classCode === WEBUSB_CDC_CLASS_DATA);
  if (cdcData.length > 0) {
    return {
      claimCandidates: cdcData,
      skippedCandidates: candidates.filter((c) => c.classCode !== WEBUSB_CDC_CLASS_DATA),
      mode: "cdc_data",
    };
  }

  const cdcAny = candidates.filter(
    (c) => c.classCode === WEBUSB_CDC_CLASS_CONTROL || c.classCode === WEBUSB_CDC_CLASS_DATA
  );
  if (cdcAny.length > 0) {
    return {
      claimCandidates: cdcAny,
      skippedCandidates: candidates.filter(
        (c) => c.classCode !== WEBUSB_CDC_CLASS_CONTROL && c.classCode !== WEBUSB_CDC_CLASS_DATA
      ),
      mode: "cdc_any",
    };
  }

  return {
    claimCandidates: candidates,
    skippedCandidates: [],
    mode: "generic",
  };
}

function maxOtaChunkForOffset(offset) {
  const fixedLen = REPEATER_CMD_TOKEN_LEN + OTA_WRITE_CMD_PREFIX.length + String(offset).length + 1;
  const availableHex = MAX_MESH_CMD_TEXT_LEN - fixedLen;
  if (availableHex < 2) return 0;
  return Math.floor(availableHex / 2);
}

function estimateOtaChunkCount(totalSize, preferredChunk) {
  if (totalSize <= 0) return 0;
  let offset = 0;
  let count = 0;
  while (offset < totalSize) {
    const maxChunk = maxOtaChunkForOffset(offset);
    if (maxChunk < 1) return 0;
    const step = Math.min(preferredChunk, maxChunk, totalSize - offset);
    offset += step;
    count += 1;
  }
  return count;
}

function estimateOtaChunkIndex(offset, preferredChunk) {
  if (offset <= 0) return 0;
  let sent = 0;
  let count = 0;
  while (sent < offset) {
    const maxChunk = maxOtaChunkForOffset(sent);
    if (maxChunk < 1) break;
    const step = Math.min(preferredChunk, maxChunk, offset - sent);
    sent += step;
    count += 1;
  }
  return count;
}

function parseOtaStatus(text) {
  const m = /(\d+)\s*\/\s*(\d+)/.exec(text || "");
  if (!m) return null;
  return { done: Number.parseInt(m[1], 10), total: Number.parseInt(m[2], 10) };
}

function parseOtaOffsetError(text) {
  const m = /Err - offset (\d+) expected (\d+)/i.exec(text || "");
  if (!m) return null;
  return { got: Number.parseInt(m[1], 10), expected: Number.parseInt(m[2], 10) };
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return "-";
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const min = Math.floor(seconds / 60);
  return `${min}m ${(seconds - (min * 60)).toFixed(1)}s`;
}

function clamp(value, minValue, maxValue) {
  return Math.max(minValue, Math.min(maxValue, value));
}

function formatRadioValue(value, decimals = 3) {
  if (!Number.isFinite(Number(value))) return "-";
  return Number(value).toFixed(decimals).replace(/\.?0+$/, "");
}

function normalizeRadioPreset(preset) {
  if (!preset) return null;
  const freq = Number(preset.freq);
  const bw = Number(preset.bw);
  const sf = Math.trunc(Number(preset.sf));
  const cr = Math.trunc(Number(preset.cr));
  if (!Number.isFinite(freq) || !Number.isFinite(bw) || !Number.isFinite(sf) || !Number.isFinite(cr)) {
    return null;
  }
  if (freq < 300.0 || freq > 2500.0) return null;
  if (bw < 7.0 || bw > 500.0) return null;
  if (sf < 5 || sf > 12) return null;
  if (cr < 5 || cr > 8) return null;
  return { freq, bw, sf, cr };
}

function extractRadioPresetFromSelfInfo(selfInfo) {
  if (!selfInfo) return null;
  return normalizeRadioPreset({
    freq: selfInfo.radio_freq,
    bw: selfInfo.radio_bw,
    sf: selfInfo.radio_sf,
    cr: selfInfo.radio_cr,
  });
}

function safeAdd32(x, y) {
  const lsw = (x & 0xffff) + (y & 0xffff);
  const msw = (x >>> 16) + (y >>> 16) + (lsw >>> 16);
  return (((msw & 0xffff) << 16) | (lsw & 0xffff)) >>> 0;
}

function rotl32(x, n) {
  return ((x << n) | (x >>> (32 - n))) >>> 0;
}

function md5Digest(bytes) {
  const s = [
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
    5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20,
    4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
  ];

  const k = [
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee,
    0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
    0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
    0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
    0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa,
    0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
    0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
    0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
    0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
    0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05,
    0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039,
    0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
    0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
  ];

  const origLen = bytes.length >>> 0;
  const padLen = ((56 - ((origLen + 1) % 64)) + 64) % 64;
  const totalLen = origLen + 1 + padLen + 8;
  const msg = new Uint8Array(totalLen);
  msg.set(bytes);
  msg[origLen] = 0x80;

  const bitLenLo = (origLen << 3) >>> 0;
  const bitLenHi = (origLen >>> 29) >>> 0;
  msg[totalLen - 8] = bitLenLo & 0xff;
  msg[totalLen - 7] = (bitLenLo >>> 8) & 0xff;
  msg[totalLen - 6] = (bitLenLo >>> 16) & 0xff;
  msg[totalLen - 5] = (bitLenLo >>> 24) & 0xff;
  msg[totalLen - 4] = bitLenHi & 0xff;
  msg[totalLen - 3] = (bitLenHi >>> 8) & 0xff;
  msg[totalLen - 2] = (bitLenHi >>> 16) & 0xff;
  msg[totalLen - 1] = (bitLenHi >>> 24) & 0xff;

  let a0 = 0x67452301;
  let b0 = 0xefcdab89;
  let c0 = 0x98badcfe;
  let d0 = 0x10325476;

  const x = new Uint32Array(16);
  for (let offset = 0; offset < totalLen; offset += 64) {
    for (let i = 0; i < 16; i += 1) {
      const j = offset + (i * 4);
      x[i] = (msg[j]) | (msg[j + 1] << 8) | (msg[j + 2] << 16) | (msg[j + 3] << 24);
    }

    let a = a0;
    let b = b0;
    let c = c0;
    let d = d0;

    for (let i = 0; i < 64; i += 1) {
      let f = 0;
      let g = 0;

      if (i < 16) {
        f = (b & c) | ((~b) & d);
        g = i;
      } else if (i < 32) {
        f = (d & b) | ((~d) & c);
        g = ((5 * i) + 1) % 16;
      } else if (i < 48) {
        f = b ^ c ^ d;
        g = ((3 * i) + 5) % 16;
      } else {
        f = c ^ (b | (~d));
        g = (7 * i) % 16;
      }

      const temp = d;
      d = c;
      c = b;
      const sum = safeAdd32(a, safeAdd32(f >>> 0, safeAdd32(k[i], x[g] >>> 0)));
      b = safeAdd32(b, rotl32(sum, s[i]));
      a = temp;
    }

    a0 = safeAdd32(a0, a);
    b0 = safeAdd32(b0, b);
    c0 = safeAdd32(c0, c);
    d0 = safeAdd32(d0, d);
  }

  const out = new Uint8Array(16);
  const words = [a0, b0, c0, d0];
  for (let i = 0; i < 4; i += 1) {
    const w = words[i] >>> 0;
    out[(i * 4)] = w & 0xff;
    out[(i * 4) + 1] = (w >>> 8) & 0xff;
    out[(i * 4) + 2] = (w >>> 16) & 0xff;
    out[(i * 4) + 3] = (w >>> 24) & 0xff;
  }
  return out;
}

function md5Hex(bytes) {
  return bytesToHex(md5Digest(bytes));
}

function estimateRejectedChunks(localOffset, serverOffset, chunkSize) {
  if (!Number.isFinite(localOffset) || !Number.isFinite(serverOffset)) return 0;
  if (serverOffset >= localOffset) return 0;
  const step = Math.max(1, Number(chunkSize) || 1);
  return Math.max(1, Math.ceil((localOffset - serverOffset) / step));
}

function estimateOtaRadioProfile(radioBwKhz, radioSf, radioCr) {
  const bw = Number(radioBwKhz);
  const sfRaw = Number(radioSf);
  const crRaw = Number(radioCr);
  if (!Number.isFinite(bw) || !Number.isFinite(sfRaw) || !Number.isFinite(crRaw) || bw <= 0) {
    return null;
  }

  const sf = Math.trunc(clamp(sfRaw, 5, 12));
  const cr = Math.trunc(clamp(crRaw, 5, 8));

  const symRatio = (2 ** (sf - 7)) * (250.0 / bw);
  const crFactor = clamp(cr / 5.0, 1.0, 1.8);
  const airFactor = clamp(symRatio * crFactor, 0.1, 20.0);

  let chunkBinary = 72;
  if (airFactor <= 1.2) chunkBinary = 132;
  else if (airFactor <= 2.5) chunkBinary = 120;
  else if (airFactor <= 5.0) chunkBinary = 104;
  else if (airFactor <= 10.0) chunkBinary = 88;

  let chunkText = 48;
  if (airFactor <= 1.2) chunkText = 80;
  else if (airFactor <= 2.5) chunkText = 72;
  else if (airFactor <= 5.0) chunkText = 64;
  else if (airFactor <= 10.0) chunkText = 56;

  let ackEvery = 2;
  if (airFactor <= 0.25) ackEvery = 12;
  else if (airFactor <= 0.6) ackEvery = 10;
  else if (airFactor <= 1.2) ackEvery = 8;
  else if (airFactor <= 2.5) ackEvery = 6;
  else if (airFactor <= 5.0) ackEvery = 4;
  else if (airFactor <= 10.0) ackEvery = 3;

  const noAckGapSec = clamp(0.015 + (0.025 * airFactor), 0.015, 0.45);
  const checkpointTimeoutSec = clamp(0.35 + (0.6 * airFactor), 1.0, 12.0);
  const statusTimeoutSec = clamp(0.35 + (0.8 * airFactor), 0.5, 10.0);

  return {
    bwKhz: bw,
    sf,
    cr,
    airFactor,
    chunkBinary,
    chunkText,
    ackEvery,
    noAckGapSec,
    checkpointTimeoutSec,
    statusTimeoutSec,
  };
}

function computeAutoOtaSettingsFromProfile(profile) {
  if (!profile) {
    return {
      profile: null,
      chunkSize: 96,
      ackEvery: 1,
      noAckGapMs: 50,
      checkpointTimeoutMs: 1000,
      statusTimeoutMs: 4000,
      ackSettleGapMs: 30,
    };
  }

  const noAckGapMs = Math.round(profile.noAckGapSec * 1000.0);
  const checkpointTimeoutMs = Math.round(profile.checkpointTimeoutSec * 1000.0);
  return {
    profile,
    chunkSize: profile.chunkBinary,
    ackEvery: profile.ackEvery,
    noAckGapMs,
    checkpointTimeoutMs,
    statusTimeoutMs: Math.round(profile.statusTimeoutSec * 1000.0),
    ackSettleGapMs: Math.round(clamp(noAckGapMs * 0.5, 10, 200)),
  };
}

function computeAutoOtaSettings(selfInfo) {
  const profile = estimateOtaRadioProfile(
    selfInfo?.radio_bw,
    selfInfo?.radio_sf,
    selfInfo?.radio_cr
  );
  return computeAutoOtaSettingsFromProfile(profile);
}

class MeshCoreSerialClient {
  constructor(logger) {
    this.log = logger;
    this.port = null;
    this.reader = null;
    this.writer = null;
    this.usbDevice = null;
    this.usbDataInterface = null;
    this.usbControlInterface = null;
    this.usbInEndpoint = null;
    this.usbOutEndpoint = null;
    this.usbInPacketSize = WEBUSB_DEFAULT_PACKET_SIZE;
    this.usbOutPacketSize = WEBUSB_DEFAULT_PACKET_SIZE;
    this.bleDevice = null;
    this.bleServer = null;
    this.bleRxCharacteristic = null;
    this.bleTxCharacteristic = null;
    this.bleNotificationHandler = null;
    this.bleDisconnectHandler = null;
    this.transport = null;
    this.connected = false;
    this.rxPending = [];
    this.waiters = [];
    this.seq = Date.now() & 0xffff;
    this.messageQueue = [];
    this.maxMessageQueue = 512;
    this.binaryQueue = [];
    this.maxBinaryQueue = 256;
    this.contactsByKey = {};
    this.contactsScanTmp = {};
    this.onEvent = null;
    this.writeChain = Promise.resolve();
  }

  async connect(portOrDevice, baudrate) {
    if (isWebSerialPortLike(portOrDevice)) {
      await this.connectSerial(portOrDevice, baudrate);
      return;
    }
    if (isWebUsbDeviceLike(portOrDevice)) {
      await this.connectWebUsb(portOrDevice, baudrate);
      return;
    }
    if (isWebBleDeviceLike(portOrDevice)) {
      await this.connectBle(portOrDevice);
      return;
    }
    throw new Error("transport non supporte");
  }

  async connectSerial(port, baudrate) {
    if (!port) throw new Error("port USB manquant");
    this.port = port;
    await this.port.open({
      baudRate: baudrate,
      dataBits: 8,
      stopBits: 1,
      parity: "none",
      flowControl: "none",
    });
    this.reader = this.port.readable.getReader();
    this.writer = this.port.writable.getWriter();
    this.transport = "web_serial";
    this.connected = true;
    this.rxPending = [];
    this.readLoopSerial();
  }

  async connectWebUsb(device, baudrate = 115200) {
    if (!device) throw new Error("peripherique USB manquant");

    this.usbDevice = device;
    await this.usbDevice.open();
    this.log(
      `WebUSB open: vid=0x${Number(this.usbDevice.vendorId || 0).toString(16)} `
      + `pid=0x${Number(this.usbDevice.productId || 0).toString(16)}`
    );
    if (!this.usbDevice.configuration) {
      const cfgs = Array.isArray(this.usbDevice.configurations)
        ? this.usbDevice.configurations
        : [];
      if (cfgs.length > 0) {
        const preferredCfg = cfgs.find((cfg) => configurationHasBulkPair(cfg)) || cfgs[0];
        this.log(`WebUSB selectConfiguration: ${preferredCfg.configurationValue}`);
        await this.usbDevice.selectConfiguration(preferredCfg.configurationValue);
      } else {
        this.log("WebUSB selectConfiguration: fallback #1");
        await this.usbDevice.selectConfiguration(1);
      }
    }

    const candidates = listWebUsbBulkCandidates(this.usbDevice);
    if (candidates.length < 1) {
      throw new Error("interface CDC bulk IN/OUT non trouvee");
    }
    this.log(
      `WebUSB candidates: ${candidates
        .map((c) => formatWebUsbCandidateLabel(c))
        .join(", ")}` 
    );

    const preferred = selectPreferredWebUsbCandidates(candidates);
    if (preferred.skippedCandidates.length > 0) {
      this.log(
        `WebUSB ignored non-serial candidates: ${preferred.skippedCandidates
          .map((c) => formatWebUsbCandidateLabel(c))
          .join(", ")}`
      );
    }

    let selected = null;
    let lastClaimErr = null;
    for (const c of preferred.claimCandidates) {
      try {
        this.log(`WebUSB claim try: if${c.interfaceNumber}`);
        await this.usbDevice.claimInterface(c.interfaceNumber);
        if (c.alternateSetting && c.alternateSetting > 0) {
          await this.usbDevice.selectAlternateInterface(c.interfaceNumber, c.alternateSetting);
        }
        this.log(`WebUSB claim ok: if${c.interfaceNumber}`);
        selected = c;
        break;
      } catch (e) {
        lastClaimErr = e;
        this.log(`WebUSB claim failed: if${c.interfaceNumber} -> ${e.message}`);
        try { await this.usbDevice.releaseInterface(c.interfaceNumber); } catch {}
      }
    }

    if (!selected) {
      const hint = preferred.mode === "generic"
        ? "interface USB bulk indisponible."
        : "interface CDC serie detectee mais verrouillee par l'OS; utiliser Web Serial si disponible.";
      const details = preferred.claimCandidates
        .map((c) => `if${c.interfaceNumber}/cls0x${Number(c.classCode).toString(16)}`)
        .join(", ");
      throw new Error(`Unable to claim interface: ${hint} candidates=[${details}] ${lastClaimErr ? `(${lastClaimErr.message})` : ""}`.trim());
    }

    this.usbDataInterface = selected.interfaceNumber;
    this.usbControlInterface = selected.controlInterfaceNumber;
    this.usbInEndpoint = selected.inEndpoint;
    this.usbOutEndpoint = selected.outEndpoint;
    this.usbInPacketSize = Math.max(8, selected.inPacketSize || WEBUSB_DEFAULT_PACKET_SIZE);
    this.usbOutPacketSize = Math.max(8, selected.outPacketSize || WEBUSB_DEFAULT_PACKET_SIZE);

    if (
      this.usbControlInterface !== null
      && this.usbControlInterface !== undefined
      && this.usbControlInterface !== this.usbDataInterface
    ) {
      try {
        await this.usbDevice.claimInterface(this.usbControlInterface);
      } catch {
        // Optional.
      }
    }

    try {
      const lineCoding = new Uint8Array(7);
      const baud = Number(baudrate) || 115200;
      lineCoding[0] = baud & 0xff;
      lineCoding[1] = (baud >> 8) & 0xff;
      lineCoding[2] = (baud >> 16) & 0xff;
      lineCoding[3] = (baud >> 24) & 0xff;
      lineCoding[4] = 0; // 1 stop bit
      lineCoding[5] = 0; // no parity
      lineCoding[6] = 8; // data bits
      await this.usbDevice.controlTransferOut(
        {
          requestType: "class",
          recipient: "interface",
          request: WEBUSB_REQ_SET_LINE_CODING,
          value: 0,
          index: this.usbControlInterface ?? this.usbDataInterface,
        },
        lineCoding
      );
    } catch {
      // Optional for some CDC implementations.
    }

    try {
      await this.usbDevice.controlTransferOut(
        {
          requestType: "class",
          recipient: "interface",
          request: WEBUSB_REQ_SET_CONTROL_LINE_STATE,
          value: 0x0001, // DTR
          index: this.usbControlInterface ?? this.usbDataInterface,
        }
      );
    } catch {
      // Optional for some CDC implementations.
    }

    this.transport = "web_usb";
    this.connected = true;
    this.rxPending = [];
    this.readLoopWebUsb();
  }

  async connectBle(device) {
    if (!device) throw new Error("peripherique BLE manquant");
    this.bleDevice = device;
    this.bleDisconnectHandler = async () => {
      if (!this.connected) return;
      this.log("Connexion BLE interrompue.");
      await this.disconnect();
    };
    this.bleDevice.addEventListener("gattserverdisconnected", this.bleDisconnectHandler);

    this.bleServer = await this.bleDevice.gatt.connect();
    const service = await this.bleServer.getPrimaryService(MESHCORE_BLE_SERVICE_UUID);
    const characteristics = await service.getCharacteristics();

    this.bleRxCharacteristic = characteristics.find(
      (c) => String(c.uuid || "").toLowerCase() === MESHCORE_BLE_RX_UUID
    ) || null;
    this.bleTxCharacteristic = characteristics.find(
      (c) => String(c.uuid || "").toLowerCase() === MESHCORE_BLE_TX_UUID
    ) || null;
    if (!this.bleRxCharacteristic || !this.bleTxCharacteristic) {
      throw new Error("service BLE MeshCore detecte, mais caracteristiques RX/TX introuvables");
    }

    this.bleNotificationHandler = (event) => {
      const value = event?.target?.value;
      if (!value || value.byteLength < 1) return;
      const frame = new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
      this.handleFrame(frame);
    };
    await this.bleTxCharacteristic.startNotifications();
    this.bleTxCharacteristic.addEventListener("characteristicvaluechanged", this.bleNotificationHandler);

    this.transport = "web_ble";
    this.connected = true;
    this.rxPending = [];
  }

  async disconnect() {
    this.connected = false;
    this.waiters.forEach((w) => {
      clearTimeout(w.timer);
      w.reject(new Error("deconnecte"));
    });
    this.waiters = [];
    if (this.reader) {
      try { await this.reader.cancel(); } catch {}
      try { this.reader.releaseLock(); } catch {}
      this.reader = null;
    }
    if (this.writer) {
      try { this.writer.releaseLock(); } catch {}
      this.writer = null;
    }
    if (this.port) {
      try { await this.port.close(); } catch {}
      this.port = null;
    }
    if (this.usbDevice) {
      try {
        if (this.usbDataInterface !== null && this.usbDataInterface !== undefined) {
          await this.usbDevice.releaseInterface(this.usbDataInterface);
        }
      } catch {}
      try {
        if (
          this.usbControlInterface !== null
          && this.usbControlInterface !== undefined
          && this.usbControlInterface !== this.usbDataInterface
        ) {
          await this.usbDevice.releaseInterface(this.usbControlInterface);
        }
      } catch {}
      try { await this.usbDevice.close(); } catch {}
      this.usbDevice = null;
    }
    this.usbDataInterface = null;
    this.usbControlInterface = null;
    this.usbInEndpoint = null;
    this.usbOutEndpoint = null;
    this.usbInPacketSize = WEBUSB_DEFAULT_PACKET_SIZE;
    this.usbOutPacketSize = WEBUSB_DEFAULT_PACKET_SIZE;
    if (this.bleTxCharacteristic && this.bleNotificationHandler) {
      try {
        this.bleTxCharacteristic.removeEventListener("characteristicvaluechanged", this.bleNotificationHandler);
      } catch {}
    }
    if (this.bleTxCharacteristic) {
      try { await this.bleTxCharacteristic.stopNotifications(); } catch {}
    }
    this.bleNotificationHandler = null;
    this.bleTxCharacteristic = null;
    this.bleRxCharacteristic = null;
    if (this.bleDevice && this.bleDisconnectHandler) {
      try {
        this.bleDevice.removeEventListener("gattserverdisconnected", this.bleDisconnectHandler);
      } catch {}
    }
    this.bleDisconnectHandler = null;
    if (this.bleServer) {
      try { this.bleServer.disconnect(); } catch {}
      this.bleServer = null;
    } else if (this.bleDevice?.gatt?.connected) {
      try { this.bleDevice.gatt.disconnect(); } catch {}
    }
    this.bleDevice = null;
    this.transport = null;
    this.writeChain = Promise.resolve();
  }

  async readLoopSerial() {
    while (this.connected && this.reader) {
      let res;
      try {
        res = await this.reader.read();
      } catch (e) {
        if (this.connected) this.log(`RX error: ${e.message}`);
        break;
      }
      if (!res || res.done) break;
      if (!res.value || res.value.length === 0) continue;
      this.pushRxChunk(res.value);
      this.parseFrames();
    }
    if (this.connected) {
      this.log("Connexion serie interrompue.");
      await this.disconnect();
    }
  }

  async readLoopWebUsb() {
    while (this.connected && this.usbDevice && this.usbInEndpoint !== null) {
      let res;
      try {
        res = await this.usbDevice.transferIn(this.usbInEndpoint, this.usbInPacketSize);
      } catch (e) {
        if (this.connected) this.log(`USB RX error: ${e.message}`);
        break;
      }
      if (!res) continue;
      if (res.status === "stall") {
        try { await this.usbDevice.clearHalt("in", this.usbInEndpoint); } catch {}
        continue;
      }
      if (res.status !== "ok" || !res.data || res.data.byteLength === 0) continue;

      const chunk = new Uint8Array(res.data.buffer, res.data.byteOffset, res.data.byteLength);
      this.pushRxChunk(chunk);
      this.parseFrames();
    }
    if (this.connected) {
      this.log("Connexion USB interrompue.");
      await this.disconnect();
    }
  }

  pushRxChunk(chunk) {
    for (const b of chunk) this.rxPending.push(b);
  }

  parseFrames() {
    while (true) {
      const start = this.rxPending.indexOf(0x3e);
      if (start < 0) {
        if (this.rxPending.length > 2048) this.rxPending = [];
        return;
      }
      if (start > 0) this.rxPending.splice(0, start);
      if (this.rxPending.length < 3) return;
      const frameSize = this.rxPending[1] | (this.rxPending[2] << 8);
      if (frameSize <= 0 || frameSize > 300) {
        this.rxPending.shift();
        continue;
      }
      if (this.rxPending.length < 3 + frameSize) return;
      const frame = this.rxPending.slice(3, 3 + frameSize);
      this.rxPending.splice(0, 3 + frameSize);
      this.handleFrame(new Uint8Array(frame));
    }
  }

  handleFrame(frame) {
    if (!frame || frame.length === 0) return;
    const packetType = frame[0];
    switch (packetType) {
      case PACKET_TYPE.OK:
        this.emit("ok", {});
        break;
      case PACKET_TYPE.ERROR:
        this.emit("error", { error_code: frame.length > 1 ? frame[1] : null });
        break;
      case PACKET_TYPE.CONTACT_START:
        this.contactsScanTmp = {};
        this.emit("contacts_start", { count: frame.length >= 5 ? parseU32LE(frame, 1) : 0 });
        break;
      case PACKET_TYPE.CONTACT: {
        const contact = this.parseContact(frame);
        if (contact?.public_key) this.contactsScanTmp[contact.public_key] = contact;
        this.emit("contact", contact);
        break;
      }
      case PACKET_TYPE.CONTACT_END: {
        const payload = {
          lastmod: frame.length >= 5 ? parseU32LE(frame, 1) : 0,
          contacts: { ...this.contactsScanTmp },
        };
        this.contactsByKey = { ...payload.contacts };
        this.emit("contacts_end", payload);
        break;
      }
      case PACKET_TYPE.SELF_INFO:
        this.emit("self_info", this.parseSelfInfo(frame));
        break;
      case PACKET_TYPE.DEVICE_INFO:
        this.emit("device_info", this.parseDeviceInfo(frame));
        break;
      case PACKET_TYPE.MSG_SENT:
        this.emit("msg_sent", this.parseMsgSent(frame));
        break;
      case PACKET_TYPE.CONTACT_MSG_RECV:
      case PACKET_TYPE.CONTACT_MSG_RECV_V3: {
        const msg = this.parseContactMsg(frame, packetType === PACKET_TYPE.CONTACT_MSG_RECV_V3);
        if (msg && msg.txt_type === 1) {
          this.messageQueue.push(msg);
          if (this.messageQueue.length > this.maxMessageQueue) {
            this.messageQueue.splice(0, this.messageQueue.length - this.maxMessageQueue);
          }
        }
        this.emit("contact_msg_recv", msg);
        break;
      }
      case PACKET_TYPE.NO_MORE_MSGS:
        this.emit("no_more_msgs", {});
        break;
      case PACKET_TYPE.MESSAGES_WAITING:
        this.emit("messages_waiting", {});
        break;
      case PACKET_TYPE.LOGIN_SUCCESS:
        this.emit("login_success", this.parseLoginSuccess(frame));
        break;
      case PACKET_TYPE.LOGIN_FAILED:
        this.emit("login_failed", this.parseLoginFailed(frame));
        break;
      case PACKET_TYPE.BINARY_RESPONSE:
      {
        const bin = this.parseBinaryResponse(frame);
        this.binaryQueue.push(bin);
        if (this.binaryQueue.length > this.maxBinaryQueue) {
          this.binaryQueue.splice(0, this.binaryQueue.length - this.maxBinaryQueue);
        }
        this.emit("binary_response", bin);
        break;
      }
      default:
        this.emit("raw", { packet_type: packetType, data_hex: bytesToHex(frame) });
        break;
    }
  }

  parseSelfInfo(frame) {
    const payload = {
      public_key: "",
      name: "",
      radio_freq: null,
      radio_bw: null,
      radio_sf: null,
      radio_cr: null,
    };
    if (frame.length < 36) return payload;
    payload.public_key = bytesToHex(frame.slice(4, 36));
    if (frame.length >= 58) {
      payload.radio_freq = parseU32LE(frame, 48) / 1000.0;
      payload.radio_bw = parseU32LE(frame, 52) / 1000.0;
      payload.radio_sf = frame[56];
      payload.radio_cr = frame[57];
      payload.name = textDecoder.decode(frame.slice(58)).replace(/\0/g, "");
    } else if (frame.length > 55) {
      payload.name = textDecoder.decode(frame.slice(55)).replace(/\0/g, "");
    }
    return payload;
  }

  parseDeviceInfo(frame) {
    const payload = { fw_ver: frame.length > 1 ? frame[1] : null };
    return payload;
  }

  parseMsgSent(frame) {
    if (frame.length < 10) {
      return { type: null, expected_ack: "", suggested_timeout: 0 };
    }
    return {
      type: frame[1],
      expected_ack: bytesToHex(frame.slice(2, 6)),
      suggested_timeout: parseU32LE(frame, 6),
    };
  }

  parseContact(frame) {
    // Packet: type(1) + pub(32) + ctype(1) + flags(1) + out_path_len(i8) + out_path(64)
    //         + adv_name(32) + last_advert(4) + lat(4) + lon(4) + lastmod(4)
    if (!frame || frame.length < 148) {
      return { public_key: "" };
    }
    const outPathLenSigned = (frame[35] << 24) >> 24;
    return {
      public_key: bytesToHex(frame.slice(1, 33)),
      type: frame[33],
      flags: frame[34],
      out_path_len: outPathLenSigned,
      out_path: bytesToHex(frame.slice(36, 100)).replace(/(00)+$/i, ""),
      adv_name: textDecoder.decode(frame.slice(100, 132)).replace(/\0/g, ""),
      last_advert: parseU32LE(frame, 132),
      adv_lat: (parseU32LE(frame, 136) | 0) / 1e6,
      adv_lon: (parseU32LE(frame, 140) | 0) / 1e6,
      lastmod: parseU32LE(frame, 144),
    };
  }

  parseBinaryResponse(frame) {
    // Packet: 0x8c, reserved(1), tag(4), data...
    if (!frame || frame.length < 6) {
      return { tag: "", data_hex: "", data_bytes: new Uint8Array() };
    }
    const tag = bytesToHex(frame.slice(2, 6));
    const dataBytes = frame.slice(6);
    return {
      tag,
      data_hex: bytesToHex(dataBytes),
      data_bytes: dataBytes,
      text: textDecoder.decode(dataBytes).replace(/\0+$/g, ""),
    };
  }

  parseLoginSuccess(frame) {
    // Packet: 0x85, perms(1), pubkey_prefix(6)
    const payload = {
      permissions: frame.length > 1 ? frame[1] : 0,
      pubkey_prefix: "",
      is_admin: false,
    };
    if (frame.length >= 8) {
      payload.pubkey_prefix = bytesToHex(frame.slice(2, 8));
    }
    payload.is_admin = (payload.permissions & 0x01) === 0x01;
    return payload;
  }

  parseLoginFailed(frame) {
    // Packet: 0x86, reason?(1), pubkey_prefix?(6)
    const payload = {
      reason_code: frame.length > 1 ? frame[1] : null,
      pubkey_prefix: "",
    };
    if (frame.length >= 8) {
      payload.pubkey_prefix = bytesToHex(frame.slice(2, 8));
    }
    return payload;
  }

  parseContactMsg(frame, withSnr) {
    let offset = 1;
    const msg = {
      pubkey_prefix: "",
      path_len: 0,
      txt_type: 0,
      sender_timestamp: 0,
      text: "",
    };
    if (withSnr) {
      offset += 3; // snr + reserved
    }
    if (frame.length < offset + 12) return msg;
    msg.pubkey_prefix = bytesToHex(frame.slice(offset, offset + 6));
    offset += 6;
    msg.path_len = frame[offset] || 0;
    offset += 1;
    msg.txt_type = frame[offset] || 0;
    offset += 1;
    msg.sender_timestamp = parseU32LE(frame, offset);
    offset += 4;
    if (msg.txt_type === 2) offset += 4; // signature
    if (offset < frame.length) {
      msg.text = textDecoder.decode(frame.slice(offset));
    }
    return msg;
  }

  emit(type, payload) {
    if (this.onEvent) {
      try { this.onEvent(type, payload); } catch {}
    }
    const resolved = [];
    for (const waiter of this.waiters) {
      if (!waiter.types.has(type)) continue;
      if (waiter.predicate && !waiter.predicate(payload, type)) continue;
      clearTimeout(waiter.timer);
      waiter.resolve({ type, payload });
      resolved.push(waiter);
    }
    if (resolved.length > 0) {
      this.waiters = this.waiters.filter((w) => !resolved.includes(w));
    }
  }

  waitForEvent(types, predicate = null, timeoutMs = 5000) {
    const typeSet = new Set(Array.isArray(types) ? types : [types]);
    return new Promise((resolve, reject) => {
      const waiter = {
        types: typeSet,
        predicate,
        resolve,
        reject,
        timer: setTimeout(() => {
          this.waiters = this.waiters.filter((w) => w !== waiter);
          reject(new Error(`timeout waiting events: ${Array.from(typeSet).join(",")}`));
        }, timeoutMs),
      };
      this.waiters.push(waiter);
    });
  }

  async sendFrame(payload) {
    if (!this.connected) {
      throw new Error("non connecte");
    }
    if (!(payload instanceof Uint8Array)) {
      payload = new Uint8Array(payload);
    }
    if (this.transport === "web_ble") {
      await this.writeRaw(payload);
      return;
    }
    const frame = new Uint8Array(3 + payload.length);
    frame[0] = 0x3c;
    frame[1] = payload.length & 0xff;
    frame[2] = (payload.length >> 8) & 0xff;
    frame.set(payload, 3);
    await this.writeRaw(frame);
  }

  async writeRaw(bytes) {
    if (this.bleRxCharacteristic) {
      return this.enqueueWrite(async () => {
        if (
          this.bleRxCharacteristic.properties?.writeWithoutResponse
          && typeof this.bleRxCharacteristic.writeValueWithoutResponse === "function"
        ) {
          await this.bleRxCharacteristic.writeValueWithoutResponse(bytes);
          return;
        }
        if (
          this.bleRxCharacteristic.properties?.write
          && typeof this.bleRxCharacteristic.writeValueWithResponse === "function"
        ) {
          await this.bleRxCharacteristic.writeValueWithResponse(bytes);
          return;
        }
        await this.bleRxCharacteristic.writeValue(bytes);
      });
    }
    if (this.writer) {
      await this.writer.write(bytes);
      return;
    }
    if (this.usbDevice && this.usbOutEndpoint !== null) {
      let offset = 0;
      while (offset < bytes.length) {
        const end = Math.min(bytes.length, offset + this.usbOutPacketSize);
        const chunk = bytes.slice(offset, end);
        const res = await this.usbDevice.transferOut(this.usbOutEndpoint, chunk);
        if (!res || res.status !== "ok") {
          throw new Error(`usb transferOut status=${res?.status || "unknown"}`);
        }
        offset = end;
      }
      return;
    }
    throw new Error("transport ecriture indisponible");
  }

  enqueueWrite(task) {
    const next = this.writeChain.catch(() => {}).then(task);
    this.writeChain = next.catch(() => {});
    return next;
  }

  async sendCommand(payload, expectedTypes = [], predicate = null, timeoutMs = 5000) {
    const wantsReply = Array.isArray(expectedTypes) ? expectedTypes.length > 0 : Boolean(expectedTypes);
    const waiter = wantsReply ? this.waitForEvent(expectedTypes, predicate, timeoutMs) : null;
    await this.sendFrame(payload);
    if (!waiter) return null;
    return waiter;
  }

  async sendAppStart(timeoutMs = null) {
    const appName = textEncoder.encode("mccli-web");
    const payload = new Uint8Array(8 + appName.length);
    payload[0] = 0x01;
    payload[1] = 0x01;
    payload.set(appName, 8);
    const waitMs = timeoutMs ?? (this.transport === "web_ble" ? 12000 : 5000);
    return this.sendCommand(payload, ["self_info", "ok", "error"], null, waitMs);
  }

  async sendDeviceQuery(timeoutMs = null) {
    const waitMs = timeoutMs ?? (this.transport === "web_ble" ? 10000 : 5000);
    return this.sendCommand(Uint8Array.of(0x16, 0x03), ["device_info", "error"], null, waitMs);
  }

  async setLocalRadioParams(freqMhz, bwKhz, sf, cr, repeat = 0) {
    const norm = normalizeRadioPreset({ freq: freqMhz, bw: bwKhz, sf, cr });
    if (!norm) {
      return { ok: false, error: "parametres radio invalides" };
    }
    const freqKhz = Math.round(norm.freq * 1000.0);
    const bwHz = Math.round(norm.bw * 1000.0);
    const payload = concatBytes(
      Uint8Array.of(CMD_SET_RADIO_PARAMS),
      u32ToBytesLE(freqKhz),
      u32ToBytesLE(bwHz),
      Uint8Array.of(norm.sf & 0xff, norm.cr & 0xff, repeat ? 1 : 0)
    );
    const evt = await this.sendCommand(payload, ["ok", "error"], null, 5000);
    if (!evt || evt.type === "error") {
      const code = evt?.payload?.error_code;
      return { ok: false, error: `set radio error${code !== undefined ? ` (code=${code})` : ""}` };
    }
    return { ok: true };
  }

  normalizeTargetPrefix(targetHex) {
    const clean = normalizeHex(targetHex);
    if (clean.length < 12) {
      throw new Error("pubkey cible invalide (minimum 12 caracteres hex)");
    }
    return clean.slice(0, 12);
  }

  normalizeTargetFullKey(targetHex) {
    const clean = normalizeHex(targetHex);
    if (clean.length < 64) {
      throw new Error("cle publique complete requise (64 hex)");
    }
    return clean.slice(0, 64);
  }

  async requestContacts(timeoutMs = 10000) {
    const endPromise = this.waitForEvent(["contacts_end", "error"], null, timeoutMs);
    await this.sendFrame(Uint8Array.of(0x04));
    const evt = await endPromise;
    if (!evt || evt.type === "error") {
      const code = evt?.payload?.error_code;
      throw new Error(`get contacts error${code !== undefined ? ` (code=${code})` : ""}`);
    }
    return evt.payload.contacts || {};
  }

  async resolveTargetFullKeyForBinary(targetHex) {
    const clean = normalizeHex(targetHex);
    if (clean.length >= 64) {
      return clean.slice(0, 64);
    }
    if (clean.length < 12) {
      return null;
    }
    const prefix = clean.slice(0, 12);

    const resolveFromMap = (map) => {
      const keys = Object.keys(map || {}).filter((k) => k.startsWith(prefix));
      if (keys.length === 1) return keys[0];
      if (keys.length > 1) {
        throw new Error(`prefix ambigu (${keys.length} contacts)`);
      }
      return null;
    };

    let full = resolveFromMap(this.contactsByKey);
    if (full) return full;

    try {
      const contacts = await this.requestContacts();
      full = resolveFromMap(contacts);
      if (full) return full;
    } catch {
      // ignore and fallback
    }
    return null;
  }

  buildBinaryReqPayload(targetFullKeyHex, requestType, dataBody = new Uint8Array()) {
    const full = this.normalizeTargetFullKey(targetFullKeyHex);
    const dst = hexToBytes(full);
    const req = Uint8Array.of(requestType & 0xff);
    return concatBytes(Uint8Array.of(0x32), dst, req, dataBody);
  }

  async sendBinaryReqCustom(targetFullKeyHex, requestType, dataBody = new Uint8Array(), waitReply = true, timeoutMs = 0, minTimeoutMs = 200) {
    const payload = this.buildBinaryReqPayload(targetFullKeyHex, requestType, dataBody);
    const sentEvt = await this.sendCommand(payload, ["msg_sent", "error"], null, 5000);
    if (!sentEvt || sentEvt.type === "error") {
      const code = sentEvt?.payload?.error_code;
      return { replyText: null, error: `send error${code !== undefined ? ` (code=${code})` : ""}` };
    }
    if (!waitReply) {
      return { replyText: "", error: null };
    }

    const tag = String(sentEvt.payload?.expected_ack || "");
    const suggested = Number(sentEvt.payload?.suggested_timeout || 4000);
    const computed = Math.max(minTimeoutMs, Math.round((suggested / 800) * 1000));
    const waitMs = timeoutMs > 0 ? timeoutMs : computed;

    const queued = this.popQueuedBinary(tag);
    if (queued) {
      const text = String(queued.text || "").replace(/\0+$/g, "");
      return { replyText: text, error: null };
    }

    let responseEvt = null;
    try {
      responseEvt = await this.waitForEvent(
        "binary_response",
        (payload) => payload && payload.tag === tag,
        waitMs
      );
    } catch {
      responseEvt = null;
    }
    if (!responseEvt) {
      return { replyText: null, error: `timeout waiting binary reply` };
    }
    const text = String(responseEvt.payload?.text || "").replace(/\0+$/g, "");
    return { replyText: text, error: null };
  }

  async sendOtaBinaryCmd(targetFullKeyHex, opcode, payload = new Uint8Array(), waitReply = true, timeoutMs = 0, minTimeoutMs = 200) {
    const body = concatBytes(Uint8Array.of(opcode & 0xff), payload);
    return this.sendBinaryReqCustom(
      targetFullKeyHex,
      OTA_BINARY_REQ_TYPE,
      body,
      waitReply,
      timeoutMs,
      minTimeoutMs
    );
  }

  async sendLogin(targetFullKeyHex, password) {
    const full = this.normalizeTargetFullKey(targetFullKeyHex);
    const dst = hexToBytes(full);
    const pwd = textEncoder.encode(String(password || ""));
    const payload = concatBytes(Uint8Array.of(0x1a), dst, pwd);

    const sentEvt = await this.sendCommand(payload, ["msg_sent", "error"], null, 5000);
    if (!sentEvt || sentEvt.type === "error") {
      const code = sentEvt?.payload?.error_code;
      return { ok: false, error: `send login error${code !== undefined ? ` (code=${code})` : ""}` };
    }

    const expectedPrefix = full.slice(0, 12);
    const suggested = Number(sentEvt.payload?.suggested_timeout || 8000);
    const waitMs = Math.max(5000, Math.round(((suggested / 800) * 1.5) * 1000));

    let evt = null;
    try {
      evt = await this.waitForEvent(
        ["login_success", "login_failed", "error"],
        (payload, type) => {
          if (type === "error") return true;
          const pfx = String(payload?.pubkey_prefix || "").toLowerCase();
          return pfx === expectedPrefix;
        },
        waitMs
      );
    } catch {
      evt = null;
    }
    if (!evt) {
      return { ok: false, error: "timeout waiting login response" };
    }
    if (evt.type === "login_success") {
      return {
        ok: true,
        is_admin: Boolean(evt.payload?.is_admin),
        permissions: Number(evt.payload?.permissions || 0),
      };
    }
    if (evt.type === "login_failed") {
      const reason = evt.payload?.reason_code;
      return { ok: false, error: `login failed${reason !== null && reason !== undefined ? ` (reason=${reason})` : ""}` };
    }
    return { ok: false, error: "login error" };
  }

  buildSendCmdPayload(targetHex, commandText) {
    const prefix = this.normalizeTargetPrefix(targetHex);
    const dst = hexToBytes(prefix);
    const ts = u32ToBytesLE(Math.floor(Date.now() / 1000));
    const cmdBytes = textEncoder.encode(commandText);
    const payload = new Uint8Array(3 + 4 + 6 + cmdBytes.length);
    payload[0] = 0x02;
    payload[1] = 0x01;
    payload[2] = 0x00;
    payload.set(ts, 3);
    payload.set(dst, 7);
    payload.set(cmdBytes, 13);
    return payload;
  }

  async sendMeshCmd(targetHex, commandText, waitMsgSent = true) {
    const payload = this.buildSendCmdPayload(targetHex, commandText);
    if (!waitMsgSent) {
      await this.sendFrame(payload);
      return { type: "ok", payload: {} };
    }
    return this.sendCommand(payload, ["msg_sent", "error"], null, 5000);
  }

  popQueuedReply(pubkeyPrefix, token) {
    for (let i = 0; i < this.messageQueue.length; i += 1) {
      const msg = this.messageQueue[i];
      if (!msg || msg.pubkey_prefix !== pubkeyPrefix) continue;
      if (!String(msg.text || "").startsWith(token)) continue;
      this.messageQueue.splice(i, 1);
      return String(msg.text || "").slice(token.length);
    }
    return null;
  }

  popQueuedBinary(tag) {
    for (let i = 0; i < this.binaryQueue.length; i += 1) {
      const item = this.binaryQueue[i];
      if (!item || item.tag !== tag) continue;
      this.binaryQueue.splice(i, 1);
      return item;
    }
    return null;
  }

  purgeTokenReplies(pubkeyPrefix, token) {
    this.messageQueue = this.messageQueue.filter((msg) => {
      if (!msg || msg.pubkey_prefix !== pubkeyPrefix) return true;
      return !String(msg.text || "").startsWith(token);
    });
  }

  popLatestOffsetError(pubkeyPrefix) {
    const kept = [];
    let latest = null;
    for (const msg of this.messageQueue) {
      if (!msg || msg.pubkey_prefix !== pubkeyPrefix) {
        kept.push(msg);
        continue;
      }
      const offErr = parseOtaOffsetError(msg.text);
      if (!offErr) {
        kept.push(msg);
        continue;
      }
      if (!latest || offErr.expected >= latest.expected) {
        latest = { ...offErr, raw: msg.text };
      }
    }
    this.messageQueue = kept;
    return latest;
  }

  async waitRepeaterReply(targetHex, token, timeoutMs) {
    const pubkeyPrefix = this.normalizeTargetPrefix(targetHex);
    const tEnd = Date.now() + timeoutMs;

    const queued = this.popQueuedReply(pubkeyPrefix, token);
    if (queued !== null) return queued;

    while (Date.now() < tEnd) {
      const timeoutLeft = Math.max(120, Math.min(900, tEnd - Date.now()));
      let evt = null;
      try {
        evt = await this.sendCommand(
          Uint8Array.of(0x0a),
          ["contact_msg_recv", "no_more_msgs", "error"],
          null,
          timeoutLeft
        );
      } catch {
        evt = null;
      }

      if (evt && evt.type === "contact_msg_recv") {
        const msg = evt.payload || {};
        if (msg.pubkey_prefix === pubkeyPrefix && String(msg.text || "").startsWith(token)) {
          return String(msg.text || "").slice(token.length);
        }
      } else if (evt && evt.type === "no_more_msgs") {
        await sleep(70);
      } else if (evt && evt.type === "error") {
        await sleep(90);
      } else {
        await sleep(70);
      }

      const queuedAgain = this.popQueuedReply(pubkeyPrefix, token);
      if (queuedAgain !== null) return queuedAgain;
    }
    return null;
  }

  async sendRepeaterCmdAndWaitReply(targetHex, cmd, timeoutSec = 0) {
    const pubkeyPrefix = this.normalizeTargetPrefix(targetHex);
    const token = `${this.seq.toString(16).padStart(4, "0")}|`;
    this.seq = (this.seq + 1) & 0xffff;
    this.purgeTokenReplies(pubkeyPrefix, token);

    const sent = await this.sendMeshCmd(targetHex, `${token}${cmd}`, true);
    if (!sent || sent.type === "error") {
      const code = sent?.payload?.error_code;
      return { reply: null, error: `send error${code !== undefined ? ` (code=${code})` : ""}` };
    }

    const suggested = Number(sent.payload?.suggested_timeout || 8000);
    const defaultTimeoutMs = Math.max(8000, Math.round(((suggested / 800) * 1.5) * 1000));
    const replyTimeoutMs = timeoutSec > 0 ? Math.round(timeoutSec * 1000) : defaultTimeoutMs;
    const reply = await this.waitRepeaterReply(targetHex, token, replyTimeoutMs);
    if (reply === null) {
      return { reply: null, error: `timeout waiting reply to '${cmd}'` };
    }
    return { reply, error: null };
  }

  async sendRepeaterCmdNoReply(targetHex, cmd) {
    const token = `${this.seq.toString(16).padStart(4, "0")}|`;
    this.seq = (this.seq + 1) & 0xffff;
    const sent = await this.sendMeshCmd(targetHex, `${token}${cmd}`, true);
    if (!sent || sent.type === "error") {
      const code = sent?.payload?.error_code;
      return `send error${code !== undefined ? ` (code=${code})` : ""}`;
    }
    return null;
  }
}

const ui = {
  connectBtn: document.querySelector("#connectBtn"),
  disconnectBtn: document.querySelector("#disconnectBtn"),
  startOtaBtn: document.querySelector("#startOtaBtn"),
  cancelOtaBtn: document.querySelector("#cancelOtaBtn"),
  refreshTargetsBtn: document.querySelector("#refreshTargetsBtn"),
  connectionMode: document.querySelector("#connectionMode"),
  baudrate: document.querySelector("#baudrate"),
  targetSelect: document.querySelector("#targetSelect"),
  targetKey: document.querySelector("#targetKey"),
  targetPassword: document.querySelector("#targetPassword"),
  firmwareFile: document.querySelector("#firmwareFile"),
  chunkSize: document.querySelector("#chunkSize"),
  ackEvery: document.querySelector("#ackEvery"),
  noAckGap: document.querySelector("#noAckGap"),
  checkpointTimeout: document.querySelector("#checkpointTimeout"),
  autoTune: document.querySelector("#autoTune"),
  useTempRadio: document.querySelector("#useTempRadio"),
  tempRadioFreq: document.querySelector("#tempRadioFreq"),
  tempRadioBw: document.querySelector("#tempRadioBw"),
  tempRadioSf: document.querySelector("#tempRadioSf"),
  tempRadioCr: document.querySelector("#tempRadioCr"),
  tempRadioMins: document.querySelector("#tempRadioMins"),
  planLine: document.querySelector("#planLine"),
  connectionStatus: document.querySelector("#connectionStatus"),
  otaStatus: document.querySelector("#otaStatus"),
  metricsLine: document.querySelector("#metricsLine"),
  selfName: document.querySelector("#selfName"),
  selfKey: document.querySelector("#selfKey"),
  selfRadio: document.querySelector("#selfRadio"),
  progressBar: document.querySelector("#progressBar"),
  log: document.querySelector("#log"),
};

let client = null;
let otaRunning = false;
let otaCancelRequested = false;
let lastSelfInfo = null;
let lastPlanSummary = "";

function appendLog(line) {
  ui.log.value += `[${nowTime()}] ${line}\n`;
  ui.log.scrollTop = ui.log.scrollHeight;
}

function setConnectionStatus(text, ok = false) {
  ui.connectionStatus.innerHTML = ok ? `<strong>${text}</strong>` : text;
}

function setOtaStatus(text) {
  ui.otaStatus.textContent = text;
}

function setProgress(percent) {
  const p = Math.max(0, Math.min(100, Number(percent) || 0));
  ui.progressBar.style.width = `${p}%`;
}

function getTransportLabel(transport) {
  if (transport === "web_usb") return "WebUSB";
  if (transport === "web_ble") return "Web Bluetooth";
  return "Web Serial";
}

function updateConnectionModeUi() {
  const mode = String(ui.connectionMode?.value || "usb");
  const isBle = mode === "ble";
  if (ui.baudrate) ui.baudrate.disabled = isBle;
  if (ui.connectBtn) ui.connectBtn.textContent = isBle ? "Connecter BLE" : "Connecter USB";
}

function formatRadioPresetText(presetLike) {
  const preset = normalizeRadioPreset(presetLike);
  if (!preset) return "-";
  return `${formatRadioValue(preset.freq)} MHz | BW ${formatRadioValue(preset.bw, 1)} kHz | SF${preset.sf} | CR${preset.cr}`;
}

function targetKeyPreview(fullKey) {
  const clean = normalizeHex(fullKey);
  if (clean.length < 12) return clean || "-";
  return `${clean.slice(0, 12)}...`;
}

function getSelectedTargetHex() {
  const selected = normalizeHex(ui.targetSelect?.value || "");
  if (selected.length >= 12) return selected;
  const manual = normalizeHex(ui.targetKey?.value || "");
  if (manual.length >= 12) return manual;
  return "";
}

function resetTargetRepeaterSelect() {
  if (!ui.targetSelect) return;
  ui.targetSelect.innerHTML = "";
  const option = document.createElement("option");
  option.value = "";
  option.textContent = "Selectionner un repetiteur...";
  ui.targetSelect.appendChild(option);
  ui.targetSelect.value = "";
}

function isRepeaterContact(contact) {
  return Number(contact?.type) === CONTACT_TYPE.REPEATER;
}

function buildRepeaterOptionLabel(contact) {
  const name = String(contact?.adv_name || "").trim() || "Repeater";
  const key = normalizeHex(contact?.public_key || "");
  const suffix = key.length >= 12 ? key.slice(0, 12) : key;
  return `${name} (${suffix})`;
}

function updateTargetRepeaterSelect(contacts, keepCurrent = true) {
  if (!ui.targetSelect) return;
  const manual = normalizeHex(ui.targetKey?.value || "");
  const previous = keepCurrent
    ? normalizeHex(ui.targetSelect.value || manual)
    : "";

  const repeaters = Object.values(contacts || {})
    .filter((c) => isRepeaterContact(c) && normalizeHex(c.public_key || "").length >= 12)
    .sort((a, b) => {
      const la = Number(a.last_advert || 0);
      const lb = Number(b.last_advert || 0);
      if (lb !== la) return lb - la;
      const na = String(a.adv_name || "").toLowerCase();
      const nb = String(b.adv_name || "").toLowerCase();
      return na.localeCompare(nb);
    });

  resetTargetRepeaterSelect();
  for (const contact of repeaters) {
    const key = normalizeHex(contact.public_key || "");
    if (key.length < 12) continue;
    const option = document.createElement("option");
    option.value = key;
    option.textContent = buildRepeaterOptionLabel(contact);
    ui.targetSelect.appendChild(option);
  }

  const allOptionValues = Array.from(ui.targetSelect.options).map((opt) => normalizeHex(opt.value || ""));
  const selected =
    allOptionValues.find((k) => previous.length >= 12 && (k === previous || k.startsWith(previous) || previous.startsWith(k)))
    || "";
  ui.targetSelect.value = selected;
  if (!manual && selected && ui.targetKey) {
    ui.targetKey.value = selected;
  }
}

function updateLiveMetrics(transport, doneBytes, totalBytes, startTs, attempts, sendFailures, serverRejects = 0) {
  const pct = totalBytes > 0 ? Math.floor((doneBytes * 100) / totalBytes) : 0;
  const elapsedSec = Math.max(0.001, (performance.now() - startTs) / 1000.0);
  const rateBps = doneBytes / elapsedSec;
  const remainingBytes = Math.max(0, totalBytes - doneBytes);
  const etaSec = rateBps > 0 ? (remainingBytes / rateBps) : NaN;
  const sendFailureRatePct = attempts > 0 ? (sendFailures * 100.0 / attempts) : 0.0;
  const serverRejectRatePct = attempts > 0 ? (serverRejects * 100.0 / attempts) : 0.0;
  const totalRejects = sendFailures + serverRejects;
  const totalRejectRatePct = attempts > 0 ? (totalRejects * 100.0 / attempts) : 0.0;
  const tr = transport === "binary_req" ? "binary" : "text";

  if (!Number.isFinite(etaSec) || doneBytes <= 0) {
    ui.metricsLine.textContent =
      `Transport ${tr} | ${pct}% | Estimation en cours... | Rejets ${totalRejects}/${attempts} (${totalRejectRatePct.toFixed(2)}%)`;
    return;
  }

  ui.metricsLine.textContent =
    `Transport ${tr} | ${pct}% | Restant ~${formatDuration(etaSec)} | `
    + `Rejets ${totalRejects}/${attempts} (${totalRejectRatePct.toFixed(2)}%) `
    + `[send ${sendFailures}/${attempts} ${sendFailureRatePct.toFixed(2)}%, `
    + `srv ${serverRejects}/${attempts} ${serverRejectRatePct.toFixed(2)}%]`;
}

function updateButtons() {
  const connected = Boolean(client && client.connected);
  ui.connectBtn.disabled = connected || otaRunning;
  ui.disconnectBtn.disabled = !connected || otaRunning;
  ui.startOtaBtn.disabled = !connected || otaRunning;
  ui.cancelOtaBtn.disabled = !otaRunning;
  if (ui.refreshTargetsBtn) ui.refreshTargetsBtn.disabled = !connected || otaRunning;
  if (ui.targetSelect) ui.targetSelect.disabled = !connected || otaRunning;
  if (ui.targetKey) ui.targetKey.disabled = otaRunning;
  if (ui.useTempRadio) {
    ui.useTempRadio.disabled = otaRunning;
  }
  updateTempRadioInputsState();
}

function updateTuneInputsState() {
  const auto = Boolean(ui.autoTune?.checked);
  if (ui.chunkSize) ui.chunkSize.disabled = auto;
  if (ui.ackEvery) ui.ackEvery.disabled = auto;
  if (ui.noAckGap) ui.noAckGap.disabled = auto;
  if (ui.checkpointTimeout) ui.checkpointTimeout.disabled = auto;
}

function updateTempRadioInputsState() {
  const useTemp = Boolean(ui.useTempRadio?.checked) && !otaRunning;
  if (ui.tempRadioFreq) ui.tempRadioFreq.disabled = !useTemp;
  if (ui.tempRadioBw) ui.tempRadioBw.disabled = !useTemp;
  if (ui.tempRadioSf) ui.tempRadioSf.disabled = !useTemp;
  if (ui.tempRadioCr) ui.tempRadioCr.disabled = !useTemp;
  if (ui.tempRadioMins) ui.tempRadioMins.disabled = !useTemp;
}

function updatePlanLine(logToConsole = true) {
  const file = ui.firmwareFile?.files && ui.firmwareFile.files[0];
  const fwSize = file ? Number(file.size || 0) : 0;
  const chunkInput = Number.parseInt(ui.chunkSize?.value || "0", 10) || 0;
  const ackEvery = Number.parseInt(ui.ackEvery?.value || "0", 10) || 0;
  let planText = "Plan OTA: selectionne un firmware";

  if (!file || fwSize <= 0 || chunkInput < 1) {
    if (ui.planLine) ui.planLine.textContent = planText;
    return planText;
  }

  const binaryChunk = clamp(chunkInput, 16, OTA_BINARY_MAX_CHUNK);
  const binaryChunks = Math.ceil(fwSize / binaryChunk);

  const safeTextLimit = maxOtaChunkForOffset(Math.max(0, fwSize - 1));
  if (safeTextLimit < 16) {
    planText = `Plan OTA: binaire ~${binaryChunks} chunks (chunk ${binaryChunk}), fallback texte indisponible`;
    if (ui.planLine) ui.planLine.textContent = planText;
    if (logToConsole && planText !== lastPlanSummary) {
      appendLog(planText);
      lastPlanSummary = planText;
    }
    return planText;
  }
  const textChunk = Math.min(binaryChunk, 80, safeTextLimit);
  const textChunks = estimateOtaChunkCount(fwSize, textChunk);
  planText =
    `Plan OTA: binaire ~${binaryChunks} chunks (chunk ${binaryChunk}, ack ${ackEvery || 1}) | `
    + `fallback texte ~${textChunks} chunks (chunk ${textChunk})`;
  if (ui.planLine) ui.planLine.textContent = planText;
  if (logToConsole && planText !== lastPlanSummary) {
    appendLog(planText);
    lastPlanSummary = planText;
  }
  return planText;
}

function applyAutoOtaSettings(logReason = null) {
  if (!ui.autoTune?.checked) return null;
  const tuning = computeAutoOtaSettings(lastSelfInfo);

  ui.chunkSize.value = String(tuning.chunkSize);
  ui.ackEvery.value = String(tuning.ackEvery);
  ui.noAckGap.value = String(tuning.noAckGapMs);
  ui.checkpointTimeout.value = String(tuning.checkpointTimeoutMs);
  updatePlanLine();

  if (logReason) {
    if (tuning.profile) {
      appendLog(
        `Auto tuning (${logReason}): chunk=${tuning.chunkSize} ack=${tuning.ackEvery} `
        + `gap=${tuning.noAckGapMs}ms timeout=${tuning.checkpointTimeoutMs}ms `
        + `| BW=${tuning.profile.bwKhz.toFixed(1)} SF=${tuning.profile.sf} CR=${tuning.profile.cr}`
      );
    } else {
      appendLog(
        `Auto tuning (${logReason}): chunk=${tuning.chunkSize} ack=${tuning.ackEvery} `
        + `gap=${tuning.noAckGapMs}ms timeout=${tuning.checkpointTimeoutMs}ms (fallback)`
      );
    }
  }
  return tuning;
}

function applyAutoOtaSettingsForPreset(preset, logReason = null) {
  const norm = normalizeRadioPreset(preset);
  if (!norm) {
    throw new Error("Preset radio invalide pour auto tuning");
  }
  const profile = estimateOtaRadioProfile(norm.bw, norm.sf, norm.cr);
  const tuning = computeAutoOtaSettingsFromProfile(profile);

  ui.chunkSize.value = String(tuning.chunkSize);
  ui.ackEvery.value = String(tuning.ackEvery);
  ui.noAckGap.value = String(tuning.noAckGapMs);
  ui.checkpointTimeout.value = String(tuning.checkpointTimeoutMs);
  updatePlanLine(true);

  if (logReason) {
    appendLog(
      `Auto tuning (${logReason}): chunk=${tuning.chunkSize} ack=${tuning.ackEvery} `
      + `gap=${tuning.noAckGapMs}ms timeout=${tuning.checkpointTimeoutMs}ms `
      + `| BW=${norm.bw.toFixed(1)} SF=${norm.sf} CR=${norm.cr}`
    );
  }
  return tuning;
}

function recalcAutoFromCurrentSelection(logReason = null) {
  if (!ui.autoTune?.checked) return null;
  if (ui.useTempRadio?.checked) {
    try {
      const preset = readTempRadioPresetFromUi();
      return applyAutoOtaSettingsForPreset(preset, logReason);
    } catch (e) {
      if (logReason) {
        appendLog(`Auto tuning ignore (preset OTA invalide): ${e.message}`);
      }
      return null;
    }
  }
  return applyAutoOtaSettings(logReason);
}

async function getFirmwareBytes(file) {
  const arrayBuffer = await file.arrayBuffer();
  return new Uint8Array(arrayBuffer);
}

async function getOtaStatus(targetHex, timeoutSec = 10) {
  const maxAttempts = 2;
  let lastErr = null;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const res = await client.sendRepeaterCmdAndWaitReply(targetHex, "ota status", timeoutSec);
    if (!res.error) {
      return { reply: res.reply, status: parseOtaStatus(res.reply), error: null };
    }
    lastErr = res.error;
    if (attempt + 1 < maxAttempts) await sleep(200 * (attempt + 1));
  }
  return { reply: null, status: null, error: lastErr };
}

async function getOtaStatusBinary(targetFullKeyHex, maxAttempts = 2, timeoutMs = 4000) {
  let lastErr = null;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const res = await client.sendOtaBinaryCmd(
      targetFullKeyHex,
      OTA_BIN_OP.STATUS,
      new Uint8Array(),
      true,
      timeoutMs,
      250
    );
    if (!res.error) {
      return { reply: res.replyText, status: parseOtaStatus(res.replyText), error: null };
    }
    lastErr = res.error;
    if (attempt + 1 < maxAttempts) await sleep(150 * (attempt + 1));
  }
  return { reply: null, status: null, error: lastErr };
}

async function runBinaryOta(params) {
  const {
    targetHex,
    targetFullKeyHex,
    firmware,
    firmwareMd5,
    chunkSizeInput,
    ackEveryInput,
    noAckGapMs,
    checkpointTimeoutMs,
    statusTimeoutMs,
    radioProfile,
  } = params;

  if (chunkSizeInput < 16 || chunkSizeInput > OTA_BINARY_MAX_CHUNK) {
    throw new Error(`chunk_size doit etre entre 16 et ${OTA_BINARY_MAX_CHUNK} en mode binaire`);
  }
  if (ackEveryInput < 1 || ackEveryInput > 64) {
    throw new Error("ack_every doit etre entre 1 et 64");
  }
  if (!firmware || firmware.length === 0) {
    throw new Error("firmware vide");
  }
  if (!/^[0-9a-f]{32}$/i.test(String(firmwareMd5 || ""))) {
    throw new Error("md5 firmware invalide");
  }

  const fwSize = firmware.length;
  let chunkSize = Math.min(chunkSizeInput, OTA_BINARY_MAX_CHUNK);
  const ackEvery = ackEveryInput;
  const totalChunks = Math.ceil(fwSize / chunkSize);

  appendLog(`Transport: binary req`);
  appendLog(`Firmware: ${fwSize} bytes (md5=${firmwareMd5})`);
  appendLog(`Chunk size: ${chunkSize} bytes (${totalChunks} chunks)`);
  appendLog(`Ack every: ${ackEvery} chunk(s)`);
  appendLog("Checkpoint mode: status");
  if (radioProfile) {
    appendLog(
      `Radio profile: BW=${radioProfile.bwKhz.toFixed(1)}kHz SF=${radioProfile.sf} `
      + `CR=${radioProfile.cr} factor=${radioProfile.airFactor.toFixed(2)}`
    );
  }

  const tStart = performance.now();
  const statusTimeout = Math.max(500, Number(statusTimeoutMs) || 4000);
  const quickStatusTimeout = Math.max(450, Math.min(2000, Math.round(statusTimeout * 0.55)));
  let offset = 0;
  let chunkIndex = 0;
  let chunksSinceAck = 0;
  let chunkWriteAttempts = 0;
  let chunkWriteFailures = 0;
  let chunkServerRejects = 0;
  let lastProgressPct = -1;
  const writeMaxRetries = 3;
  const checkpointMaxRetries = 10;
  updateLiveMetrics("binary_req", offset, fwSize, tStart, chunkWriteAttempts, chunkWriteFailures, chunkServerRejects);

  const startRes = await client.sendOtaBinaryCmd(
    targetFullKeyHex,
    OTA_BIN_OP.START,
    new Uint8Array(),
    true,
    8000,
    500
  );

  if (startRes.error) {
    if (startRes.error.startsWith("timeout waiting binary reply")) {
      appendLog("Binary OTA non disponible (pas de reponse binaire).");
      return null;
    }
    throw new Error(`Start OTA failed (binary): ${startRes.error}`);
  }

  const startReply = String(startRes.replyText || "");
  if (!startReply.toLowerCase().startsWith("ok")) {
    const low = startReply.toLowerCase();
    if (low.includes("already running")) {
      const st = await getOtaStatusBinary(targetFullKeyHex, 2, statusTimeout);
      if (st.error) {
        throw new Error(`Start OTA failed (binary): ${startReply}; ${st.error}`);
      }
      if (st.status && st.status.total === fwSize && st.status.done <= fwSize) {
        offset = st.status.done;
        chunkIndex = Math.floor(offset / chunkSize);
        appendLog(`Resuming OTA session at offset ${offset}/${fwSize}`);
      } else {
        throw new Error(`Start OTA failed (binary): ${startReply}; status=${st.reply}`);
      }
    } else if (low.includes("unknown") || low.includes("unsupported")) {
      appendLog("Binary OTA unsupported by target.");
      return null;
    } else {
      throw new Error(`Start OTA failed (binary): ${startReply}`);
    }
  }

  if (offset === 0) {
    const md5Bytes = hexToBytes(firmwareMd5);
    if (md5Bytes.length !== 16) {
      throw new Error("md5 firmware invalide (taille)");
    }
    const beginPayload = concatBytes(
      u32ToBytesLE(fwSize),
      Uint8Array.of(ackEvery & 0xff),
      md5Bytes
    );
    const beginRes = await client.sendOtaBinaryCmd(
      targetFullKeyHex,
      OTA_BIN_OP.BEGIN,
      beginPayload,
      true,
      8000,
      500
    );
    if (beginRes.error) {
      throw new Error(`OTA begin failed (binary): ${beginRes.error}`);
    }
    if (!String(beginRes.replyText || "").toLowerCase().startsWith("ok")) {
      const rep = String(beginRes.replyText || "");
      const low = rep.toLowerCase();
      if (low.includes("unsupported") || low.includes("unknown")) {
        appendLog("Binary OTA begin unsupported by target.");
        return null;
      }
      throw new Error(`OTA begin failed (binary): ${rep}`);
    }
  }

  while (offset < fwSize) {
    if (otaCancelRequested) {
      await client.sendOtaBinaryCmd(
        targetFullKeyHex,
        OTA_BIN_OP.ABORT,
        new Uint8Array(),
        false,
        0,
        100
      );
      throw new Error("OTA annulee.");
    }

    const chunkLen = Math.min(chunkSize, fwSize - offset);
    const chunk = firmware.subarray(offset, offset + chunkLen);
    const isLastChunk = offset + chunkLen >= fwSize;
    const checkpointDue = (chunksSinceAck + 1 >= ackEvery) || isLastChunk;
    const writePayload = concatBytes(
      u32ToBytesLE(offset),
      Uint8Array.of(chunkLen & 0xff),
      chunk
    );

    const writeOffset = offset;
    let writeOk = false;
    let failure = "write failed";

    for (let attempt = 1; attempt <= writeMaxRetries; attempt += 1) {
      chunkWriteAttempts += 1;
      const writeRes = await client.sendOtaBinaryCmd(
        targetFullKeyHex,
        OTA_BIN_OP.WRITE,
        writePayload,
        false,
        250,
        100
      );
      if (!writeRes.error) {
        writeOk = true;
        break;
      }

      chunkWriteFailures += 1;
      failure = writeRes.error || failure;

      // On send error, query status quickly and resync if possible.
      const st = await getOtaStatusBinary(targetFullKeyHex, 1, quickStatusTimeout);
      if (st.status && st.status.total === fwSize && st.status.done <= fwSize) {
        const srvOffset = st.status.done;
        if (srvOffset !== offset) {
          chunkServerRejects += estimateRejectedChunks(offset, srvOffset, chunkSize);
          appendLog(`Resync offset to ${srvOffset}/${fwSize}`);
          offset = srvOffset;
          chunkIndex = Math.floor(offset / chunkSize);
          chunksSinceAck = 0;
          writeOk = true;
          break;
        }
      }

      if (attempt < writeMaxRetries) {
        appendLog(`Retry write (binary) at offset ${offset} (${attempt}/${writeMaxRetries})`);
        await sleep(120 * attempt);
        continue;
      }

      if (!st.error && st.reply) {
        failure = `${failure}; status=${st.reply}`;
      }
    }

    if (!writeOk) {
      throw new Error(`OTA write failed (binary) at offset ${offset}: ${failure}`);
    }

    // If no status-driven resync happened, advance local cursor.
    if (offset === writeOffset) {
      offset += chunkLen;
      chunkIndex += 1;
      chunksSinceAck += 1;
      if (!checkpointDue && noAckGapMs > 0) await sleep(noAckGapMs);
    } else {
      // Resynced to server offset; continue from there.
      continue;
    }

    if (checkpointDue) {
      let checkpointOk = false;
      failure = "checkpoint status missing";

      for (let attempt = 1; attempt <= checkpointMaxRetries; attempt += 1) {
        const st = await getOtaStatusBinary(
          targetFullKeyHex,
          1,
          Math.max(500, checkpointTimeoutMs)
        );
        if (st.status && st.status.total === fwSize && st.status.done <= fwSize) {
          if (st.status.done !== offset) {
            chunkServerRejects += estimateRejectedChunks(offset, st.status.done, chunkSize);
            appendLog(`Resync offset to ${st.status.done}/${fwSize}`);
          }
          offset = st.status.done;
          chunkIndex = Math.floor(offset / chunkSize);
          chunksSinceAck = 0;
          checkpointOk = true;
          break;
        }

        failure = st.error || (st.reply ? `status=${st.reply}` : "invalid status response");
        if (attempt < checkpointMaxRetries) {
          appendLog(`Retry checkpoint status (binary) at offset ${offset} (${attempt}/${checkpointMaxRetries})`);
          await sleep(120 * attempt);
        }
      }

      if (!checkpointOk) {
        throw new Error(`OTA checkpoint failed (binary) at offset ${offset}: ${failure}`);
      }
    }

    const pct = Math.floor((offset * 100) / fwSize);
    if (pct !== lastProgressPct && (pct % 2 === 0 || offset === fwSize)) {
      setProgress(pct);
      setOtaStatus(`Progress: ${pct}% (${chunkIndex}/${totalChunks})`);
      updateLiveMetrics(
        "binary_req",
        offset,
        fwSize,
        tStart,
        chunkWriteAttempts,
        chunkWriteFailures,
        chunkServerRejects
      );
      lastProgressPct = pct;
    }
  }

  const endRes = await client.sendOtaBinaryCmd(
    targetFullKeyHex,
    OTA_BIN_OP.END,
    new Uint8Array(),
    true,
    12000,
    500
  );
  if (endRes.error) {
    throw new Error(`OTA end failed (binary): ${endRes.error}`);
  }
  if (!String(endRes.replyText || "").toLowerCase().startsWith("ok")) {
    throw new Error(`OTA end failed (binary): ${endRes.replyText}`);
  }

  await client.sendRepeaterCmdNoReply(targetHex, "reboot");

  const durationSec = (performance.now() - tStart) / 1000.0;
  const sendFailureRatePct = chunkWriteAttempts > 0
    ? (chunkWriteFailures * 100.0) / chunkWriteAttempts
    : 0;
  const serverRejectRatePct = chunkWriteAttempts > 0
    ? (chunkServerRejects * 100.0) / chunkWriteAttempts
    : 0;
  const totalRejects = chunkWriteFailures + chunkServerRejects;
  const failureRatePct = chunkWriteAttempts > 0
    ? (totalRejects * 100.0) / chunkWriteAttempts
    : 0;

  return {
    transport: "binary_req",
    fwSize,
    totalChunks,
    chunkSize,
    ackEvery,
    durationSec,
    chunkWriteAttempts,
    chunkWriteFailures,
    chunkServerRejects,
    totalRejects,
    sendFailureRatePct,
    serverRejectRatePct,
    failureRatePct,
  };
}

async function runTextOta(params) {
  const {
    targetHex,
    firmware,
    firmwareMd5,
    chunkSizeInput,
    ackEveryInput,
    noAckGapMs,
    checkpointTimeoutMs,
    statusTimeoutMs,
    ackSettleGapMs,
    radioProfile,
  } = params;

  if (chunkSizeInput < 16 || chunkSizeInput > 80) {
    throw new Error("chunk_size doit etre entre 16 et 80 en mode texte");
  }
  if (ackEveryInput < 1 || ackEveryInput > 64) {
    throw new Error("ack_every doit etre entre 1 et 64");
  }
  if (!firmware || firmware.length === 0) {
    throw new Error("firmware vide");
  }
  if (!/^[0-9a-f]{32}$/i.test(String(firmwareMd5 || ""))) {
    throw new Error("md5 firmware invalide");
  }

  const targetPrefix = client.normalizeTargetPrefix(targetHex);
  const fwSize = firmware.length;
  let chunkSize = chunkSizeInput;
  let ackEvery = ackEveryInput;

  const safeChunkLimit = maxOtaChunkForOffset(Math.max(0, fwSize - 1));
  if (safeChunkLimit < 16) {
    throw new Error(`transport texte trop limite pour OTA (chunk max securise ${safeChunkLimit})`);
  }
  if (chunkSize > safeChunkLimit) {
    appendLog(`Chunk size ajuste de ${chunkSize} a ${safeChunkLimit} (limite transport texte).`);
    chunkSize = safeChunkLimit;
  }

  const totalChunks = estimateOtaChunkCount(fwSize, chunkSize);
  if (totalChunks <= 0) {
    throw new Error("impossible de calculer le plan de chunks");
  }

  appendLog(`OTA target prefix: ${targetPrefix}`);
  appendLog(`Firmware: ${fwSize} bytes (md5=${firmwareMd5})`);
  appendLog(`Chunk size: ${chunkSize} bytes (${totalChunks} chunks)`);
  appendLog(`Ack every: ${ackEvery} chunk(s)`);
  if (radioProfile) {
    appendLog(
      `Radio profile: BW=${radioProfile.bwKhz.toFixed(1)}kHz SF=${radioProfile.sf} `
      + `CR=${radioProfile.cr} factor=${radioProfile.airFactor.toFixed(2)}`
    );
  }

  const statusTimeoutSec = clamp((Number(statusTimeoutMs) || 4000) / 1000.0, 2.0, 15.0);
  const startTimeoutSec = clamp(Math.max(8.0, statusTimeoutSec * 2.2), 8.0, 25.0);
  const beginTimeoutSec = clamp(Math.max(10.0, statusTimeoutSec * 2.8), 10.0, 30.0);
  const endTimeoutSec = clamp(Math.max(12.0, statusTimeoutSec * 3.0), 12.0, 35.0);
  const writeAckTimeoutSec = clamp(Math.max(5.0, (checkpointTimeoutMs / 1000.0) * 1.6), 5.0, 15.0);

  const tStart = performance.now();
  let offset = 0;
  let chunkIndex = 0;
  let chunksSinceAck = 0;
  let chunkWriteAttempts = 0;
  let chunkWriteFailures = 0;
  let chunkServerRejects = 0;
  let lastProgressPct = -1;
  updateLiveMetrics("text_cmd", offset, fwSize, tStart, chunkWriteAttempts, chunkWriteFailures, chunkServerRejects);

  const startRes = await client.sendRepeaterCmdAndWaitReply(targetHex, "start ota", startTimeoutSec);
  if (startRes.error) {
    throw new Error(`Start OTA failed: ${startRes.error}`);
  }

  let startReply = String(startRes.reply || "");
  if (!startReply.toLowerCase().startsWith("ok")) {
    if (startReply.toLowerCase().includes("already running")) {
      const st = await getOtaStatus(targetHex, statusTimeoutSec);
      if (st.error) {
        throw new Error(`Start OTA failed: ${startReply}; ${st.error}`);
      }
      if (st.status && st.status.total === fwSize && st.status.done <= fwSize) {
        offset = st.status.done;
        chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
        appendLog(`Resuming OTA session at offset ${offset}/${fwSize}`);
      } else {
        throw new Error(`Start OTA failed: ${startReply}; status=${st.reply}`);
      }
    } else {
      throw new Error(`Start OTA failed: ${startReply}`);
    }
  }

  if (offset === 0) {
    let beginCmd = `ota begin ${fwSize} ${firmwareMd5} ${ackEvery}`;
    let beginRes = await client.sendRepeaterCmdAndWaitReply(targetHex, beginCmd, beginTimeoutSec);
    if (!beginRes.error && !String(beginRes.reply || "").toLowerCase().startsWith("ok") && ackEvery > 1) {
      const r = String(beginRes.reply || "").toLowerCase();
      if (r.includes("too many args") || r.includes("bad ack")) {
        appendLog("Target ne supporte pas ack_every sur begin, fallback a ack_every=1.");
        ackEvery = 1;
        beginCmd = `ota begin ${fwSize} ${firmwareMd5}`;
        beginRes = await client.sendRepeaterCmdAndWaitReply(targetHex, beginCmd, beginTimeoutSec);
      }
    }
    if (beginRes.error) {
      throw new Error(`OTA begin failed: ${beginRes.error}`);
    }
    if (!String(beginRes.reply || "").toLowerCase().startsWith("ok")) {
      throw new Error(`OTA begin failed: ${beginRes.reply}`);
    }
  }

  while (offset < fwSize) {
    if (otaCancelRequested) {
      await client.sendRepeaterCmdNoReply(targetHex, "ota abort");
      throw new Error("OTA annulee.");
    }

    const pendingOffsetErr = client.popLatestOffsetError(targetPrefix);
    if (pendingOffsetErr && pendingOffsetErr.expected !== offset && pendingOffsetErr.expected <= fwSize) {
      chunkWriteFailures += 1;
      chunkServerRejects += estimateRejectedChunks(offset, pendingOffsetErr.expected, chunkSize);
      offset = pendingOffsetErr.expected;
      chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
      chunksSinceAck = 0;
      appendLog(`Resync offset to ${offset}/${fwSize}`);
      continue;
    }

    const maxChunk = maxOtaChunkForOffset(offset);
    if (maxChunk < 1) {
      throw new Error(`OTA write failed at offset ${offset}: no room left for ota write command`);
    }

    const chunkLen = Math.min(chunkSize, maxChunk, fwSize - offset);
    const chunk = firmware.subarray(offset, offset + chunkLen);
    const chunkCmd = `ota write ${offset} ${bytesToHex(chunk)}`;
    const isLastChunk = offset + chunkLen >= fwSize;
    const checkpointDue = (chunksSinceAck + 1 >= ackEvery) || isLastChunk;

    if (!checkpointDue) {
      chunkWriteAttempts += 1;
      const sendErr = await client.sendRepeaterCmdNoReply(targetHex, chunkCmd);
      if (sendErr) {
        chunkWriteFailures += 1;
        const st = await getOtaStatus(targetHex, statusTimeoutSec);
        if (st.status && st.status.done !== offset) {
          chunkServerRejects += estimateRejectedChunks(offset, st.status.done, chunkSize);
          offset = st.status.done;
          chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
          chunksSinceAck = 0;
          appendLog(`Resync offset to ${offset}/${fwSize}`);
          continue;
        }
        const failure = st.error ? sendErr : `${sendErr}; status=${st.reply}`;
        throw new Error(`OTA write failed at offset ${offset}: ${failure}`);
      }
      offset += chunkLen;
      chunkIndex += 1;
      chunksSinceAck += 1;

      const postSendErr = client.popLatestOffsetError(targetPrefix);
      if (postSendErr && postSendErr.expected !== offset && postSendErr.expected <= fwSize) {
        chunkWriteFailures += 1;
        chunkServerRejects += estimateRejectedChunks(offset, postSendErr.expected, chunkSize);
        offset = postSendErr.expected;
        chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
        chunksSinceAck = 0;
        appendLog(`Resync offset to ${offset}/${fwSize}`);
        continue;
      }
      if (noAckGapMs > 0) await sleep(noAckGapMs);
    } else {
      let chunkSent = false;
      let failure = "checkpoint ack missing";
      const checkpointMaxRetries = 10;
      for (let attempt = 1; attempt <= checkpointMaxRetries; attempt += 1) {
        chunkWriteAttempts += 1;
        const timeoutSec = (ackEvery > 1 ? checkpointTimeoutMs / 1000.0 : writeAckTimeoutSec);
        const writeRes = await client.sendRepeaterCmdAndWaitReply(targetHex, chunkCmd, timeoutSec);
        const reply = String(writeRes.reply || "");
        const okReply = reply === "" || reply.toLowerCase().startsWith("ok");

        if (!writeRes.error && okReply) {
          offset += chunkLen;
          chunkIndex += 1;
          chunksSinceAck = 0;
          chunkSent = true;
          break;
        }

        chunkWriteFailures += 1;

        if (writeRes.error && ackEvery > 1 && writeRes.error.startsWith("timeout waiting reply")) {
          offset += chunkLen;
          chunkIndex += 1;
          chunksSinceAck = 0;
          chunkSent = true;
          break;
        }

        const offErr = !writeRes.error ? parseOtaOffsetError(reply) : null;
        if (offErr) {
          if (offErr.expected < offset + chunkLen) {
            chunkServerRejects += estimateRejectedChunks(offset + chunkLen, offErr.expected, chunkSize);
          }
          if (offErr.expected >= offset + chunkLen) {
            offset = offErr.expected;
            chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
            chunksSinceAck = 0;
            chunkSent = true;
            break;
          }
          if (offErr.expected < offset) {
            offset = offErr.expected;
            chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
            chunksSinceAck = 0;
            chunkSent = true;
            appendLog(`Resync offset to ${offset}/${fwSize}`);
            break;
          }
        }

        failure = writeRes.error || reply || failure;
        if (attempt < checkpointMaxRetries) {
          appendLog(`Retry checkpoint at offset ${offset} (${attempt}/${checkpointMaxRetries})`);
          await sleep(120 * attempt);
          continue;
        }

        const st = await getOtaStatus(targetHex, statusTimeoutSec);
        if (st.status && st.status.total === fwSize && st.status.done <= fwSize) {
          if (st.status.done >= offset + chunkLen) {
            offset = st.status.done;
            chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
            chunksSinceAck = 0;
            chunkSent = true;
            break;
          }
          if (st.status.done < offset) {
            chunkServerRejects += estimateRejectedChunks(offset, st.status.done, chunkSize);
            offset = st.status.done;
            chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
            chunksSinceAck = 0;
            chunkSent = true;
            appendLog(`Resync offset to ${offset}/${fwSize}`);
            break;
          }
        }
        if (!st.error && st.reply) {
          failure = `${failure}; status=${st.reply}`;
        }
      }
      if (!chunkSent) {
        throw new Error(`OTA write failed at offset ${offset}: ${failure}`);
      }
      if (ackSettleGapMs > 0) await sleep(ackSettleGapMs);
    }

    const pct = Math.floor((offset * 100) / fwSize);
    if (pct !== lastProgressPct && (pct % 2 === 0 || offset === fwSize)) {
      setProgress(pct);
      setOtaStatus(`Progress: ${pct}% (${chunkIndex}/${totalChunks})`);
      updateLiveMetrics(
        "text_cmd",
        offset,
        fwSize,
        tStart,
        chunkWriteAttempts,
        chunkWriteFailures,
        chunkServerRejects
      );
      lastProgressPct = pct;
    }
  }

  const endRes = await client.sendRepeaterCmdAndWaitReply(targetHex, "ota end", endTimeoutSec);
  if (endRes.error) {
    throw new Error(`OTA end failed: ${endRes.error}`);
  }
  if (!String(endRes.reply || "").toLowerCase().startsWith("ok")) {
    throw new Error(`OTA end failed: ${endRes.reply}`);
  }

  await client.sendRepeaterCmdNoReply(targetHex, "reboot");

  const durationSec = (performance.now() - tStart) / 1000.0;
  const sendFailureRatePct = chunkWriteAttempts > 0
    ? (chunkWriteFailures * 100.0) / chunkWriteAttempts
    : 0;
  const serverRejectRatePct = chunkWriteAttempts > 0
    ? (chunkServerRejects * 100.0) / chunkWriteAttempts
    : 0;
  const totalRejects = chunkWriteFailures + chunkServerRejects;
  const failureRatePct = chunkWriteAttempts > 0
    ? (totalRejects * 100.0) / chunkWriteAttempts
    : 0;

  return {
    transport: "text_cmd",
    fwSize,
    totalChunks,
    chunkSize,
    ackEvery,
    durationSec,
    chunkWriteAttempts,
    chunkWriteFailures,
    chunkServerRejects,
    totalRejects,
    sendFailureRatePct,
    serverRejectRatePct,
    failureRatePct,
  };
}

async function runOtaWithAutoTransport(params) {
  const targetFullKeyHex = await client.resolveTargetFullKeyForBinary(params.targetHex);
  if (targetFullKeyHex) {
    appendLog(`Binary candidate: ${targetFullKeyHex.slice(0, 12)}...`);
    const binaryRes = await runBinaryOta({
      ...params,
      targetFullKeyHex,
    });
    if (binaryRes !== null) {
      return binaryRes;
    }
    appendLog("Fallback OTA transport: text commands");
  } else {
    appendLog("Binary OTA non possible (cle complete non resolue), fallback texte.");
  }

  const textParams = { ...params };
  if (textParams.radioProfile && Number.isFinite(Number(textParams.radioProfile.chunkText))) {
    const profChunk = clamp(Number(textParams.radioProfile.chunkText), 16, 80);
    if (textParams.chunkSizeInput !== profChunk) {
      appendLog(`Fallback chunk size adjusted to ${profChunk} (radio profile text mode)`);
    }
    textParams.chunkSizeInput = profChunk;
  } else if (textParams.chunkSizeInput > 80) {
    appendLog("Fallback chunk size adjusted to 80 (text mode limit)");
    textParams.chunkSizeInput = 80;
  }
  return runTextOta(textParams);
}

function formatUsbDeviceLabel(device) {
  if (!device) return "USB";
  const vid = Number(device.vendorId || 0).toString(16).padStart(4, "0");
  const pid = Number(device.productId || 0).toString(16).padStart(4, "0");
  return `USB ${vid}:${pid}`;
}

function updateSelfInfoUi(selfInfo, logRadio = false) {
  if (selfInfo) {
    lastSelfInfo = selfInfo;
    ui.selfName.textContent = `Noeud: ${selfInfo.name || "-"}`;
    const pub = selfInfo.public_key || "";
    ui.selfKey.textContent = `PubKey: ${pub ? pub.slice(0, 12) : "-"}`;
    if (ui.selfRadio) {
      ui.selfRadio.textContent = `Radio: ${formatRadioPresetText({
        freq: selfInfo.radio_freq,
        bw: selfInfo.radio_bw,
        sf: selfInfo.radio_sf,
        cr: selfInfo.radio_cr,
      })}`;
    }
    if (
      logRadio
      && Number.isFinite(Number(selfInfo.radio_bw))
      && Number.isFinite(Number(selfInfo.radio_sf))
      && Number.isFinite(Number(selfInfo.radio_cr))
    ) {
      appendLog(
        `Preset radio: ${Number(selfInfo.radio_freq).toFixed(3)} MHz, `
        + `BW ${Number(selfInfo.radio_bw).toFixed(1)} kHz, `
        + `SF${Number(selfInfo.radio_sf)}, CR${Number(selfInfo.radio_cr)}`
      );
    }
    return;
  }
  lastSelfInfo = null;
  ui.selfName.textContent = "Noeud: inconnu";
  ui.selfKey.textContent = "PubKey: -";
  if (ui.selfRadio) ui.selfRadio.textContent = "Radio: -";
}

async function syncSelfInfoFromNode(logErrors = false) {
  let selfInfo = null;
  const appStartTimeoutMs = client?.transport === "web_ble" ? 12000 : 5000;
  const deviceQueryTimeoutMs = client?.transport === "web_ble" ? 10000 : 5000;

  if (client?.transport !== "web_ble") {
    try {
      await client.sendDeviceQuery(deviceQueryTimeoutMs);
    } catch (e) {
      if (logErrors) appendLog(`DEVICE_QUERY timeout: ${e.message}`);
    }
  }

  if (client?.transport === "web_ble") {
    await sleep(250);
  }

  try {
    const startEvt = await client.sendAppStart(appStartTimeoutMs);
    if (startEvt?.type === "self_info") {
      selfInfo = startEvt.payload;
    } else if (startEvt?.type === "error") {
      if (logErrors) appendLog(`APP_START error code=${startEvt.payload?.error_code}`);
    }
  } catch (e) {
    if (client?.transport === "web_ble") {
      if (logErrors) appendLog(`APP_START timeout: ${e.message} (retry)`);
      await sleep(400);
      try {
        const retryEvt = await client.sendAppStart(appStartTimeoutMs);
        if (retryEvt?.type === "self_info") {
          selfInfo = retryEvt.payload;
        } else if (retryEvt?.type === "error" && logErrors) {
          appendLog(`APP_START retry error code=${retryEvt.payload?.error_code}`);
        }
      } catch (retryErr) {
        if (logErrors) appendLog(`APP_START retry timeout: ${retryErr.message}`);
      }
    } else if (logErrors) {
      appendLog(`APP_START timeout: ${e.message}`);
    }
  }
  updateSelfInfoUi(selfInfo, false);
  return selfInfo;
}

async function refreshKnownRepeaters(logToConsole = false) {
  if (!client || !client.connected) {
    resetTargetRepeaterSelect();
    return {};
  }
  try {
    const contacts = await client.requestContacts();
    updateTargetRepeaterSelect(contacts, true);
    const repeaters = Object.values(contacts || {}).filter((c) => isRepeaterContact(c)).length;
    if (logToConsole) {
      appendLog(`Repetiteurs connus: ${repeaters}`);
    }
    return contacts;
  } catch (e) {
    resetTargetRepeaterSelect();
    if (logToConsole) appendLog(`Lecture contacts echouee: ${e.message}`);
    return {};
  }
}

async function initClientSession(baudrate) {
  const selfInfo = await syncSelfInfoFromNode(true);

  await refreshKnownRepeaters(true);

  updateSelfInfoUi(selfInfo, true);
  if (ui.autoTune?.checked) recalcAutoFromCurrentSelection("connect");

  const mode = getTransportLabel(client.transport);
  const speedSuffix = client.transport === "web_ble" ? "" : ` (${baudrate} bps)`;
  setConnectionStatus(`Connecte ${mode}${speedSuffix}`, true);
}

function readTempRadioPresetFromUi() {
  const preset = normalizeRadioPreset({
    freq: Number.parseFloat(ui.tempRadioFreq?.value || ""),
    bw: Number.parseFloat(ui.tempRadioBw?.value || ""),
    sf: Number.parseInt(ui.tempRadioSf?.value || "", 10),
    cr: Number.parseInt(ui.tempRadioCr?.value || "", 10),
  });
  if (!preset) {
    throw new Error("Preset OTA temporaire invalide (freq,bw,sf,cr)");
  }
  const timeoutMins = Number.parseInt(ui.tempRadioMins?.value || "", 10);
  if (!Number.isFinite(timeoutMins) || timeoutMins < 1 || timeoutMins > 240) {
    throw new Error("Duree temp invalide (1-240 minutes)");
  }
  return { ...preset, timeoutMins };
}

function applySelfRadioPresetToCache(preset) {
  if (!preset) return;
  if (!lastSelfInfo) lastSelfInfo = {};
  lastSelfInfo.radio_freq = preset.freq;
  lastSelfInfo.radio_bw = preset.bw;
  lastSelfInfo.radio_sf = preset.sf;
  lastSelfInfo.radio_cr = preset.cr;
}

async function applyTempRadioPresetForOta(targetHex, preset) {
  const oldSelfInfo = await syncSelfInfoFromNode(false);
  const previousLocalPreset = extractRadioPresetFromSelfInfo(oldSelfInfo || lastSelfInfo);
  if (!previousLocalPreset) {
    appendLog("Warning: preset radio client actuel indisponible, restauration finale impossible.");
  }

  const cmd = `tempradio ${formatRadioValue(preset.freq)},${formatRadioValue(preset.bw)},${preset.sf},${preset.cr},${preset.timeoutMins}`;
  appendLog(
    `Applying OTA temp preset: target=${formatRadioValue(preset.freq)}MHz/${formatRadioValue(preset.bw)}kHz/SF${preset.sf}/CR${preset.cr} `
    + `(${preset.timeoutMins} min)`
  );
  const sentEvt = await client.sendMeshCmd(targetHex, cmd, true);
  if (!sentEvt || sentEvt.type === "error") {
    const code = sentEvt?.payload?.error_code;
    throw new Error(`tempradio cible echoue: send error${code !== undefined ? ` (code=${code})` : ""}`);
  }
  appendLog("Target tempradio: commande envoyee (pas d'ACK attendu).");

  const suggested = Number(sentEvt.payload?.suggested_timeout || 0);
  const computedGuard = Number.isFinite(suggested) && suggested > 0
    ? Math.round((suggested / 800.0) * 1000.0 * 0.6)
    : 0;
  const dispatchGuardMs = clamp(computedGuard || 1200, 800, 2500);
  appendLog(`Attente ${dispatchGuardMs}ms pour laisser partir la commande tempradio...`);
  await sleep(dispatchGuardMs);

  const localRes = await client.setLocalRadioParams(preset.freq, preset.bw, preset.sf, preset.cr, 0);
  if (!localRes.ok) {
    throw new Error(`set radio client echoue: ${localRes.error}`);
  }
  applySelfRadioPresetToCache(preset);
  appendLog("Client radio preset bascule pour OTA.");

  const settleMs = 2400;
  appendLog(`Attente ${settleMs}ms pour synchroniser le changement radio...`);
  await sleep(settleMs);

  return previousLocalPreset;
}

async function restoreLocalRadioPresetAfterOta(previousPreset) {
  if (!previousPreset) return;
  appendLog(
    `Restauration preset client: ${formatRadioValue(previousPreset.freq)}MHz/${formatRadioValue(previousPreset.bw)}kHz/`
    + `SF${previousPreset.sf}/CR${previousPreset.cr}`
  );
  const res = await client.setLocalRadioParams(
    previousPreset.freq,
    previousPreset.bw,
    previousPreset.sf,
    previousPreset.cr,
    0
  );
  if (!res.ok) {
    throw new Error(res.error || "set radio restore error");
  }
  applySelfRadioPresetToCache(previousPreset);
  appendLog("Preset radio client restaure.");
}

function attachClientDebugHooks(c) {
  c.onEvent = (type, payload) => {
    if (type === "contact_msg_recv" && payload?.txt_type === 1) {
      const txt = String(payload.text || "").trim();
      if (txt.startsWith("[OTA]")) {
        appendLog(`RX OTA: ${payload.pubkey_prefix} ${txt}`);
      }
    }
  };
}

async function connectClientViaBle() {
  if (!("bluetooth" in navigator)) {
    throw new Error("Web Bluetooth non supporte par ce navigateur");
  }
  appendLog("Tentative de connexion Web Bluetooth...");
  const device = await navigator.bluetooth.requestDevice({
    filters: [{ services: [MESHCORE_BLE_SERVICE_UUID] }],
  });
  const c = new MeshCoreSerialClient(appendLog);
  attachClientDebugHooks(c);
  await c.connectBle(device);
  client = c;
  appendLog(`BLE connecte (${device.name || "MeshCore"}).`);
}

async function connectClientWithBestUsbTransport(baudrate) {
  const preference = getUsbBrowserPreference();
  let serialErr = null;
  let usbErr = null;

  if (preference === "webserial") {
    if (!("serial" in navigator)) {
      throw new Error("Web Serial non supporte par ce navigateur desktop");
    }
    appendLog("Plateforme detectee: desktop, Web Serial force.");
    appendLog("Tentative de connexion Web Serial...");
    const port = await navigator.serial.requestPort();
    const c = new MeshCoreSerialClient(appendLog);
    attachClientDebugHooks(c);
    await c.connectSerial(port, baudrate);
    client = c;
    appendLog("USB serie connecte via Web Serial.");
    return;
  }

  if (preference === "webusb") {
    if (!("usb" in navigator)) {
      throw new Error("WebUSB non supporte par ce navigateur Android");
    }
    appendLog("Plateforme detectee: Android, WebUSB force.");
    appendLog("Tentative de connexion WebUSB...");
    const device = await navigator.usb.requestDevice({ filters: WEBUSB_DEVICE_FILTERS });
    const c = new MeshCoreSerialClient(appendLog);
    attachClientDebugHooks(c);
    await c.connectWebUsb(device, baudrate);
    client = c;
    appendLog(`USB connecte via WebUSB (${formatUsbDeviceLabel(device)}).`);
    return;
  }

  if ("serial" in navigator) {
    try {
      appendLog("Tentative de connexion Web Serial...");
      const port = await navigator.serial.requestPort();
      const c = new MeshCoreSerialClient(appendLog);
      attachClientDebugHooks(c);
      await c.connectSerial(port, baudrate);
      client = c;
      appendLog("USB serie connecte via Web Serial.");
      return;
    } catch (e) {
      serialErr = e;
      appendLog(`Web Serial indisponible: ${e.message}`);
    }
  } else {
    appendLog("Web Serial non supporte par ce navigateur.");
  }

  if (!("usb" in navigator)) {
    throw serialErr || new Error("WebUSB non supporte par ce navigateur");
  }

  appendLog("Tentative de connexion WebUSB...");
  try {
    const device = await navigator.usb.requestDevice({ filters: WEBUSB_DEVICE_FILTERS });
    const c = new MeshCoreSerialClient(appendLog);
    attachClientDebugHooks(c);
    await c.connectWebUsb(device, baudrate);
    client = c;
    appendLog(`USB connecte via WebUSB (${formatUsbDeviceLabel(device)}).`);
  } catch (e) {
    usbErr = e;
    throw serialErr || usbErr;
  }
}

async function connectClientSelectedTransport(mode, baudrate) {
  if (mode === "ble") {
    await connectClientViaBle();
    return;
  }
  await connectClientWithBestUsbTransport(baudrate);
}

ui.connectBtn.addEventListener("click", async () => {
  if (otaRunning) return;

  try {
    const mode = String(ui.connectionMode?.value || "usb");
    if (mode === "ble") {
      if (!("bluetooth" in navigator)) {
        appendLog("Web Bluetooth non supporte par ce navigateur.");
        return;
      }
    } else if (!("serial" in navigator) && !("usb" in navigator)) {
      appendLog("Ni Web Serial ni WebUSB ne sont supportes par ce navigateur.");
      return;
    }
    const baudrate = Number.parseInt(ui.baudrate.value, 10) || 115200;
    await connectClientSelectedTransport(mode, baudrate);
    await initClientSession(baudrate);
  } catch (e) {
    appendLog(`Connexion impossible: ${e.message}`);
    setConnectionStatus("Connexion echouee");
    if (client) {
      await client.disconnect();
      client = null;
    }
  } finally {
    updateButtons();
  }
});

ui.disconnectBtn.addEventListener("click", async () => {
  if (otaRunning) return;
  if (client) {
    await client.disconnect();
    client = null;
  }
  lastSelfInfo = null;
  setConnectionStatus("Non connecte");
  ui.selfName.textContent = "Noeud: -";
  ui.selfKey.textContent = "PubKey: -";
  if (ui.selfRadio) ui.selfRadio.textContent = "Radio: -";
  resetTargetRepeaterSelect();
  if (ui.targetKey) ui.targetKey.value = "";
  if (ui.autoTune?.checked) applyAutoOtaSettings("disconnect");
  appendLog("Deconnecte.");
  updateButtons();
});

ui.cancelOtaBtn.addEventListener("click", () => {
  if (!otaRunning) return;
  otaCancelRequested = true;
  setOtaStatus("Annulation demandee...");
  appendLog("Annulation OTA demandee.");
});

if (ui.targetSelect) {
  ui.targetSelect.addEventListener("change", () => {
    const selected = normalizeHex(ui.targetSelect.value || "");
    if (selected.length >= 12 && ui.targetKey) {
      ui.targetKey.value = selected;
    }
    if (selected.length >= 12) {
      appendLog(`Cible OTA selectionnee: ${targetKeyPreview(selected)}`);
    }
  });
}

if (ui.refreshTargetsBtn) {
  ui.refreshTargetsBtn.addEventListener("click", async () => {
    if (!client || !client.connected || otaRunning) return;
    await refreshKnownRepeaters(true);
  });
}

ui.startOtaBtn.addEventListener("click", async () => {
  if (!client || !client.connected) {
    appendLog("Pas de connexion active.");
    return;
  }
  if (otaRunning) return;

  const file = ui.firmwareFile.files && ui.firmwareFile.files[0];
  if (!file) {
    appendLog("Selectionne un firmware (.bin/.bin.gz).");
    return;
  }

  let firmware;
  try {
    firmware = await getFirmwareBytes(file);
  } catch (e) {
    appendLog(`Lecture firmware echouee: ${e.message}`);
    return;
  }

  let firmwareMd5;
  try {
    firmwareMd5 = md5Hex(firmware);
  } catch (e) {
    appendLog(`Calcul MD5 echoue: ${e.message}`);
    return;
  }

  otaRunning = true;
  otaCancelRequested = false;
  updateButtons();
  setProgress(0);
  setOtaStatus("Demarrage OTA...");
  ui.metricsLine.textContent = "OTA en cours...";

  let restoreClientPreset = null;

  try {
    const selectedTargetHex = getSelectedTargetHex();
    if (selectedTargetHex.length < 12) {
      throw new Error("Selectionne un repetiteur cible ou renseigne une pubkey manuelle (>=12 hex).");
    }

    const autoSettings = ui.autoTune?.checked ? recalcAutoFromCurrentSelection("ota-start") : null;
    const manualChunk = Number.parseInt(ui.chunkSize.value, 10) || 64;
    const manualAck = Number.parseInt(ui.ackEvery.value, 10) || 1;
    const manualNoAckGap = Math.max(0, Number.parseInt(ui.noAckGap.value, 10) || 0);
    const manualCheckpoint = Math.max(1000, Number.parseInt(ui.checkpointTimeout.value, 10) || 1000);
    const manualStatusTimeout = Math.max(1200, Math.round(manualCheckpoint * 1.5));

    const params = {
      targetHex: selectedTargetHex,
      firmware,
      firmwareMd5,
      chunkSizeInput: autoSettings ? autoSettings.chunkSize : manualChunk,
      ackEveryInput: autoSettings ? autoSettings.ackEvery : manualAck,
      noAckGapMs: autoSettings ? autoSettings.noAckGapMs : manualNoAckGap,
      checkpointTimeoutMs: autoSettings ? autoSettings.checkpointTimeoutMs : manualCheckpoint,
      statusTimeoutMs: autoSettings ? autoSettings.statusTimeoutMs : manualStatusTimeout,
      ackSettleGapMs: autoSettings
        ? autoSettings.ackSettleGapMs
        : Math.round(clamp(manualNoAckGap * 0.5, 10, 200)),
      radioProfile: autoSettings ? autoSettings.profile : null,
    };
    appendLog(`Firmware: ${file.name} (${firmware.length} bytes)`);
    appendLog(`MD5: ${firmwareMd5}`);
    updatePlanLine(true);

    const password = String(ui.targetPassword.value || "").trim();
    if (password.length > 0) {
      setOtaStatus("Authentification...");
      appendLog("Tentative de login...");
      const fullKeyForLogin = await client.resolveTargetFullKeyForBinary(params.targetHex);
      if (!fullKeyForLogin) {
        throw new Error("Impossible de resoudre la cle complete de la cible pour login. Renseigne la cle 64 hex.");
      }
      const loginRes = await client.sendLogin(fullKeyForLogin, password);
      if (!loginRes.ok) {
        throw new Error(`Authentification echouee: ${loginRes.error}`);
      }
      appendLog(`Login OK${loginRes.is_admin ? " (admin)" : ""}`);
    }

    if (ui.useTempRadio?.checked) {
      const tempPreset = readTempRadioPresetFromUi();
      restoreClientPreset = await applyTempRadioPresetForOta(params.targetHex, tempPreset);

      if (ui.autoTune?.checked) {
        const tempAuto = recalcAutoFromCurrentSelection("temp-radio");
        if (!tempAuto) {
          throw new Error("auto tuning indisponible apres application du preset temp");
        }
        params.chunkSizeInput = tempAuto.chunkSize;
        params.ackEveryInput = tempAuto.ackEvery;
        params.noAckGapMs = tempAuto.noAckGapMs;
        params.checkpointTimeoutMs = tempAuto.checkpointTimeoutMs;
        params.statusTimeoutMs = tempAuto.statusTimeoutMs;
        params.ackSettleGapMs = tempAuto.ackSettleGapMs;
        params.radioProfile = tempAuto.profile;
      } else {
        params.radioProfile = estimateOtaRadioProfile(tempPreset.bw, tempPreset.sf, tempPreset.cr);
      }

      updatePlanLine(true);
    }

    const result = await runOtaWithAutoTransport(params);
    appendLog("OTA staged successfully, reboot command sent.");
    appendLog(`Transport utilise: ${result.transport === "binary_req" ? "binary req" : "text cmd"}`);
    appendLog(`Duration: ${formatDuration(result.durationSec)}`);
    appendLog(
      `Rejets chunks: total ${result.totalRejects}/${result.chunkWriteAttempts} (${result.failureRatePct.toFixed(2)}%), `
      + `send ${result.chunkWriteFailures}/${result.chunkWriteAttempts} (${result.sendFailureRatePct.toFixed(2)}%), `
      + `serveur ${result.chunkServerRejects}/${result.chunkWriteAttempts} (${result.serverRejectRatePct.toFixed(2)}%)`
    );
    setProgress(100);
    setOtaStatus("OTA terminee");
    ui.metricsLine.textContent =
      `Transport ${result.transport === "binary_req" ? "binary" : "text"} | Duree ${formatDuration(result.durationSec)} | `
      + `Rejets total ${result.totalRejects}/${result.chunkWriteAttempts} (${result.failureRatePct.toFixed(2)}%) `
      + `[send ${result.chunkWriteFailures}/${result.chunkWriteAttempts} ${result.sendFailureRatePct.toFixed(2)}%, `
      + `srv ${result.chunkServerRejects}/${result.chunkWriteAttempts} ${result.serverRejectRatePct.toFixed(2)}%]`;
  } catch (e) {
    appendLog(`OTA error: ${e.message}`);
    setOtaStatus("OTA echouee");
    ui.metricsLine.textContent = `Erreur: ${e.message}`;
  } finally {
    if (restoreClientPreset) {
      try {
        await restoreLocalRadioPresetAfterOta(restoreClientPreset);
        if (ui.autoTune?.checked) {
          applyAutoOtaSettings("restore");
        }
      } catch (restoreErr) {
        appendLog(`Warning: restauration preset client echouee: ${restoreErr.message}`);
      }
    }
    otaRunning = false;
    otaCancelRequested = false;
    updateButtons();
  }
});

if (ui.autoTune) {
  ui.autoTune.addEventListener("change", () => {
    updateTuneInputsState();
    if (ui.autoTune.checked) {
      recalcAutoFromCurrentSelection("toggle");
    } else {
      appendLog("Auto tuning desactive.");
      updatePlanLine(true);
    }
  });
}

if (ui.connectionMode) {
  ui.connectionMode.addEventListener("change", () => {
    updateConnectionModeUi();
  });
}

if (ui.useTempRadio) {
  ui.useTempRadio.addEventListener("change", () => {
    updateTempRadioInputsState();
    if (ui.useTempRadio.checked) appendLog("Preset OTA temporaire active.");
    else appendLog("Preset OTA temporaire desactive.");

    if (ui.autoTune?.checked) {
      recalcAutoFromCurrentSelection(ui.useTempRadio.checked ? "temp-on" : "temp-off");
    }
  });
}

updateConnectionModeUi();

for (const el of [ui.tempRadioFreq, ui.tempRadioBw, ui.tempRadioSf, ui.tempRadioCr]) {
  if (!el) continue;
  el.addEventListener("input", () => {
    if (ui.autoTune?.checked && ui.useTempRadio?.checked) {
      recalcAutoFromCurrentSelection(null);
    }
  });
}

if (ui.firmwareFile) {
  ui.firmwareFile.addEventListener("change", () => {
    updatePlanLine(true);
  });
}
if (ui.chunkSize) {
  ui.chunkSize.addEventListener("input", () => {
    updatePlanLine(true);
  });
}
if (ui.ackEvery) {
  ui.ackEvery.addEventListener("input", () => {
    updatePlanLine(true);
  });
}
if (ui.targetKey) {
  ui.targetKey.addEventListener("input", () => {
    const manual = normalizeHex(ui.targetKey.value || "");
    if (!ui.targetSelect) return;
    if (!manual) {
      return;
    }
    const match = Array.from(ui.targetSelect.options).find((opt) => {
      const key = normalizeHex(opt.value || "");
      return key.length >= 12 && (key === manual || key.startsWith(manual) || manual.startsWith(key));
    });
    if (match) {
      ui.targetSelect.value = match.value;
    } else {
      ui.targetSelect.value = "";
    }
  });
}

setConnectionStatus("Non connecte");
setProgress(0);
resetTargetRepeaterSelect();
updateTuneInputsState();
updateTempRadioInputsState();
if (ui.autoTune?.checked) {
  recalcAutoFromCurrentSelection();
}
updatePlanLine(false);
updateButtons();
