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
// Plafond réel de la chaîne : la trame série du companion (MAX_FRAME_SIZE =
// 176) porte 40 octets d'en-tête + chunk, soit chunk <= 136. Au-delà, la trame
// est tronquée et la cible rejette silencieusement chaque write (vérifié sur
// le terrain avec 160). 132 garde une marge et reste la valeur éprouvée.
const OTA_BINARY_MAX_CHUNK = 132;
const CMD_SET_RADIO_PARAMS = 11;
const CMD_SET_TUNING_PARAMS = 21;
const CMD_GET_TUNING_PARAMS = 43;
// Facteur de budget d'airtime du companion pendant l'OTA. Le dispatcher
// maintient une réserve de 100 ms et attend (airtime/duty) après chaque TX :
// même à 0.25 (80%), l'inter-paquet reste ~t/0.8 et la file se remplit dès que
// l'alimentation va plus vite. 0 = duty 100% : plus aucune attente de budget,
// le gap d'alimentation devient l'unique régulateur. Restauré en fin d'OTA.
const OTA_AIRTIME_FACTOR = 0;

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
  TUNING_PARAMS: 23,
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
const XIP_BASE = 0x10000000;
const UF2_MAGIC_START0 = 0x0a324655;
const UF2_MAGIC_START1 = 0x9e5d5157;
const UF2_MAGIC_END = 0x0ab16f30;
const UF2_FLAG_NO_FLASH = 0x00000001;
const UF2_FLAG_FAMILY_ID_PRESENT = 0x00002000;
const UF2_RP2040_FAMILY_ID = 0xe48bff56;
const UF2_MAX_PADDING_BYTES = 16 * 1024 * 1024;
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

function lowerFileName(file) {
  return String(file?.name || "").trim().toLowerCase();
}

function formatByteCount(bytes) {
  const value = Number(bytes) || 0;
  if (value < 1024) return `${value} o`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / (1024 * 1024)).toFixed(2)} MiB`;
}

function parseUf2FamilyIds(bytes) {
  const ids = new Set();
  const blockCount = Math.floor(bytes.length / 512);
  for (let blockNo = 0; blockNo < blockCount; blockNo += 1) {
    const offset = blockNo * 512;
    const magic0 = parseU32LE(bytes, offset);
    const magic1 = parseU32LE(bytes, offset + 4);
    const magicEnd = parseU32LE(bytes, offset + 508);
    if (magic0 !== UF2_MAGIC_START0 || magic1 !== UF2_MAGIC_START1 || magicEnd !== UF2_MAGIC_END) {
      continue;
    }
    const flags = parseU32LE(bytes, offset + 8);
    if ((flags & UF2_FLAG_NO_FLASH) !== 0) {
      continue;
    }
    if ((flags & UF2_FLAG_FAMILY_ID_PRESENT) !== 0) {
      ids.add(parseU32LE(bytes, offset + 28) >>> 0);
    }
  }
  return ids;
}

function convertUf2ToBin(bytes) {
  appendLog(`UF2: analyse ${Math.floor(bytes.length / 512)} blocs (${formatByteCount(bytes.length)})`);
  if (!(bytes instanceof Uint8Array) || bytes.length < 512 || (bytes.length % 512) !== 0) {
    throw new Error("UF2 invalide (taille non multiple de 512)");
  }

  const familyIds = parseUf2FamilyIds(bytes);
  if (familyIds.size > 0) {
    appendLog(`UF2: familles détectées ${Array.from(familyIds, (id) => `0x${id.toString(16).padStart(8, "0")}`).join(", ")}`);
  } else {
    appendLog("UF2: aucun family id explicite, poursuite en mode compatible");
  }
  if (familyIds.size > 0 && !familyIds.has(UF2_RP2040_FAMILY_ID)) {
    const ids = Array.from(familyIds, (id) => `0x${id.toString(16).padStart(8, "0")}`).join(", ");
    throw new Error(`UF2 non RP2040 (${ids})`);
  }

  const blockCount = bytes.length / 512;
  const blocks = [];
  for (let blockNo = 0; blockNo < blockCount; blockNo += 1) {
    const offset = blockNo * 512;
    const magic0 = parseU32LE(bytes, offset);
    const magic1 = parseU32LE(bytes, offset + 4);
    const magicEnd = parseU32LE(bytes, offset + 508);
    if (magic0 !== UF2_MAGIC_START0 || magic1 !== UF2_MAGIC_START1 || magicEnd !== UF2_MAGIC_END) {
      throw new Error(`UF2 invalide (bloc ${blockNo} corrompu)`);
    }

    const flags = parseU32LE(bytes, offset + 8);
    if ((flags & UF2_FLAG_NO_FLASH) !== 0) {
      continue;
    }

    const targetAddr = parseU32LE(bytes, offset + 12) >>> 0;
    const payloadSize = parseU32LE(bytes, offset + 16) >>> 0;
    const familyId = parseU32LE(bytes, offset + 28) >>> 0;
    if (payloadSize === 0 || payloadSize > 476) {
      throw new Error(`UF2 invalide (payload bloc ${blockNo})`);
    }
    if ((flags & UF2_FLAG_FAMILY_ID_PRESENT) !== 0 && familyId !== UF2_RP2040_FAMILY_ID) {
      continue;
    }

    blocks.push({
      targetAddr,
      payload: bytes.slice(offset + 32, offset + 32 + payloadSize),
    });
  }

  if (blocks.length === 0) {
    throw new Error("UF2 vide ou sans bloc flash RP2040");
  }

  blocks.sort((a, b) => a.targetAddr - b.targetAddr);
  const baseAddr = blocks[0].targetAddr >>> 0;
  if (baseAddr !== XIP_BASE) {
    throw new Error(`UF2 inattendu: adresse de base 0x${baseAddr.toString(16)}`);
  }

  const chunks = [];
  let currentAddr = baseAddr;
  for (const block of blocks) {
    if (block.targetAddr < currentAddr) {
      throw new Error("UF2 invalide (blocs hors ordre)");
    }
    const padding = block.targetAddr - currentAddr;
    if (padding > UF2_MAX_PADDING_BYTES) {
      throw new Error("UF2 invalide (trou trop grand)");
    }
    if (padding > 0) {
      chunks.push(new Uint8Array(padding));
    }
    chunks.push(block.payload);
    currentAddr = block.targetAddr + block.payload.length;
  }

  const out = concatBytes(...chunks);
  appendLog(`UF2: ${blocks.length} blocs flash retenus, binaire reconstruit ${formatByteCount(out.length)}`);
  return out;
}

async function gzipBytes(bytes) {
  appendLog(`GZIP: compression démarrée (${formatByteCount(bytes.length)})`);
  const sourceStream = new Blob([bytes]).stream();
  appendLog("GZIP: pipeline Blob.stream -> CompressionStream créé");
  const compressed = await new Response(
    sourceStream.pipeThrough(new CompressionStream("gzip"))
  ).arrayBuffer();
  const out = new Uint8Array(compressed);
  appendLog(`GZIP: compression terminée (${formatByteCount(out.length)})`);
  return out;
}

async function gunzipBytes(bytes) {
  const sourceStream = new Blob([bytes]).stream();
  const decompressed = await new Response(
    sourceStream.pipeThrough(new DecompressionStream("gzip"))
  ).arrayBuffer();
  const out = new Uint8Array(decompressed);
  appendLog(`GZIP: décompression ${formatByteCount(bytes.length)} -> ${formatByteCount(out.length)}`);
  return out;
}

// La cible annonce sa capacité gzip dans la réponse START via 'gz=' : gz=1 →
// décompression embarquée, gz=0 → image brute exigée (ESP32 sans inflater).
// Les firmwares RP2040 antérieurs à ce jeton décompressent nativement via le
// bootloader arduino-pico, d'où le défaut à true en son absence.
function targetSupportsGzip(startReply) {
  const m = /gz=([01])/.exec(String(startReply || ""));
  return m ? m[1] === "1" : true;
}

function chooseFirmwareVariant(startReply, variants) {
  if (!variants) return null;
  if (targetSupportsGzip(startReply)) {
    return variants.gz || variants.raw || null;
  }
  if (variants.raw) return variants.raw;
  throw new Error(
    "La cible exige un .bin brut (gz=0) mais seule la forme gzip est disponible "
    + "(source .gz non décompressable dans ce navigateur)."
  );
}

// En reprise de session, la capacité gzip n'est pas re-annoncée : on identifie
// la forme (gzip ou brute) déjà en cours par la taille totale de la session.
function matchFirmwareVariantBySize(variants, total) {
  if (!variants || !Number.isFinite(total)) return null;
  if (variants.gz && variants.gz.bytes.length === total) return variants.gz;
  if (variants.raw && variants.raw.bytes.length === total) return variants.raw;
  return null;
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
  const status = {
    done: Number.parseInt(m[1], 10),
    total: Number.parseInt(m[2], 10),
    missing: [],
    selectiveNack: /\bnack=miss\b/.test(text || ""),
  };
  // NACK sélectif (cibles récentes) : 'miss=<off>+<len>,...' liste les trous
  // du bloc de staging courant — seuls ces octets sont à réémettre, au lieu
  // de rembobiner tout le lot à 'done'. Absent sur les cibles anciennes.
  const missM = /miss=([0-9+,]+)/.exec(text || "");
  if (missM) {
    for (const tok of missM[1].split(",")) {
      const mm = /^(\d+)\+(\d+)$/.exec(tok);
      if (mm) {
        const off = Number.parseInt(mm[1], 10);
        const len = Number.parseInt(mm[2], 10);
        if (len > 0) status.missing.push({ off, len });
      }
    }
  }
  return status;
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

// A full companion TX slot becomes available roughly every 48 ms on the
// measured SF5/BW250 path. The producer deliberately uses the companion queue
// for overlap; when it fills, wait slightly beyond one dispatcher slot.
function otaBusyRetryDelay(noAckGapMs, busyOrdinal = 1) {
  const base = Math.max(55, Number(noAckGapMs) || 0);
  return Math.min(180, base + (20 * Math.max(0, busyOrdinal - 1)));
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

  // Le plaintext chiffre = 11 octets d'en-tete + chunk, padde AES par blocs de
  // 16 : les tailles ≡ 5 (mod 16) evitent tout octet de padding transmis pour
  // rien (117 -> 128 exact, 101 -> 112, 85 -> 96, 69 -> 80). 132 est garde au
  // palier haut : au plafond de la trame serie du companion (chunk <= 136),
  // +15 octets utiles battent 1 octet de padding economise.
  let chunkBinary = 69;
  if (airFactor <= 1.2) chunkBinary = 132;
  else if (airFactor <= 2.5) chunkBinary = 117;
  else if (airFactor <= 5.0) chunkBinary = 101;
  else if (airFactor <= 10.0) chunkBinary = 85;

  let chunkText = 48;
  if (airFactor <= 1.2) chunkText = 80;
  else if (airFactor <= 2.5) chunkText = 72;
  else if (airFactor <= 5.0) chunkText = 64;
  else if (airFactor <= 10.0) chunkText = 56;

  // The firmware suppresses every per-write ACK in binary transport (no more
  // ACK/STATUS collision), so larger batches between STATUS checkpoints are safe.
  // On very slow links (high airFactor) the STATUS round-trip dominates wall-clock,
  // so batching aggressively is the main lever for OTA duration.
  let ackEvery = 6;
  if (airFactor <= 0.25) ackEvery = 12;
  else if (airFactor <= 0.6) ackEvery = 10;
  else if (airFactor <= 1.2) ackEvery = 8;
  else if (airFactor <= 2.5) ackEvery = 6;
  else if (airFactor <= 5.0) ackEvery = 5;
  else if (airFactor <= 10.0) ackEvery = 4;

  // Feed the companion queue in short bursts. Its dispatcher drains more
  // slowly, but pacing every producer write at the radio rate (~50 ms) throws
  // away the queue's useful overlap. Actual saturation is handled by the
  // code=3 flow-control retry, slightly beyond one dispatcher slot.
  const noAckGapSec = clamp(0.015 + (0.025 * airFactor), 0.015, 0.45);
  const checkpointTimeoutSec = clamp(0.9 + (0.9 * airFactor), 1.5, 12.0);
  const statusTimeoutSec = clamp(1.4 + (1.2 * airFactor), 2.0, 12.0);

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
    this.ws = null;
    this._tcpResolve = null;
    this._tcpReject = null;
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
    this.onDisconnected = null;
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
    throw new Error("transport non supporté");
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
    if (!device) throw new Error("périphérique USB manquant");

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
      throw new Error("interface CDC bulk IN/OUT non trouvée");
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
        : "interface CDC série détectée mais verrouillée par l'OS; utiliser Web Serial si disponible.";
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
    if (!device) throw new Error("périphérique BLE manquant");
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
      throw new Error("service BLE MeshCore détecté, mais caractéristiques RX/TX introuvables");
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

  // TCP companion (WiFi) via a local WebSocket<->TCP bridge.
  // The companion speaks the exact same framing as the serial interface
  // ('>'/'<' + LE16 length), so on-wire this transport behaves like serial:
  // the bridge is a dumb byte pipe and we reuse parseFrames() untouched.
  async connectTcp(companionHost, companionPort) {
    if (!companionHost) throw new Error("adresse companion manquante");
    const scheme = location.protocol === "https:" ? "wss://" : "ws://";
    const url = `${scheme}${location.host}/tcp`
      + `?host=${encodeURIComponent(companionHost)}`
      + `&port=${encodeURIComponent(companionPort)}`;
    this.log(`Connexion au pont TCP: ${url}`);

    const ws = new WebSocket(url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;
    this.rxPending = [];

    const ready = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout de connexion au pont")), 8000);
      this._tcpResolve = () => { clearTimeout(timer); resolve(); };
      this._tcpReject = (msg) => { clearTimeout(timer); reject(new Error(msg)); };
    });
    const settleReady = () => { this._tcpResolve?.(); this._tcpResolve = null; this._tcpReject = null; };
    const settleFail = (msg) => { this._tcpReject?.(msg); this._tcpResolve = null; this._tcpReject = null; };

    ws.addEventListener("message", (ev) => {
      if (typeof ev.data === "string") {
        // Control channel from the bridge: "ready" once the TCP dial to the
        // companion succeeds, or "error <msg>" if it fails.
        if (ev.data.startsWith("ready")) settleReady();
        else if (ev.data.startsWith("error")) settleFail(ev.data.slice(6).trim() || "erreur pont");
        return;
      }
      // Any binary frame implies the tunnel is live.
      if (this._tcpResolve) settleReady();
      this.handleTcpChunk(ev.data);
    });
    ws.addEventListener("error", () => {
      if (this._tcpReject) settleFail("connexion au pont échouée");
    });
    ws.addEventListener("close", (ev) => {
      if (this._tcpReject) { settleFail(ev.reason || "pont fermé avant établissement"); return; }
      if (!this.connected) return;
      this.log("Connexion TCP interrompue.");
      this.disconnect();
    });

    await ready;

    this.transport = "web_tcp";
    this.connected = true;
  }

  handleTcpChunk(arrayBuffer) {
    const chunk = new Uint8Array(arrayBuffer);
    if (chunk.length === 0) return;
    this.pushRxChunk(chunk);
    this.parseFrames();
  }

  async disconnect() {
    const wasConnected = this.connected;
    this.connected = false;
    this.waiters.forEach((w) => {
      clearTimeout(w.timer);
      w.reject(new Error("déconnecté"));
    });
    this.waiters = [];
    if (wasConnected && this.onDisconnected) {
      try { this.onDisconnected(); } catch {}
    }
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
    if (this.ws) {
      try { this.ws.close(); } catch {}
      this.ws = null;
    }
    this._tcpResolve = null;
    this._tcpReject = null;
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
      this.log("Connexion série interrompue.");
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
      case PACKET_TYPE.TUNING_PARAMS:
        this.emit("tuning_params", {
          rx_delay_base: frame.length >= 9 ? parseU32LE(frame, 1) / 1000.0 : null,
          airtime_factor: frame.length >= 9 ? parseU32LE(frame, 5) / 1000.0 : null,
        });
        break;
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
      throw new Error("non connecté");
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
    if (this.ws) {
      if (this.ws.readyState !== WebSocket.OPEN) throw new Error("pont TCP non ouvert");
      this.ws.send(bytes);
      return;
    }
    throw new Error("transport écriture indisponible");
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

  async getTuningParams(timeoutMs = 3000) {
    const evt = await this.sendCommand(
      Uint8Array.of(CMD_GET_TUNING_PARAMS),
      ["tuning_params", "error"],
      null,
      timeoutMs
    );
    if (!evt || evt.type === "error") {
      return null; // firmware companion trop ancien ou commande refusée
    }
    const rx = Number(evt.payload?.rx_delay_base);
    const af = Number(evt.payload?.airtime_factor);
    if (!Number.isFinite(rx) || !Number.isFinite(af)) return null;
    return { rx_delay_base: rx, airtime_factor: af };
  }

  async setTuningParams(rxDelayBase, airtimeFactor, timeoutMs = 3000) {
    const rx = Math.round(Math.max(0, Number(rxDelayBase) || 0) * 1000);
    const af = Math.round(Math.max(0, Number(airtimeFactor) || 0) * 1000);
    const payload = concatBytes(
      Uint8Array.of(CMD_SET_TUNING_PARAMS),
      u32ToBytesLE(rx),
      u32ToBytesLE(af)
    );
    const evt = await this.sendCommand(payload, ["ok", "error"], null, timeoutMs);
    if (!evt || evt.type === "error") {
      const code = evt?.payload?.error_code;
      return { ok: false, error: `set tuning error${code !== undefined ? ` (code=${code})` : ""}` };
    }
    return { ok: true };
  }

  async setLocalRadioParams(freqMhz, bwKhz, sf, cr, repeat = 0) {
    const norm = normalizeRadioPreset({ freq: freqMhz, bw: bwKhz, sf, cr });
    if (!norm) {
      return { ok: false, error: "paramètres radio invalides" };
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
      throw new Error("pubkey cible invalide (minimum 12 caractères hex)");
    }
    return clean.slice(0, 12);
  }

  normalizeTargetFullKey(targetHex) {
    const clean = normalizeHex(targetHex);
    if (clean.length < 64) {
      throw new Error("clé publique complète requise (64 hex)");
    }
    return clean.slice(0, 64);
  }

  async requestContacts(timeoutMs = 10000) {
    const startPromise = this.waitForEvent(["contacts_start", "error"], null, timeoutMs);
    await this.sendFrame(Uint8Array.of(0x04));
    const startEvt = await startPromise;
    if (!startEvt || startEvt.type === "error") {
      const code = startEvt?.payload?.error_code;
      throw new Error(`get contacts error${code !== undefined ? ` (code=${code})` : ""}`);
    }
    // Scale timeout by contact count: 200ms/contact minimum for BLE, with 3s buffer
    const count = startEvt.payload.count || 0;
    const endTimeoutMs = Math.max(timeoutMs, count * 200 + 3000);
    const evt = await this.waitForEvent(["contacts_end", "error"], null, endTimeoutMs);
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
        throw new Error(`préfixe ambigu (${keys.length} contacts)`);
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
    let sentEvt = null;
    try {
      sentEvt = await this.sendCommand(payload, ["msg_sent", "error"], null, 5000);
    } catch (e) {
      return { replyText: null, error: e?.message || "timeout waiting send ack" };
    }
    if (!sentEvt || sentEvt.type === "error") {
      const code = sentEvt?.payload?.error_code;
      return { replyText: null, error: `send error${code !== undefined ? ` (code=${code})` : ""}` };
    }
    const routeFlood = Number(sentEvt.payload?.type) === 1;
    if (!waitReply) {
      return { replyText: "", error: null, routeFlood };
    }

    const tag = String(sentEvt.payload?.expected_ack || "");
    const suggested = Number(sentEvt.payload?.suggested_timeout || 4000);
    const computed = Math.max(minTimeoutMs, Math.round((suggested / 800) * 1000));
    const waitMs = timeoutMs > 0 ? timeoutMs : computed;

    const queued = this.popQueuedBinary(tag);
    if (queued) {
      const text = String(queued.text || "").replace(/\0+$/g, "");
      return { replyText: text, error: null, routeFlood };
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
    return { replyText: text, error: null, routeFlood };
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
  tcpTarget: document.querySelector("#tcpTarget"),
  tcpTargetField: document.querySelector("#tcpTargetField"),
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
  nodePills: document.querySelector("#nodePills"),
  step1badge: document.querySelector("#step1badge"),
  step2badge: document.querySelector("#step2badge"),
  step3badge: document.querySelector("#step3badge"),
  rebootBtn: document.querySelector("#rebootBtn"),
  copyLogBtn: document.querySelector("#copyLogBtn"),
  clearLogBtn: document.querySelector("#clearLogBtn"),
  modalOverlay: document.querySelector("#modalOverlay"),
  modalMessage: document.querySelector("#modalMessage"),
  modalConfirmBtn: document.querySelector("#modalConfirmBtn"),
  modalCancelBtn: document.querySelector("#modalCancelBtn"),
  toastContainer: document.querySelector("#toastContainer"),
  progressBar: document.querySelector("#progressBar"),
  log: document.querySelector("#log"),
};

let client = null;
let otaRunning = false;
let otaCancelRequested = false;
let otaCompleted = false;
// Compteurs de backpressure locale (code=3), partagés entre runBinaryOta et
// getOtaStatusBinary, remis à zéro à chaque transfert.
const otaBusyStats = { events: 0, sleepMs: 0 };
let lastSelfInfo = null;
let lastDeviceInfo = null;
let lastPlanSummary = "";

const LOG_MAX_CHARS = 200000;

function appendLog(line) {
  let value = `${ui.log.value}[${nowTime()}] ${line}\n`;
  if (value.length > LOG_MAX_CHARS) {
    const cut = value.indexOf("\n", value.length - Math.floor(LOG_MAX_CHARS * 0.75));
    value = `[journal tronqué]\n${value.slice(cut + 1)}`;
  }
  ui.log.value = value;
  ui.log.scrollTop = ui.log.scrollHeight;
}

function showToast(message, kind = "") {
  if (!ui.toastContainer) return;
  const toast = document.createElement("div");
  toast.className = `toast${kind ? ` ${kind}` : ""}`;
  toast.textContent = message;
  ui.toastContainer.appendChild(toast);
  setTimeout(() => toast.remove(), 3500);
}

function showConfirm(message) {
  if (!ui.modalOverlay || !ui.modalMessage) {
    return Promise.resolve(window.confirm(message));
  }
  return new Promise((resolve) => {
    ui.modalMessage.textContent = message;
    ui.modalOverlay.hidden = false;
    const done = (value) => {
      ui.modalOverlay.hidden = true;
      ui.modalConfirmBtn.onclick = null;
      ui.modalCancelBtn.onclick = null;
      ui.modalOverlay.onclick = null;
      resolve(value);
    };
    ui.modalConfirmBtn.onclick = () => done(true);
    ui.modalCancelBtn.onclick = () => done(false);
    ui.modalOverlay.onclick = (e) => {
      if (e.target === ui.modalOverlay) done(false);
    };
  });
}

function updateStepBadges() {
  const connected = Boolean(client && client.connected);
  const hasTarget = getSelectedTargetHex().length >= 12;
  const hasFile = Boolean(ui.firmwareFile?.files && ui.firmwareFile.files[0]);
  if (ui.step1badge) ui.step1badge.classList.toggle("done", connected);
  if (ui.step2badge) ui.step2badge.classList.toggle("done", connected && hasTarget && hasFile);
  if (ui.step3badge) ui.step3badge.classList.toggle("done", otaCompleted);
}

function setConnectionStatus(text, state = "") {
  ui.connectionStatus.textContent = text;
  ui.connectionStatus.className = state;
  if (ui.nodePills) ui.nodePills.style.display = state === "ok" ? "flex" : "none";
  updateStepBadges();
}

function setOtaStatus(text) {
  ui.otaStatus.textContent = text;
}

function setProgress(percent) {
  const p = Math.max(0, Math.min(100, Number(percent) || 0));
  ui.progressBar.classList.remove("indeterminate");
  ui.progressBar.style.width = `${p}%`;
}

function setProgressIndeterminate() {
  ui.progressBar.classList.add("indeterminate");
}

function getTransportLabel(transport) {
  if (transport === "web_usb") return "WebUSB";
  if (transport === "web_ble") return "Web Bluetooth";
  if (transport === "web_tcp") return "TCP (pont)";
  return "Web Serial";
}

function updateConnectionModeUi() {
  const mode = String(ui.connectionMode?.value || "usb");
  const isBle = mode === "ble";
  const isTcp = mode === "tcp";
  if (ui.baudrate) ui.baudrate.disabled = isBle || isTcp;
  if (ui.tcpTargetField) ui.tcpTargetField.style.display = isTcp ? "" : "none";
  if (ui.connectBtn) {
    ui.connectBtn.textContent = isBle ? "Connecter BLE" : (isTcp ? "Connecter TCP" : "Connecter USB");
  }
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
  option.textContent = "Sélectionner un répéteur…";
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
      `Transport ${tr} | ${pct}% | Estimation en cours… | Rejets ${totalRejects}/${attempts} (${totalRejectRatePct.toFixed(2)}%)`;
    return;
  }

  ui.metricsLine.textContent =
    `Transport ${tr} | ${pct}% | ${(rateBps / 1024).toFixed(2)} ko/s | Restant ~${formatDuration(etaSec)} | `
    + `Rejets ${totalRejects}/${attempts} (${totalRejectRatePct.toFixed(2)}%) `
    + `[send ${sendFailures}/${attempts} ${sendFailureRatePct.toFixed(2)}%, `
    + `srv ${serverRejects}/${attempts} ${serverRejectRatePct.toFixed(2)}%]`;
}

function updateButtons() {
  const connected = Boolean(client && client.connected);
  const mode = String(ui.connectionMode?.value || "usb");
  const isBle = mode === "ble";
  const isTcp = mode === "tcp";
  ui.connectBtn.disabled = connected || otaRunning;
  ui.disconnectBtn.disabled = !connected || otaRunning;
  ui.startOtaBtn.disabled = !connected || otaRunning;
  ui.cancelOtaBtn.disabled = !otaRunning;
  if (ui.refreshTargetsBtn) ui.refreshTargetsBtn.disabled = !connected || otaRunning;
  if (ui.targetSelect) ui.targetSelect.disabled = !connected || otaRunning;
  if (ui.targetKey) ui.targetKey.disabled = otaRunning;
  if (ui.targetPassword) ui.targetPassword.disabled = otaRunning;
  if (ui.firmwareFile) ui.firmwareFile.disabled = otaRunning;
  if (ui.connectionMode) ui.connectionMode.disabled = otaRunning;
  if (ui.baudrate) ui.baudrate.disabled = otaRunning || isBle || isTcp;
  if (ui.tcpTarget) ui.tcpTarget.disabled = otaRunning || connected;
  if (ui.rebootBtn) ui.rebootBtn.disabled = otaRunning;
  if (ui.useTempRadio) {
    ui.useTempRadio.disabled = otaRunning;
  }
  updateTempRadioInputsState();
  updateStepBadges();
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
  let planText = "Plan OTA: sélectionne un firmware";

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
        appendLog(`Auto tuning ignoré (preset OTA invalide): ${e.message}`);
      }
      return null;
    }
  }
  return applyAutoOtaSettings(logReason);
}

// Prépare les DEUX formes du firmware (brute et gzip) sans en choisir une :
// le choix dépend de la capacité de la cible ('gz=' dans la réponse START,
// connue seulement après la sonde) — voir chooseFirmwareVariant(). Le gzip
// réduit le volume transmis de ~40-50% quand la cible sait décompresser
// (bootloader arduino-pico, inflater ROM des ESP32).
async function getFirmwareBytes(file) {
  const sourceBytes = new Uint8Array(await file.arrayBuffer());
  const fileName = lowerFileName(file);
  appendLog(`Lecture firmware: ${file.name} (${formatByteCount(sourceBytes.length)})`);

  const canGzip = typeof CompressionStream === "function";
  const canGunzip = typeof DecompressionStream === "function";

  const buildGzCandidate = async (rawBytes) => {
    if (!canGzip) {
      appendLog("Firmware: CompressionStream indisponible, forme gzip non préparée");
      return null;
    }
    const gzipPayload = await gzipBytes(rawBytes);
    if (gzipPayload.length >= rawBytes.length) {
      appendLog("Firmware: gzip sans gain, forme gzip écartée");
      return null;
    }
    appendLog(
      `Firmware: forme gzip prête ${formatByteCount(rawBytes.length)} -> ${formatByteCount(gzipPayload.length)} `
      + `(${Math.round((1 - (gzipPayload.length / rawBytes.length)) * 100)}% de moins si la cible décompresse)`
    );
    return gzipPayload;
  };

  if (fileName.endsWith(".uf2")) {
    appendLog("Firmware: format source détecté = uf2");
    const binBytes = convertUf2ToBin(sourceBytes);
    return {
      sourceFormat: "uf2",
      originalSize: sourceBytes.length,
      extractedSize: binBytes.length,
      rawBytes: binBytes,
      gzBytes: await buildGzCandidate(binBytes),
    };
  }

  const isGzip = fileName.endsWith(".gz");
  appendLog(`Firmware: format source détecté = ${isGzip ? "bin.gz" : "bin"}`);

  if (isGzip) {
    let rawBytes = null;
    if (canGunzip) {
      try {
        rawBytes = await gunzipBytes(sourceBytes);
      } catch (e) {
        appendLog(`Firmware: décompression du .gz impossible (${e.message}), forme brute non préparée`);
      }
    } else {
      appendLog("Firmware: DecompressionStream indisponible, forme brute non préparée");
    }
    return {
      sourceFormat: "bin.gz",
      originalSize: sourceBytes.length,
      extractedSize: rawBytes ? rawBytes.length : null,
      rawBytes,
      gzBytes: sourceBytes,
    };
  }

  return {
    sourceFormat: "bin",
    originalSize: sourceBytes.length,
    extractedSize: sourceBytes.length,
    rawBytes: sourceBytes,
    gzBytes: await buildGzCandidate(sourceBytes),
  };
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
    let res = await client.sendOtaBinaryCmd(
      targetFullKeyHex,
      OTA_BIN_OP.STATUS,
      new Uint8Array(),
      true,
      timeoutMs,
      250
    );
    // ERR_CODE_TABLE_FULL (code=3) : la file TX du companion est pleine — le
    // STATUS de checkpoint est soumis au pic de remplissage, juste après le
    // dernier chunk du lot. Condition purement locale : on attend le drain et
    // on resoumet sans consommer de tentative radio. Ce délai laisse aussi le
    // chunk-frontière atterrir et le gel flash de la cible se terminer avant
    // que le STATUS ne parte en l'air.
    for (let busy = 0; busy < 4 && res.error && String(res.error).includes("code=3"); busy += 1) {
      otaBusyStats.events += 1;
      otaBusyStats.sleepMs += 90 + (60 * busy);
      await sleep(90 + (60 * busy));
      res = await client.sendOtaBinaryCmd(
        targetFullKeyHex,
        OTA_BIN_OP.STATUS,
        new Uint8Array(),
        true,
        timeoutMs,
        250
      );
    }
    if (!res.error) {
      return { reply: res.replyText, status: parseOtaStatus(res.replyText), error: null };
    }
    lastErr = res.error;
    if (attempt + 1 < maxAttempts) await sleep(150 * (attempt + 1));
  }
  return { reply: null, status: null, error: lastErr };
}

function isOkOtaReply(text) {
  return String(text || "").trim().toLowerCase().startsWith("ok");
}

async function abortTargetOtaSession(targetHex, targetFullKeyHex = "", reason = "reset") {
  const cleanFull = normalizeHex(targetFullKeyHex || "");
  const cleanTarget = normalizeHex(targetHex || "");

  appendLog(`OTA cleanup (${reason}): tentative de remise à zéro de la session cible.`);

  if (cleanFull.length >= 64) {
    try {
      const binRes = await client.sendOtaBinaryCmd(
        cleanFull,
        OTA_BIN_OP.ABORT,
        new Uint8Array(),
        true,
        6000,
        400
      );
      if (!binRes.error && isOkOtaReply(binRes.replyText)) {
        appendLog(`OTA cleanup (${reason}): abort binaire confirmé.`);
        return true;
      }
      appendLog(
        `OTA cleanup (${reason}): abort binaire non confirmé`
        + `${binRes.error ? ` (${binRes.error})` : ` (${String(binRes.replyText || "").trim() || "réponse vide"})`}.`
      );
    } catch (e) {
      appendLog(`OTA cleanup (${reason}): exception abort binaire (${e.message}).`);
    }
  }

  if (cleanTarget.length >= 12) {
    try {
      const textRes = await client.sendRepeaterCmdAndWaitReply(cleanTarget, "ota abort", 8);
      if (!textRes.error && isOkOtaReply(textRes.reply)) {
        appendLog(`OTA cleanup (${reason}): abort texte confirmé.`);
        return true;
      }
      appendLog(
        `OTA cleanup (${reason}): abort texte non confirmé`
        + `${textRes.error ? ` (${textRes.error})` : ` (${String(textRes.reply || "").trim() || "réponse vide"})`}.`
      );
    } catch (e) {
      appendLog(`OTA cleanup (${reason}): exception abort texte (${e.message}).`);
    }
  }

  appendLog(`OTA cleanup (${reason}): remise à zéro non confirmée.`);
  return false;
}

async function sendRemoteReboot(targetHex) {
  if (!targetHex) {
    appendLog("Erreur: Pas de cible sélectionnée");
    return { error: true, message: "No target selected" };
  }

  try {
    const password = String(ui.targetPassword?.value || "").trim();
    if (password.length > 0) {
      appendLog("Tentative de login...");
      const fullKey = await client.resolveTargetFullKeyForBinary(targetHex);
      if (!fullKey) {
        throw new Error("Impossible de résoudre la clé complète de la cible. Renseigne la clé 64 hex.");
      }
      const loginRes = await client.sendLogin(fullKey, password);
      if (!loginRes.ok) {
        throw new Error(`Authentification échouée: ${loginRes.error}`);
      }
      appendLog(`Login OK${loginRes.is_admin ? " (admin)" : ""}`);
    }

    appendLog(`Envoi de la commande reboot à ${targetHex}...`);
    await client.sendRepeaterCmdNoReply(targetHex, "reboot");
    appendLog(`Reboot envoyé avec succès à ${targetHex}`);
    return { error: false };
  } catch (e) {
    const msg = e.message || String(e);
    appendLog(`Erreur lors du reboot: ${msg}`);
    return { error: true, message: msg };
  }
}

async function runBinaryOta(params) {
  const {
    targetHex,
    targetFullKeyHex,
    firmwareVariants,
    chunkSizeInput,
    ackEveryInput,
    noAckGapMs,
    checkpointTimeoutMs,
    statusTimeoutMs,
    radioProfile,
  } = params;
  let firmware = params.firmware;
  let firmwareMd5 = params.firmwareMd5;

  if (chunkSizeInput < 16 || chunkSizeInput > OTA_BINARY_MAX_CHUNK) {
    throw new Error(`chunk_size doit être entre 16 et ${OTA_BINARY_MAX_CHUNK} en mode binaire`);
  }
  if (ackEveryInput < 1 || ackEveryInput > 64) {
    throw new Error("ack_every doit être entre 1 et 64");
  }
  if (!firmware || firmware.length === 0) {
    throw new Error("firmware vide");
  }
  if (!/^[0-9a-f]{32}$/i.test(String(firmwareMd5 || ""))) {
    throw new Error("md5 firmware invalide");
  }

  let fwSize = firmware.length;
  let chunkSize = Math.min(chunkSizeInput, OTA_BINARY_MAX_CHUNK);
  // En binaire, le firmware supprime tous les ACK d'écriture : la cadence des
  // checkpoints STATUS est purement côté client, donc adaptable en cours de
  // route sans renégocier le BEGIN.
  // Avec le staging 16 Ko de la cible, les checkpoints obligatoires tombent
  // tous les ~124 chunks : le plafond d'ack doit pouvoir monter au-delà pour
  // que la cadence soit dictée par les frontières de flush, pas par le compte.
  const ACK_EVERY_MAX = 128;
  const ACK_EVERY_MIN = 2; // jamais 1 : un STATUS par chunk effondre le débit
  let ackEvery = Math.max(ACK_EVERY_MIN, ackEveryInput);
  let ackEveryCeiling = ACK_EVERY_MAX;
  let cleanCheckpoints = 0;
  let totalChunks = Math.ceil(fwSize / chunkSize);

  // Bascule sur la forme (gzip ou brute) que la cible sait consommer, connue
  // après la réponse START ('gz=') ou déduite de la taille en reprise.
  const applyFirmwareVariant = (variant, why) => {
    if (!variant) return;
    if (firmware !== variant.bytes) {
      firmware = variant.bytes;
      firmwareMd5 = variant.md5;
      fwSize = firmware.length;
      totalChunks = Math.ceil(fwSize / chunkSize);
    }
    appendLog(`Firmware retenu: ${variant.format} ${fwSize} bytes (md5=${firmwareMd5}) [${why}]`);
  };

  appendLog(`Transport: binary req`);
  appendLog(`Firmware: ${fwSize} bytes (md5=${firmwareMd5})`);
  appendLog(`Chunk size: ${chunkSize} bytes (${totalChunks} chunks)`);
  appendLog(`Ack every: ${ackEvery} chunk(s) (adaptatif ${ACK_EVERY_MIN}..${ACK_EVERY_MAX})`);
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

  // La cible gèle sa flash à chaque flush de son tampon de staging : les chunks
  // arrivant pendant ce gel sont perdus. Une pause côté client ne suffit pas
  // (elle suspend l'alimentation de la file du companion, pas l'antenne). On
  // force donc un checkpoint à chaque frontière de bloc : le client se tait,
  // la file se draine, et le seul paquet en vol pendant le gel (le STATUS)
  // survit dans le FIFO du SX1262. Les firmwares récents mettent en tampon RAM
  // 16 Ko et annoncent leur taille de bloc via 'blk=' dans la réponse START ;
  // 4096 (le tampon interne de l'Updater) reste le défaut sûr.
  let targetFlushBlock = 4096;
  let offset = 0;
  let chunkIndex = 0;
  let chunksSinceAck = 0;
  let chunkWriteAttempts = 0;
  let chunkWriteFailures = 0;
  let chunkServerRejects = 0;
  let lastProgressPct = -1;
  const writeMaxRetries = 3;
  const checkpointMaxRetries = 10;
  let shouldCleanupTargetOta = false;
  let resuming = false;

  // Instrumentation : où part le temps ? (résumé loggé en fin de transfert)
  let writeRttSumMs = 0;
  let writeRttCount = 0;
  let writeRttMaxMs = 0;
  let checkpointSumMs = 0;
  let checkpointCount = 0;
  let feedGapSleepMs = 0;
  let repairedChunks = 0;
  let repairedBytes = 0;
  let repairSendFailures = 0;
  let classicReplayChunks = 0;
  let classicReplayBytes = 0;
  let resyncEvents = 0;
  let selectiveNackCapability = null;
  otaBusyStats.events = 0;
  otaBusyStats.sleepMs = 0;

  // Réémission ciblée des trous annoncés par la cible via 'miss=' (NACK
  // sélectif) : envois no-reply comme le flux principal, la confirmation
  // passe par le STATUS suivant. Backpressure code=3 gérée comme dans la
  // boucle principale.
  const resendMissingRanges = async (missing) => {
    for (const range of missing) {
      let missOff = range.off;
      let left = range.len;
      const rangeChunks = Math.ceil(range.len / chunkSize);
      appendLog(
        `NACK: renvoi ${range.off}..${range.off + range.len - 1} `
        + `(${range.len} octets, ${rangeChunks} chunk(s))`
      );
      while (left > 0 && !otaCancelRequested) {
        const n = Math.min(chunkSize, left);
        const payload = concatBytes(
          u32ToBytesLE(missOff),
          Uint8Array.of(n & 0xff),
          firmware.subarray(missOff, missOff + n)
        );
        const busyDeadlineMs = performance.now() + Math.max(8000, (Number(checkpointTimeoutMs) || 1500) * 4);
        let busyWaits = 0;
        for (;;) {
          chunkWriteAttempts += 1;
          const res = await client.sendOtaBinaryCmd(
            targetFullKeyHex,
            OTA_BIN_OP.WRITE,
            payload,
            false,
            250,
            100
          );
          if (!res.error) {
            repairedChunks += 1;
            repairedBytes += n;
            break;
          }
          const isLocalBusy = String(res.error || "").includes("code=3");
          if (isLocalBusy && performance.now() < busyDeadlineMs) {
            busyWaits += 1;
            const busySleep = otaBusyRetryDelay(noAckGapMs, busyWaits);
            otaBusyStats.events += 1;
            otaBusyStats.sleepMs += busySleep;
            await sleep(busySleep);
            continue;
          }
          chunkWriteFailures += 1;
          repairSendFailures += 1;
          appendLog(`NACK: échec du renvoi à ${missOff} (${n} octets): ${res.error}`);
          break; // perte locale non-busy : le STATUS suivant re-listera ce trou
        }
        missOff += n;
        left -= n;
        if (noAckGapMs > 0) await sleep(noAckGapMs);
      }
    }
    return !otaCancelRequested;
  };

  // Détection de stagnation : si les checkpoints resynchronisent en boucle sans
  // que le serveur avance (lien asymétrique), on abandonne au lieu de tourner
  // indéfiniment.
  let lastResyncOffset = -1;
  let resyncStalls = 0;
  const noteResync = (localOffset, newOffset, source, statusReply = "") => {
    const replayBytes = Math.max(0, localOffset - newOffset);
    const replayChunks = estimateRejectedChunks(localOffset, newOffset, chunkSize);
    resyncEvents += 1;
    classicReplayBytes += replayBytes;
    classicReplayChunks += replayChunks;
    resyncStalls = newOffset <= lastResyncOffset ? resyncStalls + 1 : 0;
    lastResyncOffset = newOffset;
    appendLog(
      `Rejeu classique (${source}): client=${localOffset}, cible=${newOffset}, `
      + `recul=${replayBytes} octets (~${replayChunks} chunk(s)); `
      + `cumul=${classicReplayChunks} chunk(s) sur ${resyncEvents} resync`
    );
    if (statusReply) appendLog(`Status brut (${source}): ${statusReply}`);
    if (resyncStalls >= 5) {
      throw new Error(`OTA bloquée: resynchronisations répétées sans progression à l'offset ${newOffset}`);
    }
  };

  updateLiveMetrics("binary_req", offset, fwSize, tStart, chunkWriteAttempts, chunkWriteFailures, chunkServerRejects);

  try {
    let startRes = await client.sendOtaBinaryCmd(
      targetFullKeyHex,
      OTA_BIN_OP.START,
      new Uint8Array(),
      true,
      8000,
      500
    );

    if (startRes.error) {
      if (startRes.error.startsWith("timeout waiting binary reply")) {
        appendLog("Binary OTA non disponible (pas de réponse binaire).");
        return null;
      }
      throw new Error(`Start OTA failed (binary): ${startRes.error}`);
    }

    let startReply = String(startRes.replyText || "");
    if (!isOkOtaReply(startReply)) {
      const low = startReply.toLowerCase();
      if (low.includes("already running")) {
        const st = await getOtaStatusBinary(targetFullKeyHex, 2, statusTimeout);
        let resumeVariant = (st.status && st.status.done <= st.status.total)
          ? matchFirmwareVariantBySize(firmwareVariants, st.status.total)
          : null;
        if (!resumeVariant && st.status && st.status.total === fwSize && st.status.done <= fwSize) {
          resumeVariant = { bytes: firmware, md5: firmwareMd5, format: "forme courante" };
        }
        if (resumeVariant) {
          applyFirmwareVariant(resumeVariant, "reprise de session");
          offset = st.status.done;
          chunkIndex = Math.floor(offset / chunkSize);
          resuming = true;
          const statusBlk = /blk=(\d+)/.exec(String(st.reply || ""));
          if (statusBlk) {
            targetFlushBlock = Math.max(1024, Number.parseInt(statusBlk[1], 10) || 4096);
          }
          if (st.status.selectiveNack) {
            selectiveNackCapability = true;
            ackEveryCeiling = ACK_EVERY_MAX;
            appendLog(
              `NACK sélectif: session reprise, checkpoints limités aux frontières `
              + `de ${targetFlushBlock} octets.`
            );
          }
          appendLog(`Reprise de session OTA à l'offset ${offset}/${fwSize}`);
        } else {
          appendLog(
            `Binary OTA: session précédente incompatible, reset requis`
            + `${st.error ? ` (${st.error})` : st.reply ? ` (${st.reply})` : ""}.`
          );
          const resetOk = await abortTargetOtaSession(targetHex, targetFullKeyHex, "restart binary");
          if (!resetOk) {
            throw new Error(
              `Start OTA failed (binary): ${startReply}`
              + `${st.error ? `; ${st.error}` : st.reply ? `; status=${st.reply}` : ""}`
            );
          }
          startRes = await client.sendOtaBinaryCmd(
            targetFullKeyHex,
            OTA_BIN_OP.START,
            new Uint8Array(),
            true,
            8000,
            500
          );
          if (startRes.error) {
            throw new Error(`Start OTA failed after abort (binary): ${startRes.error}`);
          }
          startReply = String(startRes.replyText || "");
          if (!isOkOtaReply(startReply)) {
            throw new Error(`Start OTA failed after abort (binary): ${startReply}`);
          }
        }
      } else if (low.includes("unknown") || low.includes("unsupported")) {
        appendLog("Binary OTA unsupported by target.");
        return null;
      } else {
        throw new Error(`Start OTA failed (binary): ${startReply}`);
      }
    }

    shouldCleanupTargetOta = true;

    {
      const blkMatch = /blk=(\d+)/.exec(startReply);
      if (blkMatch) {
        targetFlushBlock = Math.max(1024, Number.parseInt(blkMatch[1], 10) || 4096);
        appendLog(
          `Bloc de flush cible: ${targetFlushBlock} octets `
          + `(checkpoint obligatoire ~tous les ${Math.floor(targetFlushBlock / chunkSize)} chunks)`
        );
      }

      if (/\bnack=miss\b/.test(startReply)) {
        selectiveNackCapability = true;
        ackEveryCeiling = ACK_EVERY_MAX;
        cleanCheckpoints = 0;
        appendLog(
          `NACK sélectif: support annoncé dès START; checkpoints limités aux frontières `
          + `de ${targetFlushBlock} octets.`
        );
      }
    }

    if (startRes.routeFlood !== undefined) {
      appendLog(
        `Route vers la cible: ${startRes.routeFlood
          ? "FLOOD (chemin direct inconnu — les réponses de la cible gardent le délai standard de 300ms !)"
          : "directe"}`
      );
    }

    if (!resuming) {
      const chosen = chooseFirmwareVariant(startReply, firmwareVariants);
      if (chosen) {
        applyFirmwareVariant(chosen, targetSupportsGzip(startReply) ? "cible avec décompression" : "cible gz=0, bin brut");
      }
    }

    // En reprise, le BEGIN a déjà été accepté par la session en cours : le
    // renvoyer répondrait "already running" et ferait échouer l'OTA (y compris
    // à offset 0).
    if (!resuming) {
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
      if (!isOkOtaReply(beginRes.replyText)) {
        const rep = String(beginRes.replyText || "");
        const low = rep.toLowerCase();
        if (low.includes("unsupported") || low.includes("unknown")) {
          appendLog("Binary OTA begin unsupported by target.");
          return null;
        }
        throw new Error(`OTA begin failed (binary): ${rep}`);
      }
    }

    // Pacing adaptatif de l'alimentation, par recherche de la frontière code=3.
    //
    // La file TX du companion se draine à un débit fixe (~48 ms/paquet, mécanique
    // interne invariante). Le plancher (airtime pur) ne s'atteint QUE si la file
    // n'est jamais vide — donc si le client alimente au moins aussi vite que le
    // drain, ce qui implique de toucher code=3 de temps en temps. code=3 n'est
    // PAS l'ennemi : un companion inactif l'est (airtime perdu, jamais rattrapé).
    //
    // La courbe est asymétrique : sous le drain, la file reste pleine et code=3
    // cadence (léger surcoût de RTT ratés) ; AU-DESSUS du drain, la file se vide
    // et l'idle explose (falaise). Comme code=3 tombe à zéro *à la fois* au drain
    // exact et au-dessus, on ne peut pas distinguer les deux — on vise donc juste
    // EN DESSOUS : toute fenêtre sans aucun code=3 = risque d'idle -> on raccourcit
    // le gap ; une fenêtre trop congestionnée -> on l'allonge ; entre les deux, on
    // tient. Converge juste sous le débit de drain, quel que soit le preset.
    //
    // (Le NACK sélectif a supprimé les checkpoints fréquents qui servaient de
    // purge implicite de la file, d'où ce pacing explicite.)
    const GAP_MIN_MS = 4;
    const GAP_MAX_MS = 120;
    const PACE_WINDOW = 24;
    let dynamicGapMs = noAckGapMs;
    let paceWindowCount = 0;
    let paceWindowCongested = 0;

    while (offset < fwSize) {
      if (otaCancelRequested) {
        // Abort confirmé (avec retry et fallback texte) : un ABORT perdu
        // laisserait la session cible armée et son profil horloge OTA actif.
        await abortTargetOtaSession(targetHex, targetFullKeyHex, "annulation");
        shouldCleanupTargetOta = false;
        throw new Error("OTA annulée.");
      }

      // Never straddle a staging boundary. If the preceding block has a hole,
      // the target cannot flush it yet and would otherwise have to discard the
      // tail of this chunk in the following block, causing a small classic
      // rewind even after a successful selective repair.
      const bytesToFlushBoundary = targetFlushBlock - (offset % targetFlushBlock);
      const chunkLen = Math.min(chunkSize, fwSize - offset, bytesToFlushBoundary);
      const chunk = firmware.subarray(offset, offset + chunkLen);
      const isLastChunk = offset + chunkLen >= fwSize;
      const crossesFlushBoundary =
        Math.floor((offset + chunkLen) / targetFlushBlock) !== Math.floor(offset / targetFlushBlock);
      const checkpointDue = crossesFlushBoundary
        || (selectiveNackCapability !== true && chunksSinceAck + 1 >= ackEvery)
        || isLastChunk;
      const writePayload = concatBytes(
        u32ToBytesLE(offset),
        Uint8Array.of(chunkLen & 0xff),
        chunk
      );

      const writeOffset = offset;
      const writeStartMs = performance.now();
      let writeOk = false;
      let failure = "write failed";

      // ERR_CODE_TABLE_FULL (code=3) : la file TX du companion est pleine —
      // contrôle de flux purement local, elle se draine à ~48 ms/paquet émis.
      // Ces attentes ne consomment PAS de tentative radio : elles sont bornées
      // par une échéance murale, sinon un lot long (ack adaptatif élevé) fait
      // échouer le transfert alors que ni la radio ni la cible ne sont en
      // cause (vu sur le terrain : abort à ack=48 avec cible parfaitement en
      // phase).
      const busyDeadlineMs = writeStartMs + Math.max(8000, (Number(checkpointTimeoutMs) || 1500) * 4);
      let busyWaits = 0;
      let attempt = 0;

      while (!otaCancelRequested) {
        chunkWriteAttempts += 1;
        const attemptStartMs = performance.now();
        const writeRes = await client.sendOtaBinaryCmd(
          targetFullKeyHex,
          OTA_BIN_OP.WRITE,
          writePayload,
          false,
          250,
          100
        );
        if (!writeRes.error) {
          const rtt = performance.now() - attemptStartMs;
          writeRttSumMs += rtt;
          writeRttCount += 1;
          if (rtt > writeRttMaxMs) writeRttMaxMs = rtt;
          writeOk = true;
          break;
        }

        failure = writeRes.error || failure;

        const isLocalBusy = String(writeRes.error || "").includes("code=3");
        if (isLocalBusy && performance.now() < busyDeadlineMs) {
          busyWaits += 1;
          const busySleep = otaBusyRetryDelay(noAckGapMs, busyWaits);
          otaBusyStats.events += 1;
          otaBusyStats.sleepMs += busySleep;
          await sleep(busySleep);
          continue;
        }

        chunkWriteFailures += 1;
        updateLiveMetrics("binary_req", offset, fwSize, tStart, chunkWriteAttempts, chunkWriteFailures, chunkServerRejects);

        attempt += 1;
        const st = await getOtaStatusBinary(targetFullKeyHex, 1, quickStatusTimeout);
        if (st.status && st.status.total === fwSize && st.status.done <= fwSize) {
          const srvOffset = st.status.done;
          if (srvOffset !== offset) {
            if (srvOffset < offset) {
              chunkServerRejects += estimateRejectedChunks(offset, srvOffset, chunkSize);
              noteResync(offset, srvOffset, "échec write", st.reply);
            } else {
              appendLog(`Serveur en avance (${srvOffset}/${fwSize}), rattrapage sans pénalité`);
            }
            offset = srvOffset;
            chunkIndex = Math.floor(offset / chunkSize);
            chunksSinceAck = 0;
            writeOk = true;
            break;
          }
          // Serveur en phase à notre offset : le chunk n'est jamais parti
          // (busy prolongé) ou s'est perdu en l'air — rejouable tel quel.
          // Le RTT du STATUS (~850 ms) a en plus laissé la file se drainer.
        }

        if (attempt < writeMaxRetries) {
          appendLog(`Retry write (binary) at offset ${offset} (${attempt}/${writeMaxRetries}): ${failure}`);
          await sleep(120 * attempt);
          continue;
        }

        if (!st.error && st.reply) {
          failure = `${failure}; status=${st.reply}`;
        }
        break;
      }

      if (!writeOk) {
        if (otaCancelRequested) {
          await abortTargetOtaSession(targetHex, targetFullKeyHex, "annulation");
          shouldCleanupTargetOta = false;
          throw new Error("OTA annulée.");
        }
        throw new Error(`OTA write failed (binary) at offset ${offset}: ${failure}`);
      }

      if (offset === writeOffset) {
        offset += chunkLen;
        chunkIndex += 1;
        chunksSinceAck += 1;

        // Recherche de la frontière code=3 sur une fenêtre glissante. Zéro
        // congestion sur toute la fenêtre = on est au drain ou au-dessus (idle
        // possible) -> on raccourcit ; congestion majoritaire = on sature trop
        // -> on allonge ; entre les deux = point de fonctionnement, on tient.
        if (busyWaits > 0) paceWindowCongested += 1;
        if (++paceWindowCount >= PACE_WINDOW) {
          if (paceWindowCongested === 0) {
            dynamicGapMs = Math.max(GAP_MIN_MS, dynamicGapMs - 4);
          } else if (paceWindowCongested >= PACE_WINDOW * 0.5) {
            dynamicGapMs = Math.min(GAP_MAX_MS, dynamicGapMs + 3);
          }
          paceWindowCount = 0;
          paceWindowCongested = 0;
        }

        if (!checkpointDue && dynamicGapMs > 0) {
          // Le round-trip série (attente du msg_sent) et les attentes code=3
          // font déjà partie de l'intervalle : on ne dort que le reliquat, pas
          // en plus. Un chunk qui a busy-waité au-delà du gap ne dort donc pas.
          const gapLeft = dynamicGapMs - (performance.now() - writeStartMs);
          if (gapLeft > 0) {
            feedGapSleepMs += gapLeft;
            await sleep(gapLeft);
          }
        }
      } else {
        continue;
      }

      if (checkpointDue) {
        let checkpointOk = false;
        failure = "checkpoint status missing";
        const checkpointStartMs = performance.now();

        for (let attempt = 1; attempt <= checkpointMaxRetries; attempt += 1) {
          const st = await getOtaStatusBinary(
            targetFullKeyHex,
            1,
            Math.max(1500, checkpointTimeoutMs, statusTimeout)
          );
          if (st.status && st.status.total === fwSize && st.status.done <= fwSize) {
            let srvDone = st.status.done;

            if (selectiveNackCapability === null) {
              selectiveNackCapability = st.status.selectiveNack;
              appendLog(selectiveNackCapability
                ? "NACK sélectif: support annoncé par la cible (nack=miss)."
                : "NACK sélectif: non annoncé par la cible; les pertes utiliseront le rejeu classique.");
            }

            // NACK sélectif : la cible a gardé les chunks arrivés après le(s)
            // trou(s) et nous liste les octets manquants — on ne réémet que
            // ça. Si la liste est vide ou absente (cible ancienne), on
            // retombe sur le rembobinage classique à 'done' ci-dessous.
            let missing = (st.status.missing || [])
              .filter((r) => r.off >= 0 && r.len > 0 && r.off + r.len <= fwSize);
            let repairRounds = 0;
            while (missing.length > 0 && repairRounds < 6 && !otaCancelRequested) {
              repairRounds += 1;
              const holeBytes = missing.reduce((a, r) => a + r.len, 0);
              appendLog(
                `Réparation ciblée: ${missing.length} trou(s), ${holeBytes} octets `
                + `(round ${repairRounds}, done ${srvDone}/${fwSize}) | status=${st.reply}`
              );
              await resendMissingRanges(missing);
              const st2 = await getOtaStatusBinary(targetFullKeyHex, 1, quickStatusTimeout);
              if (!(st2.status && st2.status.total === fwSize && st2.status.done <= fwSize)) {
                break;
              }
              const newMissing = (st2.status.missing || [])
                .filter((r) => r.off >= 0 && r.len > 0 && r.off + r.len <= fwSize);
              const newHoleBytes = newMissing.reduce((a, r) => a + r.len, 0);
              const progressed = st2.status.done > srvDone || newHoleBytes < holeBytes;
              srvDone = st2.status.done;
              missing = newMissing;
              if (!progressed) break;  // le rembobinage classique prendra le relais
            }

            // behind = des chunks ont réellement été perdus en l'air.
            // ahead  = le serveur avait déjà reçu ce qu'on croyait perdu (course
            //          entre chunks en vol et resync arrière) : c'est du progrès,
            //          aucune pénalité à appliquer.
            const behind = srvDone < offset;
            const ahead = srvDone > offset;
            if (behind) {
              chunkServerRejects += estimateRejectedChunks(offset, srvDone, chunkSize);
              noteResync(offset, srvDone, "checkpoint", st.reply);
            } else if (ahead) {
              appendLog(`Serveur en avance (${srvDone}/${fwSize}), rattrapage sans pénalité`);
            }
            offset = srvDone;
            chunkIndex = Math.floor(offset / chunkSize);
            chunksSinceAck = 0;
            checkpointOk = true;

            // ack_every adaptatif (AIMD borné) : pertes -> on divise par 2 et le
            // plafond descend d'un cran ; lien propre -> on remonte après 3
            // checkpoints propres, et le plafond lui-même se relève après 5
            // checkpoints propres consécutifs au plafond. Une perte RF sporadique
            // ne condamne donc plus la session (l'ancien plafond irréversible
            // faisait s'effondrer ack jusqu'à 1).
            if (selectiveNackCapability !== true) {
              if (behind) {
                cleanCheckpoints = 0;
                if (ackEvery > ACK_EVERY_MIN) {
                  ackEvery = Math.max(ACK_EVERY_MIN, Math.floor(ackEvery / 2));
                  ackEveryCeiling = Math.max(ACK_EVERY_MIN, Math.min(ackEveryCeiling, ackEvery * 2));
                  appendLog(`Ack adaptatif: réduit à ${ackEvery} (pertes, plafond ${ackEveryCeiling})`);
                }
              } else {
                cleanCheckpoints += 1;
                if (ackEvery < ackEveryCeiling) {
                  if (cleanCheckpoints >= 3) {
                    ackEvery = Math.min(ackEveryCeiling, ackEvery * 2);
                    cleanCheckpoints = 0;
                    appendLog(`Ack adaptatif: augmenté à ${ackEvery} (lien propre)`);
                  }
                } else if (ackEveryCeiling < ACK_EVERY_MAX && cleanCheckpoints >= 5) {
                  ackEveryCeiling = Math.min(ACK_EVERY_MAX, ackEveryCeiling * 2);
                  ackEvery = Math.min(ackEveryCeiling, ackEvery * 2);
                  cleanCheckpoints = 0;
                  appendLog(`Ack adaptatif: plafond relevé à ${ackEveryCeiling}, ack ${ackEvery}`);
                }
              }
            }
            break;
          }

          failure = st.error || (st.reply ? `status=${st.reply}` : "invalid status response");
          if (attempt < checkpointMaxRetries) {
            appendLog(`Retry checkpoint status (binary) at offset ${offset} (${attempt}/${checkpointMaxRetries}): ${failure}`);
            updateLiveMetrics("binary_req", offset, fwSize, tStart, chunkWriteAttempts, chunkWriteFailures, chunkServerRejects);
            await sleep(120 * attempt);
          }
        }

        if (!checkpointOk) {
          throw new Error(`OTA checkpoint failed (binary) at offset ${offset}: ${failure}`);
        }
        checkpointSumMs += performance.now() - checkpointStartMs;
        checkpointCount += 1;
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
    if (!isOkOtaReply(endRes.replyText)) {
      throw new Error(`OTA end failed (binary): ${endRes.replyText}`);
    }
    shouldCleanupTargetOta = false;

    if (writeRttCount > 0) {
      appendLog(
        `Timing: write RTT moyen ${(writeRttSumMs / writeRttCount).toFixed(1)}ms `
        + `(max ${writeRttMaxMs.toFixed(0)}ms, ${writeRttCount} mesures) | `
        + `checkpoints ${checkpointCount} x ${checkpointCount > 0 ? (checkpointSumMs / checkpointCount).toFixed(0) : 0}ms `
        + `(total ${(checkpointSumMs / 1000).toFixed(1)}s) | `
        + `pacing ${(feedGapSleepMs / 1000).toFixed(1)}s (gap final ${dynamicGapMs}ms) | `
        + `backpressure code=3: ${otaBusyStats.events} attentes, ${(otaBusyStats.sleepMs / 1000).toFixed(1)}s | `
        + `NACK ciblé: ${repairedChunks} chunk(s)/${repairedBytes} octets`
        + `${repairSendFailures ? ` (${repairSendFailures} échec(s))` : ""} | `
        + `rejeu classique: ${classicReplayChunks} chunk(s)/${classicReplayBytes} octets, ${resyncEvents} resync`
      );
    }

    await sleep(1000);
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
  } catch (e) {
    if (shouldCleanupTargetOta && !otaCancelRequested) {
      await abortTargetOtaSession(targetHex, targetFullKeyHex, "binary failure");
    }
    throw e;
  }
}

async function runTextOta(params) {
  const {
    targetHex,
    firmwareVariants,
    chunkSizeInput,
    ackEveryInput,
    noAckGapMs,
    checkpointTimeoutMs,
    statusTimeoutMs,
    ackSettleGapMs,
    radioProfile,
  } = params;
  let firmware = params.firmware;
  let firmwareMd5 = params.firmwareMd5;

  if (chunkSizeInput < 16 || chunkSizeInput > 80) {
    throw new Error("chunk_size doit être entre 16 et 80 en mode texte");
  }
  if (ackEveryInput < 1 || ackEveryInput > 64) {
    throw new Error("ack_every doit être entre 1 et 64");
  }
  if (!firmware || firmware.length === 0) {
    throw new Error("firmware vide");
  }
  if (!/^[0-9a-f]{32}$/i.test(String(firmwareMd5 || ""))) {
    throw new Error("md5 firmware invalide");
  }

  const targetPrefix = client.normalizeTargetPrefix(targetHex);
  let fwSize = firmware.length;
  let chunkSize = chunkSizeInput;
  let ackEvery = ackEveryInput;

  const computeSafeChunkLimit = () => {
    const limit = maxOtaChunkForOffset(Math.max(0, fwSize - 1));
    if (limit < 16) {
      throw new Error(`transport texte trop limité pour OTA (chunk max sécurisé ${limit})`);
    }
    if (chunkSize > limit) {
      appendLog(`Chunk size ajusté de ${chunkSize} à ${limit} (limite transport texte).`);
      chunkSize = limit;
    }
  };
  computeSafeChunkLimit();

  let totalChunks = estimateOtaChunkCount(fwSize, chunkSize);
  if (totalChunks <= 0) {
    throw new Error("impossible de calculer le plan de chunks");
  }

  // Voir runBinaryOta : la forme (gzip ou brute) est choisie après la sonde.
  const applyFirmwareVariant = (variant, why) => {
    if (!variant) return;
    if (firmware !== variant.bytes) {
      firmware = variant.bytes;
      firmwareMd5 = variant.md5;
      fwSize = firmware.length;
      computeSafeChunkLimit();
      totalChunks = estimateOtaChunkCount(fwSize, chunkSize);
    }
    appendLog(`Firmware retenu: ${variant.format} ${fwSize} bytes (md5=${firmwareMd5}) [${why}]`);
  };

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
  let shouldCleanupTargetOta = false;
  let resuming = false;

  // Voir runBinaryOta : coupe court aux boucles de resynchronisation sans progrès.
  let lastResyncOffset = -1;
  let resyncStalls = 0;
  const noteResync = (newOffset) => {
    resyncStalls = newOffset <= lastResyncOffset ? resyncStalls + 1 : 0;
    lastResyncOffset = newOffset;
    appendLog(`Resync offset -> ${newOffset}/${fwSize}`);
    if (resyncStalls >= 5) {
      throw new Error(`OTA bloquée: resynchronisations répétées sans progression à l'offset ${newOffset}`);
    }
  };

  updateLiveMetrics("text_cmd", offset, fwSize, tStart, chunkWriteAttempts, chunkWriteFailures, chunkServerRejects);

  try {
    let startRes = await client.sendRepeaterCmdAndWaitReply(targetHex, "start ota", startTimeoutSec);
    if (startRes.error) {
      throw new Error(`Start OTA failed: ${startRes.error}`);
    }

    let startReply = String(startRes.reply || "");
    if (!isOkOtaReply(startReply)) {
      if (startReply.toLowerCase().includes("already running")) {
        const st = await getOtaStatus(targetHex, statusTimeoutSec);
        let resumeVariant = (st.status && st.status.done <= st.status.total)
          ? matchFirmwareVariantBySize(firmwareVariants, st.status.total)
          : null;
        if (!resumeVariant && st.status && st.status.total === fwSize && st.status.done <= fwSize) {
          resumeVariant = { bytes: firmware, md5: firmwareMd5, format: "forme courante" };
        }
        if (resumeVariant) {
          applyFirmwareVariant(resumeVariant, "reprise de session");
          offset = st.status.done;
          chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
          resuming = true;
          appendLog(`Reprise de session OTA à l'offset ${offset}/${fwSize}`);
        } else {
          appendLog(
            `Text OTA: session précédente incompatible, reset requis`
            + `${st.error ? ` (${st.error})` : st.reply ? ` (${st.reply})` : ""}.`
          );
          const resetOk = await abortTargetOtaSession(targetHex, "", "restart text");
          if (!resetOk) {
            throw new Error(
              `Start OTA failed: ${startReply}`
              + `${st.error ? `; ${st.error}` : st.reply ? `; status=${st.reply}` : ""}`
            );
          }
          startRes = await client.sendRepeaterCmdAndWaitReply(targetHex, "start ota", startTimeoutSec);
          if (startRes.error) {
            throw new Error(`Start OTA failed after abort: ${startRes.error}`);
          }
          startReply = String(startRes.reply || "");
          if (!isOkOtaReply(startReply)) {
            throw new Error(`Start OTA failed after abort: ${startReply}`);
          }
        }
      } else {
        throw new Error(`Start OTA failed: ${startReply}`);
      }
    }

    shouldCleanupTargetOta = true;

    if (!resuming) {
      const chosen = chooseFirmwareVariant(startReply, firmwareVariants);
      if (chosen) {
        applyFirmwareVariant(chosen, targetSupportsGzip(startReply) ? "cible avec décompression" : "cible gz=0, bin brut");
      }
    }

    // En reprise, la session a déjà accepté son BEGIN : le renvoyer ferait
    // échouer l'OTA ("already running"), y compris à offset 0.
    if (!resuming) {
      let beginCmd = `ota begin ${fwSize} ${firmwareMd5} ${ackEvery}`;
      let beginRes = await client.sendRepeaterCmdAndWaitReply(targetHex, beginCmd, beginTimeoutSec);
      if (!beginRes.error && !isOkOtaReply(beginRes.reply) && ackEvery > 1) {
        const r = String(beginRes.reply || "").toLowerCase();
        if (r.includes("too many args") || r.includes("bad ack")) {
          appendLog("Target ne supporte pas ack_every sur begin, fallback à ack_every=1.");
          ackEvery = 1;
          beginCmd = `ota begin ${fwSize} ${firmwareMd5}`;
          beginRes = await client.sendRepeaterCmdAndWaitReply(targetHex, beginCmd, beginTimeoutSec);
        }
      }
      if (beginRes.error) {
        throw new Error(`OTA begin failed: ${beginRes.error}`);
      }
      if (!isOkOtaReply(beginRes.reply)) {
        throw new Error(`OTA begin failed: ${beginRes.reply}`);
      }
    }

    while (offset < fwSize) {
      if (otaCancelRequested) {
        // Abort confirmé : un "ota abort" perdu laisserait la session cible armée.
        await abortTargetOtaSession(targetHex, "", "annulation");
        shouldCleanupTargetOta = false;
        throw new Error("OTA annulée.");
      }

      const pendingOffsetErr = client.popLatestOffsetError(targetPrefix);
      if (pendingOffsetErr && pendingOffsetErr.expected !== offset && pendingOffsetErr.expected <= fwSize) {
        chunkWriteFailures += 1;
        chunkServerRejects += estimateRejectedChunks(offset, pendingOffsetErr.expected, chunkSize);
        offset = pendingOffsetErr.expected;
        chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
        chunksSinceAck = 0;
        noteResync(offset);
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
        let sendErr = await client.sendRepeaterCmdNoReply(targetHex, chunkCmd);
        // code=3 : file TX du companion pleine — backpressure locale, on
        // laisse drainer (~48 ms/paquet) et on resoumet le même chunk, borné
        // par une échéance murale. Voir le même traitement en binaire.
        if (sendErr && String(sendErr).includes("code=3")) {
          const busyDeadlineMs = performance.now() + Math.max(8000, (Number(checkpointTimeoutMs) || 1500) * 4);
          let busyWaits = 0;
          while (sendErr && String(sendErr).includes("code=3")
                 && performance.now() < busyDeadlineMs && !otaCancelRequested) {
            busyWaits += 1;
            const busySleep = otaBusyRetryDelay(noAckGapMs, busyWaits);
            otaBusyStats.events += 1;
            otaBusyStats.sleepMs += busySleep;
            await sleep(busySleep);
            chunkWriteAttempts += 1;
            sendErr = await client.sendRepeaterCmdNoReply(targetHex, chunkCmd);
          }
        }
        if (sendErr) {
          chunkWriteFailures += 1;
          updateLiveMetrics("text_cmd", offset, fwSize, tStart, chunkWriteAttempts, chunkWriteFailures, chunkServerRejects);
          const st = await getOtaStatus(targetHex, statusTimeoutSec);
          if (st.status && st.status.done !== offset) {
            chunkServerRejects += estimateRejectedChunks(offset, st.status.done, chunkSize);
            offset = st.status.done;
            chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
            chunksSinceAck = 0;
            noteResync(offset);
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
          noteResync(offset);
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
          const okReply = reply === "" || isOkOtaReply(reply);

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
              noteResync(offset);
              break;
            }
          }

          failure = writeRes.error || reply || failure;
          if (attempt < checkpointMaxRetries) {
            appendLog(`Retry checkpoint at offset ${offset} (${attempt}/${checkpointMaxRetries})`);
            updateLiveMetrics("text_cmd", offset, fwSize, tStart, chunkWriteAttempts, chunkWriteFailures, chunkServerRejects);
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
              noteResync(offset);
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
    if (!isOkOtaReply(endRes.reply)) {
      throw new Error(`OTA end failed: ${endRes.reply}`);
    }
    shouldCleanupTargetOta = false;

    await sleep(1000);
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
  } catch (e) {
    if (shouldCleanupTargetOta && !otaCancelRequested) {
      await abortTargetOtaSession(targetHex, "", "text failure");
    }
    throw e;
  }
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
    appendLog("Binary OTA non possible (clé complète non résolue), fallback texte.");
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
    ui.selfName.textContent = `Nœud: ${selfInfo.name || "-"}`;
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
  ui.selfName.textContent = "Nœud: inconnu";
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
      appendLog(`Répéteurs connus: ${repeaters}`);
    }
    return contacts;
  } catch (e) {
    resetTargetRepeaterSelect();
    if (logToConsole) appendLog(`Lecture contacts échouée: ${e.message}`);
    return {};
  }
}

async function initClientSession(baudrate) {
  const selfInfo = await syncSelfInfoFromNode(true);

  await refreshKnownRepeaters(true);

  updateSelfInfoUi(selfInfo, true);
  if (ui.autoTune?.checked) recalcAutoFromCurrentSelection("connect");

  const mode = getTransportLabel(client.transport);
  const usesBaud = client.transport !== "web_ble" && client.transport !== "web_tcp";
  const speedSuffix = usesBaud ? ` (${baudrate} bps)` : "";
  setConnectionStatus(`Connecté ${mode}${speedSuffix}`, "ok");
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
    throw new Error("Durée temp invalide (1-240 minutes)");
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

// --- Coordination robuste du basculement de preset radio temporaire (OTA) ---
// L'ancien schéma basculait le companion local sur des sleep fixes en devinant
// l'instant de bascule de la cible (réception variable + délai firmware de
// 2000ms, futureMillis(2000) dans MyMesh.cpp). Sur un nœud lointain, tout écart
// laissait les deux radios sur des presets différents : l'OTA ne démarrait
// jamais et la cible restait bloquée sur le preset court jusqu'au timeout du
// tempradio. On attend désormais l'ACK CLI de la cible (émis sur l'ANCIEN
// preset avant la bascule) puis on sonde la cible EN BOUCLE sur le nouveau
// preset jusqu'à réponse effective, plutôt que de deviner le timing.
const TEMP_RADIO_ACK_TIMEOUT_SEC = 6;     // attente bornée de l'ACK "OK - temp params"
const TEMP_RADIO_SWITCH_SETTLE_MS = 2600; // délai firmware futureMillis(2000) + marge
const TEMP_RADIO_PROBE_WINDOW_MS = 14000; // fenêtre de sonde par tentative
const TEMP_RADIO_MAX_ATTEMPTS = 3;        // renvois tempradio avant abandon propre

// Sonde la cible SUR LE PRESET TEMPORAIRE en boucle jusqu'à obtenir une réponse
// (n'importe laquelle — "OK", "Err - admin required", un statut… — toute
// réponse prouve la joignabilité, seul but ici) ou l'expiration de la fenêtre.
// STATUS binaire d'abord (rapide, si la clé complète est résolue), puis repli
// sur "ota status" texte (cibles sans OTA binaire). Ne modifie aucun état cible.
async function probeTargetReachableOnTemp(targetHex, probeKey, windowMs) {
  const tEnd = performance.now() + windowMs;
  let lastErr = "aucune sonde";
  while (performance.now() < tEnd) {
    if (probeKey) {
      const b = await getOtaStatusBinary(probeKey, 1, 3000);
      if (!b.error) return { ok: true, via: "binaire", reply: b.reply };
      lastErr = b.error;
      if (performance.now() >= tEnd) break;
    }
    const t = await client.sendRepeaterCmdAndWaitReply(targetHex, "ota status", 4);
    if (!t.error) return { ok: true, via: "texte", reply: t.reply };
    lastErr = t.error;
    if (performance.now() < tEnd) await sleep(300);
  }
  return { ok: false, error: lastErr };
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

  // Attendre la réponse CLI "OK - temp params" de la cible. Le firmware l'émet
  // sur l'ANCIEN preset AVANT de basculer (délai futureMillis(2000)), donc le
  // client — encore sur l'ancien preset — la reçoit. Deux bénéfices vs l'ancien
  // fire-and-forget qui devinait un temps de vol variable :
  //   1. confirmation POSITIVE que la cible a reçu la commande ;
  //   2. au retour de cet appel, msg_sent est déjà passé -> le paquet tempradio
  //      a quitté le companion, donc basculer le local ensuite ne peut plus le
  //      tronquer (plus besoin de l'ancien guard plafonné à 4s, trop court pour
  //      un nœud lointain).
  const ackRes = await client.sendRepeaterCmdAndWaitReply(targetHex, cmd, TEMP_RADIO_ACK_TIMEOUT_SEC);
  if (!ackRes.error) {
    appendLog(`Cible a confirmé le tempradio ("${String(ackRes.reply || "").trim() || "réponse reçue"}").`);
  } else {
    appendLog(
      `Pas d'ACK tempradio (${ackRes.error}) — commande possiblement perdue ; `
      + "bascule locale puis sonde de joignabilité pour trancher."
    );
  }

  // Laisser la cible exécuter sa bascule différée (~2000ms après sa réponse)
  // avant de basculer le local. La sonde en boucle du caller absorbe tout écart
  // de timing résiduel.
  appendLog(`Attente ${TEMP_RADIO_SWITCH_SETTLE_MS}ms (bascule différée de la cible)...`);
  await sleep(TEMP_RADIO_SWITCH_SETTLE_MS);

  const localRes = await client.setLocalRadioParams(preset.freq, preset.bw, preset.sf, preset.cr, 0);
  if (!localRes.ok) {
    throw new Error(`set radio client échoué: ${localRes.error}`);
  }
  applySelfRadioPresetToCache(preset);
  appendLog("Client radio preset basculé pour OTA.");

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
  appendLog("Preset radio client restauré.");
}

async function restoreTargetRadioPresetAfterFailedOta(targetHex, previousPreset) {
  if (!targetHex || !previousPreset) return;
  const cmd = `tempradio ${formatRadioValue(previousPreset.freq)},${formatRadioValue(previousPreset.bw)},`
    + `${previousPreset.sf},${previousPreset.cr},1`;
  appendLog("Échec OTA: restauration du preset radio de la cible avant celle du client…");
  const sentEvt = await client.sendMeshCmd(targetHex, cmd, true);
  if (!sentEvt || sentEvt.type === "error") {
    const code = sentEvt?.payload?.error_code;
    throw new Error(`commande cible non envoyée${code !== undefined ? ` (code=${code})` : ""}`);
  }
  const suggested = Number(sentEvt.payload?.suggested_timeout || 0);
  const dispatchGuardMs = clamp(
    (Number.isFinite(suggested) && suggested > 0 ? Math.round(suggested + 2200) : 3000),
    2200,
    5000
  );
  await sleep(dispatchGuardMs);
  appendLog("Preset radio cible restauré après échec OTA.");
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
  c.onDisconnected = () => {
    if (client !== c) return;
    setConnectionStatus("Connexion perdue", "err");
    updateButtons();
  };
}

async function connectClientViaBle() {
  if (!("bluetooth" in navigator)) {
    throw new Error("Web Bluetooth non supporté par ce navigateur");
  }
  appendLog("Tentative de connexion Web Bluetooth...");
  const device = await navigator.bluetooth.requestDevice({
    filters: [{ services: [MESHCORE_BLE_SERVICE_UUID] }],
  });
  const c = new MeshCoreSerialClient(appendLog);
  attachClientDebugHooks(c);
  await c.connectBle(device);
  client = c;
  appendLog(`BLE connecté (${device.name || "MeshCore"}).`);
}

function parseTcpTarget(raw) {
  const s = String(raw || "").trim();
  if (!s) throw new Error("Renseigne l'adresse du companion (host:port)");
  let host = s;
  let port = 5000;
  const v6 = s.match(/^\[([^\]]+)\]:(\d+)$/); // [ipv6]:port
  if (v6) {
    host = v6[1];
    port = Number(v6[2]);
  } else {
    const idx = s.lastIndexOf(":");
    // Only treat the trailing part as a port if there is a single ':'.
    if (idx > 0 && s.indexOf(":") === idx) {
      host = s.slice(0, idx);
      port = Number(s.slice(idx + 1)) || 5000;
    }
  }
  if (!host) throw new Error("hôte companion invalide");
  if (!(port > 0 && port < 65536)) throw new Error("port companion invalide");
  return { host, port };
}

async function connectClientViaTcp() {
  if (!("WebSocket" in window)) {
    throw new Error("WebSocket non supporté par ce navigateur");
  }
  const { host, port } = parseTcpTarget(ui.tcpTarget?.value);
  appendLog(`Tentative de connexion TCP via pont local (${host}:${port})...`);
  const c = new MeshCoreSerialClient(appendLog);
  attachClientDebugHooks(c);
  await c.connectTcp(host, port);
  client = c;
  appendLog(`Companion TCP connecté (${host}:${port}).`);
}

async function connectClientWithBestUsbTransport(baudrate) {
  const preference = getUsbBrowserPreference();
  let serialErr = null;
  let usbErr = null;

  if (preference === "webserial") {
    if (!("serial" in navigator)) {
      throw new Error("Web Serial non supporté par ce navigateur desktop");
    }
    appendLog("Plateforme détectée: desktop, Web Serial forcé.");
    appendLog("Tentative de connexion Web Serial...");
    const port = await navigator.serial.requestPort();
    const c = new MeshCoreSerialClient(appendLog);
    attachClientDebugHooks(c);
    await c.connectSerial(port, baudrate);
    client = c;
    appendLog("USB série connecté via Web Serial.");
    return;
  }

  if (preference === "webusb") {
    if (!("usb" in navigator)) {
      throw new Error("WebUSB non supporté par ce navigateur Android");
    }
    appendLog("Plateforme détectée: Android, WebUSB forcé.");
    appendLog("Tentative de connexion WebUSB...");
    const device = await navigator.usb.requestDevice({ filters: WEBUSB_DEVICE_FILTERS });
    const c = new MeshCoreSerialClient(appendLog);
    attachClientDebugHooks(c);
    await c.connectWebUsb(device, baudrate);
    client = c;
    appendLog(`USB connecté via WebUSB (${formatUsbDeviceLabel(device)}).`);
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
      appendLog("USB série connecté via Web Serial.");
      return;
    } catch (e) {
      serialErr = e;
      appendLog(`Web Serial indisponible: ${e.message}`);
    }
  } else {
    appendLog("Web Serial non supporté par ce navigateur.");
  }

  if (!("usb" in navigator)) {
    throw serialErr || new Error("WebUSB non supporté par ce navigateur");
  }

  appendLog("Tentative de connexion WebUSB...");
  try {
    const device = await navigator.usb.requestDevice({ filters: WEBUSB_DEVICE_FILTERS });
    const c = new MeshCoreSerialClient(appendLog);
    attachClientDebugHooks(c);
    await c.connectWebUsb(device, baudrate);
    client = c;
    appendLog(`USB connecté via WebUSB (${formatUsbDeviceLabel(device)}).`);
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
  if (mode === "tcp") {
    await connectClientViaTcp();
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
        appendLog("Web Bluetooth non supporté par ce navigateur.");
        return;
      }
    } else if (mode === "tcp") {
      if (!("WebSocket" in window)) {
        appendLog("WebSocket non supporté par ce navigateur.");
        return;
      }
    } else if (!("serial" in navigator) && !("usb" in navigator)) {
      appendLog("Ni Web Serial ni WebUSB ne sont supportés par ce navigateur.");
      return;
    }
    const baudrate = Number.parseInt(ui.baudrate.value, 10) || 115200;
    await connectClientSelectedTransport(mode, baudrate);
    await initClientSession(baudrate);
  } catch (e) {
    appendLog(`Connexion impossible: ${e.message}`);
    setConnectionStatus("Connexion échouée", "err");
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
  otaCompleted = false;
  setConnectionStatus("Non connecté");
  ui.selfName.textContent = "Nœud: -";
  ui.selfKey.textContent = "PubKey: -";
  if (ui.selfRadio) ui.selfRadio.textContent = "Radio: -";
  resetTargetRepeaterSelect();
  if (ui.targetKey) ui.targetKey.value = "";
  if (ui.autoTune?.checked) applyAutoOtaSettings("disconnect");
  appendLog("Déconnecté.");
  updateButtons();
});

ui.cancelOtaBtn.addEventListener("click", () => {
  if (!otaRunning) return;
  otaCancelRequested = true;
  setOtaStatus("Annulation demandée...");
  appendLog("Annulation OTA demandée.");
});

if (ui.targetSelect) {
  ui.targetSelect.addEventListener("change", () => {
    const selected = normalizeHex(ui.targetSelect.value || "");
    if (selected.length >= 12 && ui.targetKey) {
      ui.targetKey.value = selected;
    }
    if (selected.length >= 12) {
      appendLog(`Cible OTA sélectionnée: ${targetKeyPreview(selected)}`);
    }
    updateStepBadges();
  });
}

if (ui.rebootBtn) {
  ui.rebootBtn.addEventListener("click", async () => {
    if (!client || !client.connected) {
      showToast("Connecte d'abord un appareil", "err");
      return;
    }
    const targetHex = getSelectedTargetHex();
    if (!targetHex || targetHex.length < 12) {
      showToast("Sélectionne d'abord une cible", "err");
      return;
    }
    const label = ui.targetSelect?.options[ui.targetSelect.selectedIndex]?.textContent?.trim()
      || targetKeyPreview(targetHex);
    const confirmed = await showConfirm(`Redémarrer ${label} ?`);
    if (!confirmed) return;

    ui.rebootBtn.disabled = true;
    try {
      const res = await sendRemoteReboot(targetHex);
      if (res.error) {
        showToast(`Reboot échoué: ${res.message}`, "err");
      } else {
        showToast("Commande reboot envoyée", "ok");
      }
    } finally {
      ui.rebootBtn.disabled = false;
    }
  });
}

if (ui.clearLogBtn) {
  ui.clearLogBtn.addEventListener("click", () => {
    ui.log.value = "";
  });
}

if (ui.copyLogBtn) {
  ui.copyLogBtn.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(ui.log.value);
      showToast("Journal copié", "ok");
    } catch (e) {
      showToast(`Copie impossible: ${e.message}`, "err");
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
  appendLog("Lancer OTA: clic détecté.");
  if (!client || !client.connected) {
    appendLog(`Lancer OTA: abandon, connexion inactive (client=${Boolean(client)}, connected=${Boolean(client?.connected)}).`);
    return;
  }
  if (otaRunning) {
    appendLog("Lancer OTA: ignoré, OTA déjà en cours.");
    return;
  }

  const file = ui.firmwareFile.files && ui.firmwareFile.files[0];
  if (!file) {
    appendLog("Lancer OTA: aucun firmware sélectionné (.bin/.bin.gz/.uf2).");
    return;
  }
  appendLog(`Lancer OTA: préparation du fichier ${file.name}.`);

  let firmwareInfo;
  let firmwareVariants;
  let firmware;
  let firmwareMd5;
  try {
    firmwareInfo = await getFirmwareBytes(file);
    appendLog("Lancer OTA: calcul MD5 des formes disponibles...");
    firmwareVariants = {
      raw: firmwareInfo.rawBytes
        ? { bytes: firmwareInfo.rawBytes, md5: md5Hex(firmwareInfo.rawBytes), format: "bin" }
        : null,
      gz: firmwareInfo.gzBytes
        ? { bytes: firmwareInfo.gzBytes, md5: md5Hex(firmwareInfo.gzBytes), format: "bin.gz" }
        : null,
    };
    // Forme par défaut (métriques initiales) : gzip si disponible, comme les
    // cibles historiques ; le choix définitif se fait sur la réponse START.
    const preferred = firmwareVariants.gz || firmwareVariants.raw;
    if (!preferred) {
      throw new Error("aucune forme de firmware exploitable");
    }
    firmware = preferred.bytes;
    firmwareMd5 = preferred.md5;
    appendLog(
      "Lancer OTA: payload prêt ("
      + (firmwareVariants.raw ? `bin ${formatByteCount(firmwareVariants.raw.bytes.length)}` : "bin indisponible")
      + (firmwareVariants.gz ? `, bin.gz ${formatByteCount(firmwareVariants.gz.bytes.length)}` : ", gzip indisponible")
      + ") — choix final après sonde de la cible."
    );
  } catch (e) {
    appendLog(`Préparation firmware échouée: ${e.message}`);
    return;
  }

  otaRunning = true;
  otaCancelRequested = false;
  otaCompleted = false;
  updateButtons();
  setProgress(0);
  setProgressIndeterminate();
  setOtaStatus("Démarrage OTA…");
  ui.metricsLine.textContent = "OTA en cours…";

  let restoreClientPreset = null;
  let restoreTuningParams = null;
  let activeTargetHex = "";

  try {
    const selectedTargetHex = getSelectedTargetHex();
    if (selectedTargetHex.length < 12) {
      throw new Error("Sélectionne un répéteur cible ou renseigne une pubkey manuelle (>=12 hex).");
    }
    activeTargetHex = selectedTargetHex;

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
      firmwareVariants,
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
    if (firmwareInfo?.sourceFormat === "uf2") {
      appendLog(
        `Firmware: ${file.name} (${formatByteCount(firmwareInfo.originalSize)}) `
        + `-> bin ${formatByteCount(firmwareInfo.extractedSize)}`
        + (firmwareVariants.gz ? ` (+ forme gzip ${formatByteCount(firmwareVariants.gz.bytes.length)})` : "")
      );
    } else {
      appendLog(`Firmware: ${file.name} (source ${firmwareInfo?.sourceFormat || "bin"} ${formatByteCount(firmwareInfo?.originalSize || firmware.length)})`);
    }
    updatePlanLine(true);

    const password = String(ui.targetPassword.value || "").trim();
    if (password.length > 0) {
      setOtaStatus("Authentification...");
      appendLog("Tentative de login...");
      const fullKeyForLogin = await client.resolveTargetFullKeyForBinary(params.targetHex);
      if (!fullKeyForLogin) {
        throw new Error("Impossible de résoudre la clé complète de la cible pour login. Renseigne la clé 64 hex.");
      }
      const loginRes = await client.sendLogin(fullKeyForLogin, password);
      if (!loginRes.ok) {
        throw new Error(`Authentification échouée: ${loginRes.error}`);
      }
      appendLog(`Login OK${loginRes.is_admin ? " (admin)" : ""}`);
    }

    // Booste le budget d'airtime du companion local le temps de l'OTA (réglage
    // par USB, restauré en fin de transfert). Sans ça, le duty cycle par défaut
    // plafonne le débit radio bien en dessous de l'airtime réel des chunks.
    try {
      const tuning = await client.getTuningParams();
      if (tuning && tuning.airtime_factor > OTA_AIRTIME_FACTOR) {
        const set = await client.setTuningParams(tuning.rx_delay_base, OTA_AIRTIME_FACTOR);
        if (set.ok) {
          restoreTuningParams = tuning;
          appendLog(
            `Airtime factor companion: ${tuning.airtime_factor} -> ${OTA_AIRTIME_FACTOR} `
            + `(duty ${Math.round(100 / (1 + tuning.airtime_factor))}% -> ${Math.round(100 / (1 + OTA_AIRTIME_FACTOR))}%, durée de l'OTA)`
          );
        } else {
          appendLog(`Airtime factor companion non ajusté: ${set.error}`);
        }
      } else if (!tuning) {
        appendLog("Airtime factor companion: lecture non supportée, réglage inchangé.");
      }
    } catch (e) {
      appendLog(`Airtime factor companion non ajusté: ${e.message}`);
    }

    if (ui.useTempRadio?.checked) {
      const tempPreset = readTempRadioPresetFromUi();
      const probeKey = await client.resolveTargetFullKeyForBinary(params.targetHex);

      // Bascule + vérification EN BOUCLE sur le preset temporaire. On ne devine
      // plus l'instant de bascule de la cible : on la sonde jusqu'à réponse, et
      // si elle reste muette on rejoue toute la séquence tempradio. Après
      // épuisement des tentatives on abandonne PROPREMENT (le finally restaure
      // les presets) au lieu de lancer l'OTA à l'aveugle sur un lien désynchro
      // et de laisser la cible bloquée sur le preset court jusqu'au timeout.
      setOtaStatus("Bascule radio de la cible…");
      restoreClientPreset = await applyTempRadioPresetForOta(params.targetHex, tempPreset);

      setOtaStatus("Vérification de la cible…");
      let reached = null;
      for (let attempt = 1; attempt <= TEMP_RADIO_MAX_ATTEMPTS; attempt += 1) {
        const probe = await probeTargetReachableOnTemp(
          params.targetHex,
          probeKey,
          TEMP_RADIO_PROBE_WINDOW_MS
        );
        if (probe.ok) {
          appendLog(
            `Cible joignable sur le preset temporaire (tentative ${attempt}/${TEMP_RADIO_MAX_ATTEMPTS}, via ${probe.via}`
            + `${probe.reply ? ` : ${String(probe.reply).trim()}` : ""}).`
          );
          reached = probe;
          break;
        }
        if (attempt < TEMP_RADIO_MAX_ATTEMPTS) {
          appendLog(
            `Cible muette sur le preset temporaire (tentative ${attempt}/${TEMP_RADIO_MAX_ATTEMPTS}, ${probe.error}) : `
            + "repli du client puis renvoi de tempradio…"
          );
          setOtaStatus("Nouvelle bascule radio de la cible…");
          // Repli du local sur l'ancien preset pour que le renvoi parte là où la
          // cible écoute encore si elle n'a pas (ou pas encore) basculé.
          await restoreLocalRadioPresetAfterOta(restoreClientPreset);
          restoreClientPreset = await applyTempRadioPresetForOta(params.targetHex, tempPreset);
          setOtaStatus("Vérification de la cible…");
        }
      }
      if (!reached) {
        throw new Error(
          `Cible injoignable sur le preset temporaire après ${TEMP_RADIO_MAX_ATTEMPTS} tentatives — `
          + "OTA annulée (preset restauré ; si la cible avait basculé, elle reviendra "
          + "à l'expiration du tempradio)."
        );
      }

      if (ui.autoTune?.checked) {
        const tempAuto = recalcAutoFromCurrentSelection("temp-radio");
        if (!tempAuto) {
          throw new Error("auto tuning indisponible après application du preset temp");
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

    setOtaStatus("Démarrage du transfert OTA…");
    const result = await runOtaWithAutoTransport(params);
    appendLog("OTA staged successfully, reboot command sent.");
    appendLog(`Transport utilisé: ${result.transport === "binary_req" ? "binary req" : "text cmd"}`);
    appendLog(`Duration: ${formatDuration(result.durationSec)}`);
    appendLog(
      `Rejets chunks: total ${result.totalRejects}/${result.chunkWriteAttempts} (${result.failureRatePct.toFixed(2)}%), `
      + `send ${result.chunkWriteFailures}/${result.chunkWriteAttempts} (${result.sendFailureRatePct.toFixed(2)}%), `
      + `serveur ${result.chunkServerRejects}/${result.chunkWriteAttempts} (${result.serverRejectRatePct.toFixed(2)}%)`
    );
    otaCompleted = true;
    setProgress(100);
    setOtaStatus("OTA terminée");
    showToast("OTA terminée, reboot envoyé", "ok");
    ui.metricsLine.textContent =
      `Transport ${result.transport === "binary_req" ? "binary" : "text"} | Durée ${formatDuration(result.durationSec)} | `
      + `Rejets total ${result.totalRejects}/${result.chunkWriteAttempts} (${result.failureRatePct.toFixed(2)}%) `
      + `[send ${result.chunkWriteFailures}/${result.chunkWriteAttempts} ${result.sendFailureRatePct.toFixed(2)}%, `
      + `srv ${result.chunkServerRejects}/${result.chunkWriteAttempts} ${result.serverRejectRatePct.toFixed(2)}%]`;
  } catch (e) {
    appendLog(`OTA error: ${e.message}`);
    setOtaStatus("OTA échouée");
    showToast(`OTA échouée: ${e.message}`, "err");
    ui.metricsLine.textContent = `Erreur: ${e.message}`;
  } finally {
    ui.progressBar.classList.remove("indeterminate");
    if (restoreClientPreset && !otaCompleted && activeTargetHex) {
      try {
        await restoreTargetRadioPresetAfterFailedOta(activeTargetHex, restoreClientPreset);
      } catch (targetRestoreErr) {
        appendLog(
          `Warning: restauration preset cible échouée (${targetRestoreErr.message}); `
          + "elle reviendra automatiquement à son preset permanent à l'expiration du tempradio."
        );
      }
    }
    if (restoreTuningParams) {
      try {
        const r = await client.setTuningParams(restoreTuningParams.rx_delay_base, restoreTuningParams.airtime_factor);
        if (r.ok) {
          appendLog(`Airtime factor companion restauré (${restoreTuningParams.airtime_factor}).`);
        } else {
          appendLog(`Warning: restauration airtime factor échouée (${r.error}) — vérifie les réglages du companion.`);
        }
      } catch (tuningErr) {
        appendLog(`Warning: restauration airtime factor échouée (${tuningErr.message}) — vérifie les réglages du companion.`);
      }
    }
    if (restoreClientPreset) {
      try {
        await restoreLocalRadioPresetAfterOta(restoreClientPreset);
        if (ui.autoTune?.checked) {
          applyAutoOtaSettings("restore");
        }
      } catch (restoreErr) {
        appendLog(`Warning: restauration preset client échouée: ${restoreErr.message}`);
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
      appendLog("Auto tuning désactivé.");
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
    if (ui.useTempRadio.checked) appendLog("Preset OTA temporaire activé.");
    else appendLog("Preset OTA temporaire désactivé.");

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
    updateStepBadges();
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
    if (!ui.targetSelect) {
      updateStepBadges();
      return;
    }
    if (!manual) {
      updateStepBadges();
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
    updateStepBadges();
  });
}

setConnectionStatus("Non connecté");
setProgress(0);
resetTargetRepeaterSelect();
updateTuneInputsState();
updateTempRadioInputsState();
if (ui.autoTune?.checked) {
  recalcAutoFromCurrentSelection();
}
updatePlanLine(false);
updateButtons();
