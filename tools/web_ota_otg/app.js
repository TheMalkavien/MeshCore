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
  BINARY_RESPONSE: 0x8c,
};

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

class MeshCoreSerialClient {
  constructor(logger) {
    this.log = logger;
    this.port = null;
    this.reader = null;
    this.writer = null;
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
  }

  async connect(port, baudrate) {
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
    this.connected = true;
    this.rxPending = [];
    this.readLoop();
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
  }

  async readLoop() {
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
    };
    if (frame.length < 55) return payload;
    payload.public_key = bytesToHex(frame.slice(4, 36));
    payload.name = textDecoder.decode(frame.slice(55)).replace(/\0/g, "");
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
    if (!this.connected || !this.writer) {
      throw new Error("non connecte");
    }
    if (!(payload instanceof Uint8Array)) {
      payload = new Uint8Array(payload);
    }
    const frame = new Uint8Array(3 + payload.length);
    frame[0] = 0x3c;
    frame[1] = payload.length & 0xff;
    frame[2] = (payload.length >> 8) & 0xff;
    frame.set(payload, 3);
    await this.writer.write(frame);
  }

  async sendCommand(payload, expectedTypes = [], predicate = null, timeoutMs = 5000) {
    const wantsReply = Array.isArray(expectedTypes) ? expectedTypes.length > 0 : Boolean(expectedTypes);
    const waiter = wantsReply ? this.waitForEvent(expectedTypes, predicate, timeoutMs) : null;
    await this.sendFrame(payload);
    if (!waiter) return null;
    return waiter;
  }

  async sendAppStart() {
    const payload = new Uint8Array(11);
    payload[0] = 0x01;
    payload[1] = 0x03;
    payload.set(textEncoder.encode("mccli"), 2);
    return this.sendCommand(payload, ["self_info", "ok", "error"], null, 5000);
  }

  async sendDeviceQuery() {
    return this.sendCommand(Uint8Array.of(0x16, 0x03), ["device_info", "error"], null, 5000);
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
  baudrate: document.querySelector("#baudrate"),
  targetKey: document.querySelector("#targetKey"),
  firmwareFile: document.querySelector("#firmwareFile"),
  chunkSize: document.querySelector("#chunkSize"),
  ackEvery: document.querySelector("#ackEvery"),
  noAckGap: document.querySelector("#noAckGap"),
  checkpointTimeout: document.querySelector("#checkpointTimeout"),
  connectionStatus: document.querySelector("#connectionStatus"),
  otaStatus: document.querySelector("#otaStatus"),
  metricsLine: document.querySelector("#metricsLine"),
  selfName: document.querySelector("#selfName"),
  selfKey: document.querySelector("#selfKey"),
  progressBar: document.querySelector("#progressBar"),
  log: document.querySelector("#log"),
};

let client = null;
let otaRunning = false;
let otaCancelRequested = false;

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

function updateButtons() {
  const connected = Boolean(client && client.connected);
  ui.connectBtn.disabled = connected || otaRunning;
  ui.disconnectBtn.disabled = !connected || otaRunning;
  ui.startOtaBtn.disabled = !connected || otaRunning;
  ui.cancelOtaBtn.disabled = !otaRunning;
}

async function getFirmwareBytes(file) {
  const arrayBuffer = await file.arrayBuffer();
  return new Uint8Array(arrayBuffer);
}

async function getOtaStatus(targetHex) {
  const maxAttempts = 2;
  let lastErr = null;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const res = await client.sendRepeaterCmdAndWaitReply(targetHex, "ota status", 10);
    if (!res.error) {
      return { reply: res.reply, status: parseOtaStatus(res.reply), error: null };
    }
    lastErr = res.error;
    if (attempt + 1 < maxAttempts) await sleep(200 * (attempt + 1));
  }
  return { reply: null, status: null, error: lastErr };
}

async function getOtaStatusBinary(targetFullKeyHex) {
  const maxAttempts = 2;
  let lastErr = null;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const res = await client.sendOtaBinaryCmd(
      targetFullKeyHex,
      OTA_BIN_OP.STATUS,
      new Uint8Array(),
      true,
      4000,
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
    chunkSizeInput,
    ackEveryInput,
    noAckGapMs,
    checkpointTimeoutMs,
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

  const fwSize = firmware.length;
  let chunkSize = Math.min(chunkSizeInput, OTA_BINARY_MAX_CHUNK);
  const ackEvery = ackEveryInput;
  const totalChunks = Math.ceil(fwSize / chunkSize);

  appendLog(`Transport: binary req`);
  appendLog(`Firmware: ${fwSize} bytes`);
  appendLog(`Chunk size: ${chunkSize} bytes (${totalChunks} chunks)`);
  appendLog(`Ack every: ${ackEvery} chunk(s)`);

  const tStart = performance.now();
  let offset = 0;
  let chunkIndex = 0;
  let chunksSinceAck = 0;
  let chunkWriteAttempts = 0;
  let chunkWriteFailures = 0;
  let lastProgressPct = -1;

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
      const st = await getOtaStatusBinary(targetFullKeyHex);
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
    const beginPayload = concatBytes(
      u32ToBytesLE(fwSize),
      Uint8Array.of(ackEvery & 0xff)
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

    if (!checkpointDue) {
      chunkWriteAttempts += 1;
      const writeRes = await client.sendOtaBinaryCmd(
        targetFullKeyHex,
        OTA_BIN_OP.WRITE,
        writePayload,
        false,
        250,
        100
      );
      if (writeRes.error) {
        chunkWriteFailures += 1;
        const st = await getOtaStatusBinary(targetFullKeyHex);
        if (st.status && st.status.total === fwSize && st.status.done <= fwSize) {
          offset = st.status.done;
          chunkIndex = Math.floor(offset / chunkSize);
          chunksSinceAck = 0;
          appendLog(`Resync offset to ${offset}/${fwSize}`);
          continue;
        }
        throw new Error(`OTA write failed (binary) at offset ${offset}: ${writeRes.error}`);
      }

      offset += chunkLen;
      chunkIndex += 1;
      chunksSinceAck += 1;
      if (noAckGapMs > 0) await sleep(noAckGapMs);
    } else {
      let chunkSent = false;
      let failure = "checkpoint ack missing";
      const maxRetries = 3;

      for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
        chunkWriteAttempts += 1;
        const writeRes = await client.sendOtaBinaryCmd(
          targetFullKeyHex,
          OTA_BIN_OP.WRITE,
          writePayload,
          true,
          checkpointTimeoutMs,
          100
        );
        const reply = String(writeRes.replyText || "");
        const okReply = reply === "" || reply.toLowerCase().startsWith("ok");

        if (!writeRes.error && okReply) {
          offset += chunkLen;
          chunkIndex += 1;
          chunksSinceAck = 0;
          chunkSent = true;
          break;
        }

        chunkWriteFailures += 1;

        if (writeRes.error && writeRes.error.startsWith("timeout waiting binary reply")) {
          offset += chunkLen;
          chunkIndex += 1;
          chunksSinceAck = 0;
          chunkSent = true;
          break;
        }

        const offErr = parseOtaOffsetError(reply);
        if (offErr) {
          if (offErr.expected >= offset + chunkLen) {
            offset = offErr.expected;
            chunkIndex = Math.floor(offset / chunkSize);
            chunksSinceAck = 0;
            chunkSent = true;
            break;
          }
          if (offErr.expected < offset) {
            offset = offErr.expected;
            chunkIndex = Math.floor(offset / chunkSize);
            chunksSinceAck = 0;
            chunkSent = true;
            appendLog(`Resync offset to ${offset}/${fwSize}`);
            break;
          }
        }

        failure = writeRes.error || reply || failure;
        if (attempt < maxRetries) {
          appendLog(`Retry checkpoint (binary) at offset ${offset} (${attempt}/${maxRetries})`);
          await sleep(120 * attempt);
          continue;
        }

        const st = await getOtaStatusBinary(targetFullKeyHex);
        if (st.status && st.status.total === fwSize && st.status.done <= fwSize) {
          if (st.status.done >= offset + chunkLen) {
            offset = st.status.done;
            chunkIndex = Math.floor(offset / chunkSize);
            chunksSinceAck = 0;
            chunkSent = true;
            break;
          }
          if (st.status.done < offset) {
            offset = st.status.done;
            chunkIndex = Math.floor(offset / chunkSize);
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
        throw new Error(`OTA write failed (binary) at offset ${offset}: ${failure}`);
      }
      await sleep(30);
    }

    const pct = Math.floor((offset * 100) / fwSize);
    if (pct !== lastProgressPct && (pct % 2 === 0 || offset === fwSize)) {
      setProgress(pct);
      setOtaStatus(`Progress: ${pct}% (${chunkIndex}/${totalChunks})`);
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
  const failureRatePct = chunkWriteAttempts > 0
    ? (chunkWriteFailures * 100.0) / chunkWriteAttempts
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
    failureRatePct,
  };
}

async function runTextOta(params) {
  const {
    targetHex,
    firmware,
    chunkSizeInput,
    ackEveryInput,
    noAckGapMs,
    checkpointTimeoutMs,
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
  appendLog(`Firmware: ${fwSize} bytes`);
  appendLog(`Chunk size: ${chunkSize} bytes (${totalChunks} chunks)`);
  appendLog(`Ack every: ${ackEvery} chunk(s)`);

  const tStart = performance.now();
  let offset = 0;
  let chunkIndex = 0;
  let chunksSinceAck = 0;
  let chunkWriteAttempts = 0;
  let chunkWriteFailures = 0;
  let lastProgressPct = -1;

  const startRes = await client.sendRepeaterCmdAndWaitReply(targetHex, "start ota", 15);
  if (startRes.error) {
    throw new Error(`Start OTA failed: ${startRes.error}`);
  }

  let startReply = String(startRes.reply || "");
  if (!startReply.toLowerCase().startsWith("ok")) {
    if (startReply.toLowerCase().includes("already running")) {
      const st = await getOtaStatus(targetHex);
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
    let beginCmd = `ota begin ${fwSize} ${ackEvery}`;
    let beginRes = await client.sendRepeaterCmdAndWaitReply(targetHex, beginCmd, 20);
    if (!beginRes.error && !String(beginRes.reply || "").toLowerCase().startsWith("ok") && ackEvery > 1) {
      const r = String(beginRes.reply || "").toLowerCase();
      if (r.includes("too many args") || r.includes("bad ack")) {
        appendLog("Target ne supporte pas ack_every sur begin, fallback a ack_every=1.");
        ackEvery = 1;
        beginCmd = `ota begin ${fwSize}`;
        beginRes = await client.sendRepeaterCmdAndWaitReply(targetHex, beginCmd, 20);
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
        const st = await getOtaStatus(targetHex);
        if (st.status && st.status.done !== offset) {
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
      const maxRetries = 3;
      for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
        chunkWriteAttempts += 1;
        const timeoutSec = (ackEvery > 1 ? checkpointTimeoutMs : 5000) / 1000.0;
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
        if (attempt < maxRetries) {
          appendLog(`Retry checkpoint at offset ${offset} (${attempt}/${maxRetries})`);
          await sleep(120 * attempt);
          continue;
        }

        const st = await getOtaStatus(targetHex);
        if (st.status && st.status.total === fwSize && st.status.done <= fwSize) {
          if (st.status.done >= offset + chunkLen) {
            offset = st.status.done;
            chunkIndex = estimateOtaChunkIndex(offset, chunkSize);
            chunksSinceAck = 0;
            chunkSent = true;
            break;
          }
          if (st.status.done < offset) {
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
      await sleep(30);
    }

    const pct = Math.floor((offset * 100) / fwSize);
    if (pct !== lastProgressPct && (pct % 2 === 0 || offset === fwSize)) {
      setProgress(pct);
      setOtaStatus(`Progress: ${pct}% (${chunkIndex}/${totalChunks})`);
      lastProgressPct = pct;
    }
  }

  const endRes = await client.sendRepeaterCmdAndWaitReply(targetHex, "ota end", 25);
  if (endRes.error) {
    throw new Error(`OTA end failed: ${endRes.error}`);
  }
  if (!String(endRes.reply || "").toLowerCase().startsWith("ok")) {
    throw new Error(`OTA end failed: ${endRes.reply}`);
  }

  await client.sendRepeaterCmdNoReply(targetHex, "reboot");

  const durationSec = (performance.now() - tStart) / 1000.0;
  const failureRatePct = chunkWriteAttempts > 0
    ? (chunkWriteFailures * 100.0) / chunkWriteAttempts
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
  if (textParams.chunkSizeInput > 80) {
    appendLog("Fallback chunk size adjusted to 80 (text mode limit)");
    textParams.chunkSizeInput = 80;
  }
  return runTextOta(textParams);
}

ui.connectBtn.addEventListener("click", async () => {
  if (!("serial" in navigator)) {
    appendLog("Web Serial non supporte par ce navigateur.");
    return;
  }
  if (otaRunning) return;

  try {
    const baudrate = Number.parseInt(ui.baudrate.value, 10) || 115200;
    const port = await navigator.serial.requestPort();
    client = new MeshCoreSerialClient(appendLog);
    client.onEvent = (type, payload) => {
      if (type === "contact_msg_recv" && payload?.txt_type === 1) {
        const txt = String(payload.text || "").trim();
        if (txt.startsWith("[OTA]")) {
          appendLog(`RX OTA: ${payload.pubkey_prefix} ${txt}`);
        }
      }
    };

    await client.connect(port, baudrate);
    setConnectionStatus(`Connecte (${baudrate} bps)`, true);
    appendLog("USB serie connecte.");

    let selfInfo = null;
    try {
      const startEvt = await client.sendAppStart();
      if (startEvt?.type === "self_info") {
        selfInfo = startEvt.payload;
      } else if (startEvt?.type === "error") {
        appendLog(`APP_START error code=${startEvt.payload?.error_code}`);
      }
    } catch (e) {
      appendLog(`APP_START timeout: ${e.message}`);
    }

    try {
      await client.sendDeviceQuery();
    } catch {}

    if (selfInfo) {
      ui.selfName.textContent = `Noeud: ${selfInfo.name || "-"}`;
      const pub = selfInfo.public_key || "";
      ui.selfKey.textContent = `PubKey: ${pub ? pub.slice(0, 12) : "-"}`;
    } else {
      ui.selfName.textContent = "Noeud: inconnu";
      ui.selfKey.textContent = "PubKey: -";
    }
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
  setConnectionStatus("Non connecte");
  ui.selfName.textContent = "Noeud: -";
  ui.selfKey.textContent = "PubKey: -";
  appendLog("Deconnecte.");
  updateButtons();
});

ui.cancelOtaBtn.addEventListener("click", () => {
  if (!otaRunning) return;
  otaCancelRequested = true;
  setOtaStatus("Annulation demandee...");
  appendLog("Annulation OTA demandee.");
});

ui.startOtaBtn.addEventListener("click", async () => {
  if (!client || !client.connected) {
    appendLog("Pas de connexion USB.");
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

  otaRunning = true;
  otaCancelRequested = false;
  updateButtons();
  setProgress(0);
  setOtaStatus("Demarrage OTA...");
  ui.metricsLine.textContent = "OTA en cours...";

  try {
    const params = {
      targetHex: ui.targetKey.value,
      firmware,
      chunkSizeInput: Number.parseInt(ui.chunkSize.value, 10) || 64,
      ackEveryInput: Number.parseInt(ui.ackEvery.value, 10) || 1,
      noAckGapMs: Math.max(0, Number.parseInt(ui.noAckGap.value, 10) || 0),
      checkpointTimeoutMs: Math.max(200, Number.parseInt(ui.checkpointTimeout.value, 10) || 1500),
    };
    appendLog(`Firmware: ${file.name} (${firmware.length} bytes)`);

    const result = await runOtaWithAutoTransport(params);
    appendLog("OTA staged successfully, reboot command sent.");
    appendLog(`Transport utilise: ${result.transport === "binary_req" ? "binary req" : "text cmd"}`);
    appendLog(`Duration: ${formatDuration(result.durationSec)}`);
    appendLog(
      `Chunk failures: ${result.chunkWriteFailures}/${result.chunkWriteAttempts} (${result.failureRatePct.toFixed(2)}%)`
    );
    setProgress(100);
    setOtaStatus("OTA terminee");
    ui.metricsLine.textContent =
      `Transport ${result.transport === "binary_req" ? "binary" : "text"} | Duree ${formatDuration(result.durationSec)} | Echecs chunks ${result.chunkWriteFailures}/${result.chunkWriteAttempts} (${result.failureRatePct.toFixed(2)}%)`;
  } catch (e) {
    appendLog(`OTA error: ${e.message}`);
    setOtaStatus("OTA echouee");
    ui.metricsLine.textContent = `Erreur: ${e.message}`;
  } finally {
    otaRunning = false;
    otaCancelRequested = false;
    updateButtons();
  }
});

setConnectionStatus("Non connecte");
setProgress(0);
updateButtons();
