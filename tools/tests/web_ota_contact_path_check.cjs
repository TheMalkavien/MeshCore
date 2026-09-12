// Vérifie la lecture/réécriture du chemin d'un contact par l'outil web OTA.
//
// La commande CMD_ADD_UPDATE_CONTACT réécrit l'enregistrement ENTIER côté
// companion : une trame mal alignée n'échoue pas, elle écrase silencieusement le
// nom, le type ou les droits du contact. Ce test rejoue donc les deux bouts :
//   - parseContact() sur une trame telle que MyMesh::writeContactRespFrame l'émet ;
//   - setContactOutPath() relu avec les offsets de MyMesh::updateContactFromFrame.
//
// Usage: node tools/tests/web_ota_contact_path_check.cjs

const fs = require("fs");
const path = require("path");
const vm = require("vm");
const assert = require("assert");

const APP_JS = path.join(__dirname, "..", "web_ota_otg", "app.js");

// ── Stub DOM minimal : app.js construit son objet `ui` au chargement ────────
function makeElement() {
  const el = {
    textContent: "",
    innerHTML: "",
    value: "",
    disabled: false,
    checked: false,
    hidden: false,
    className: "",
    scrollTop: 0,
    scrollHeight: 0,
    style: {},
    files: null,
    options: [],
    selectedIndex: -1,
    dataset: {},
    classList: { add() {}, remove() {}, toggle() { return false; }, contains() { return false; } },
    addEventListener() {},
    removeEventListener() {},
    appendChild() {},
    setAttribute() {},
    focus() {},
    remove() {},
    dispatchEvent() {},
    querySelector: () => makeElement(),
  };
  return el;
}

const sandbox = {
  console,
  setTimeout,
  clearTimeout,
  TextEncoder,
  TextDecoder,
  performance,
  Date,
  Math,
  URL,
  document: {
    querySelector: () => makeElement(),
    createElement: () => makeElement(),
    addEventListener() {},
  },
  navigator: {},
  WebSocket: function () {},
};
sandbox.window = sandbox;
sandbox.globalThis = sandbox;

const context = vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(APP_JS, "utf8"), context, { filename: "app.js" });

const { MeshCoreSerialClient, MAX_PATH_SIZE, OUT_PATH_UNKNOWN } =
  vm.runInContext(
    "({ MeshCoreSerialClient, MAX_PATH_SIZE, OUT_PATH_UNKNOWN })",
    context
  );

// ── Trame contact telle que le firmware l'émet (148 octets) ────────────────
// resp(1) pub(32) type(1) flags(1) out_path_len(1) out_path(64) name(32)
// last_advert(4) lat(4) lon(4) lastmod(4)
const PUB = Buffer.from("0a0c54d3".repeat(8), "hex"); // 32 octets
const NAME = "Répéteur Col";
// Un hash de saut peut légitimement valoir 0x00 : c'est exactement le cas que
// l'ancien découpage "jusqu'aux zéros de fin" perdait.
const HOPS = Buffer.from([0x3a, 0x00, 0xd7]);

function buildContactFrame(outPathLen, hops) {
  const f = Buffer.alloc(148);
  f[0] = 0x84; // RESP_CODE_CONTACT
  PUB.copy(f, 1);
  f[33] = 2; // ADV_TYPE_REPEATER
  f[34] = 0x11; // fav + remote CLI allowed
  f[35] = outPathLen & 0xff;
  if (hops) hops.copy(f, 36);
  f.write(NAME, 100, "utf8");
  f.writeUInt32LE(1757600000, 132); // last_advert
  f.writeInt32LE(45123456, 136); // lat  = 45.123456
  f.writeInt32LE(-1234567, 140); // lon  = -1.234567
  f.writeUInt32LE(1757600100, 144); // lastmod
  return new Uint8Array(f);
}

const client = new MeshCoreSerialClient(() => {});

// ── 1. Lecture : le chemin est délimité par out_path_len, pas par les zéros ─
const parsed = client.parseContact(buildContactFrame(HOPS.length, HOPS));
assert.strictEqual(parsed.out_path_len, 3, "out_path_len doit rester brut (uint8)");
assert.strictEqual(parsed.out_path, "3a00d7", "le hop 0x00 ne doit pas être tronqué");
assert.strictEqual(parsed.adv_name, NAME);
assert.strictEqual(parsed.type, 2);
assert.strictEqual(parsed.flags, 0x11);
assert.ok(Math.abs(parsed.adv_lat - 45.123456) < 1e-9, "latitude");
assert.ok(Math.abs(parsed.adv_lon + 1.234567) < 1e-9, "longitude négative");

// ── 2. Sentinelle 0xFF : aucun chemin connu ────────────────────────────────
const unknown = client.parseContact(buildContactFrame(OUT_PATH_UNKNOWN, HOPS));
assert.strictEqual(unknown.out_path_len, OUT_PATH_UNKNOWN, "0xFF doit rester 0xFF, pas -1");
assert.strictEqual(unknown.out_path, "", "aucun chemin exploitable quand out_path_len = 0xFF");

// ── 3. Écriture : offsets de MyMesh::updateContactFromFrame ────────────────
const sent = [];
client.sendCommand = async (payload) => {
  sent.push(Buffer.from(payload));
  return { type: "ok", payload: {} };
};

(async () => {
  const res = await client.setContactOutPath(parsed, 0, new Uint8Array());
  assert.ok(res.ok, `setContactOutPath: ${res.error || ""}`);
  assert.strictEqual(sent.length, 1);

  const f = sent[0];
  // updateContactFromFrame lit sans condition jusqu'à l'offset 136, puis lat/lon
  // si len >= 144. On envoie 144 : lastmod reste au nœud.
  assert.strictEqual(f.length, 144, "trame CMD_ADD_UPDATE_CONTACT = 1+32+1+1+1+64+32+4+4+4");
  assert.strictEqual(f[0], 9, "CMD_ADD_UPDATE_CONTACT");
  assert.ok(f.subarray(1, 33).equals(PUB), "pubkey préservée");
  assert.strictEqual(f[33], 2, "type préservé");
  assert.strictEqual(f[34], 0x11, "flags préservés (fav / remote CLI)");
  assert.strictEqual(f[35], 0, "out_path_len forcé à 0 = direct, zéro saut");
  assert.ok(f.subarray(36, 100).every((b) => b === 0), "out_path purgé");
  assert.strictEqual(
    f.subarray(100, 132).toString("utf8").replace(/\0+$/, ""),
    NAME,
    "nom préservé"
  );
  assert.strictEqual(f.readUInt32LE(132), 1757600000, "last_advert préservé");
  assert.strictEqual(f.readInt32LE(136), 45123456, "latitude préservée");
  assert.strictEqual(f.readInt32LE(140), -1234567, "longitude préservée");

  // ── 4. Restauration d'un chemin multi-saut ──────────────────────────────
  sent.length = 0;
  const restore = await client.setContactOutPath(parsed, 3, Buffer.from(HOPS));
  assert.ok(restore.ok);
  const g = sent[0];
  assert.strictEqual(g[35], 3, "out_path_len restauré");
  assert.ok(g.subarray(36, 39).equals(HOPS), "chemin restauré à l'identique");
  assert.ok(g.subarray(39, 100).every((b) => b === 0), "reste du champ à zéro");

  // ── 5. Restauration de la sentinelle "inconnu" ──────────────────────────
  sent.length = 0;
  const unknownRes = await client.setContactOutPath(parsed, OUT_PATH_UNKNOWN);
  assert.ok(unknownRes.ok);
  assert.strictEqual(sent[0][35], OUT_PATH_UNKNOWN, "0xFF accepté comme longueur");

  // ── 6. Longueur invalide refusée sans émettre ───────────────────────────
  sent.length = 0;
  const bad = await client.setContactOutPath(parsed, MAX_PATH_SIZE + 1);
  assert.ok(!bad.ok, "out_path_len > MAX_PATH_SIZE doit être refusé");
  assert.strictEqual(sent.length, 0, "aucune trame émise sur longueur invalide");

  console.log("web_ota_otg contact path: OK");
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
