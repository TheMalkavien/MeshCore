// Exercice du lecteur de paquet DFU nRF52 de tools/web_ota_otg/app.js.
//
// Les fonctions sont extraites du source de l'outil web et evaluees ici, donc
// c'est bien le code livre qui est teste, pas une copie. Les jeux d'essai sont
// fabriques par dfu_zip_check.py, qui appelle ce script - ne pas le lancer
// directement.
//
// Ce qui compte: l'image tiree du .zip doit etre identique a celle tiree du
// .hex du meme build, et un paquet contenant un SoftDevice ou un bootloader
// doit etre refuse, pas envoye a 0x26000 comme une application.

const fs = require("fs");

const src = fs.readFileSync("tools/web_ota_otg/app.js", "utf8");
const start = src.indexOf("const ZIP_EOCD_SIG");
const end = src.indexOf("async function gzipBytes");
if (start < 0 || end < 0) throw new Error("zip helpers not found in app.js");
const body = src.slice(start, end);

const logs = [];
globalThis.textDecoder = new TextDecoder("utf-8", { fatal: false });
globalThis.NRF52_APP_BASE = 0x26000;
globalThis.appendLog = (m) => logs.push(m);
globalThis.formatByteCount = (n) => `${n} o`;
globalThis.parseU32LE = function (bytes, offset) {
  return (
    (bytes[offset] || 0) |
    ((bytes[offset + 1] || 0) << 8) |
    ((bytes[offset + 2] || 0) << 16) |
    ((bytes[offset + 3] || 0) << 24)
  ) >>> 0;
};

const api = eval(body + "\n({ readZipEntries, extractDfuZipImage })");

let failures = 0;
function ok(name, cond, detail) {
  console.log(`  ${cond ? "ok  " : "FAIL"}  ${name}${detail ? " — " + detail : ""}`);
  if (!cond) failures += 1;
}
async function expectThrow(name, fn, needle) {
  try {
    await fn();
    ok(name, false, "aucune erreur levee");
  } catch (e) {
    ok(name, e.message.includes(needle), e.message);
  }
}

async function main() {
  const [zipPath, refPath, deflatedPath, mixedPath, plainPath, brokenPath] = process.argv.slice(2);

  const ref = new Uint8Array(fs.readFileSync(refPath));

  const bin = await api.extractDfuZipImage(new Uint8Array(fs.readFileSync(zipPath)));
  ok("paquet DFU PlatformIO: image extraite", bin.length === ref.length, `${bin.length} o`);
  ok("paquet DFU PlatformIO: identique a l'image du .hex",
     Buffer.compare(Buffer.from(bin), Buffer.from(ref)) === 0);

  const bin2 = await api.extractDfuZipImage(new Uint8Array(fs.readFileSync(deflatedPath)));
  ok("paquet deflate (methode 8): identique",
     Buffer.compare(Buffer.from(bin2), Buffer.from(ref)) === 0);

  await expectThrow("paquet SoftDevice/bootloader refuse",
                    () => api.extractDfuZipImage(new Uint8Array(fs.readFileSync(mixedPath))),
                    "ne met a jour que");
  await expectThrow("zip quelconque refuse",
                    () => api.extractDfuZipImage(new Uint8Array(fs.readFileSync(plainPath))),
                    "manifest.json");
  await expectThrow("manifeste incoherent refuse",
                    () => api.extractDfuZipImage(new Uint8Array(fs.readFileSync(brokenPath))),
                    "absent");

  console.log(logs.map((l) => "    log: " + l).join("\n"));
  process.exit(failures ? 1 : 0);
}

main();
