# MeshCore Web OTA OTG Prototype

Prototype web (single-page app) pour lancer une OTA RP2040 via un **client MeshCore connecte en USB ou BLE**.

Le prototype utilise le protocole companion MeshCore et execute la sequence OTA distante:

- **USB**: framing serie companion (`<len + payload` / `>len + payload`)
- **BLE**: frames companion **brutes** sur le service UART MeshCore/Nordic (`6E400001...`)

En mode `USB`, il tente:

1. Connexion USB via **Web Serial**
2. Fallback connexion USB via **WebUSB (CDC bulk)**
3. OTA **binaire** (REQ custom `0x70`)
4. fallback OTA **texte** (`start ota`, `ota begin`, `ota write`, `ota end`)

## Ce que fait le prototype

- Connexion USB serie via Web Serial.
- Connexion BLE via Web Bluetooth.
- Handshake `APP_START` puis `DEVICE_QUERY`.
- Envoi OTA en **transport binaire** (si support cible), sinon fallback texte.
- Affiche une estimation du temps restant pendant l'upload.
- Affiche le `Plan OTA` dans le journal (console) au lieu d'une ligne UI dediee.
- Mode `ack_every` (checkpoint tous les N chunks).
- Option `Preset OTA temporaire`:
  - envoie `tempradio` a la cible avant OTA
  - bascule le client USB sur le meme preset
  - restaure le preset radio du client en fin d'OTA
- Resume simple si `start ota` retourne `already running`.
- Statistiques en fin d'OTA:
  - duree
  - ratio d'echecs chunks

## Limitations connues

- En mode binaire, `ota begin` est envoye sans MD5 (size + ack_every), donc verification MD5 non active dans ce prototype.
- Si tu charges un `.uf2`, l'interface web reconstruit le `.bin` puis le recompresse en `.gz` avant l'envoi OTA quand le navigateur le supporte, sinon elle envoie le `.bin` extrait.
- Web Serial depend du support navigateur/OS. Sur Android, le prototype force plutot WebUSB.
- Le fallback WebUSB depend des interfaces USB exposees par le firmware companion (CDC-ACM bulk IN/OUT requis).
- Si erreur `Unable to claim interface`: Android peut deja attacher le driver CDC systeme sur l'interface serie USB. Dans ce cas, WebUSB navigateur ne peut pas toujours la prendre.
- Le mode BLE depend du support `Web Bluetooth` du navigateur et du companion BLE.

## Lancer en local

Depuis `MeshCore`:

```bash
cd tools/web_ota_otg
python -m http.server 8080
```

Puis ouvrir:

- `http://127.0.0.1:8080` (desktop)
- ou l'URL locale equivalente depuis ton appareil (si meme reseau).

## Usage

1. Choisir `USB` ou `BLE`.
2. Connecter le client MeshCore.
3. Cliquer `Connecter`.
4. Renseigner la cible OTA (pubkey hex, min 12 chars = prefix 6 bytes).
5. (Optionnel) renseigner `Mot de passe` pour faire un login avant OTA.
6. Selectionner le firmware `.bin`, `.bin.gz` ou `.uf2`.
7. Regler `chunk size`, `ack every`, `gap`, `checkpoint timeout`.
8. (Optionnel) activer `Preset OTA temporaire` et regler `freq,bw,sf,cr` (defaut: `869.4,250,5,5`).
9. Cliquer `Lancer OTA`.

## Reglages de depart recommandes

- `chunk size`: 64
- `ack every`: 1
- `gap no-ack`: 50 ms
- `checkpoint timeout`: 1000 ms
