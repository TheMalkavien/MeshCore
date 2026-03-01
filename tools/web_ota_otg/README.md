# MeshCore Web OTA OTG Prototype

Prototype web (single-page app) pour lancer une OTA RP2040 via un **client MeshCore connecte en USB** (OTG sur Android possible).

Le prototype utilise le protocole companion serie (`<len + payload` / `>len + payload`) et execute la sequence OTA distante.

Par defaut, il tente:

1. OTA **binaire** (REQ custom `0x70`)
2. fallback OTA **texte** (`start ota`, `ota begin`, `ota write`, `ota end`)

## Ce que fait le prototype

- Connexion USB serie via Web Serial.
- Handshake `APP_START` puis `DEVICE_QUERY`.
- Envoi OTA en **transport binaire** (si support cible), sinon fallback texte.
- Mode `ack_every` (checkpoint tous les N chunks).
- Resume simple si `start ota` retourne `already running`.
- Statistiques en fin d'OTA:
  - duree
  - ratio d'echecs chunks

## Limitations connues

- En mode binaire, `ota begin` est envoye sans MD5 (size + ack_every), donc verification MD5 non active dans ce prototype.
- Web Serial depend du support navigateur/OS. Sur Android, le support peut varier selon version de Chrome.

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

1. Connecter le client MeshCore en USB.
2. Cliquer `Connecter USB`.
3. Renseigner la cible OTA (pubkey hex, min 12 chars = prefix 6 bytes).
4. Selectionner le firmware `.bin` ou `.bin.gz`.
5. Regler `chunk size`, `ack every`, `gap`, `checkpoint timeout`.
6. Cliquer `Lancer OTA`.

## Reglages de depart recommandes

- `chunk size`: 96 (binaire) ou 64 (texte)
- `ack every`: 4
- `gap no-ack`: 45 ms
- `checkpoint timeout`: 1500 ms
