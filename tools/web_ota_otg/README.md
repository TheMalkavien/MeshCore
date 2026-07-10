# MeshCore Web OTA OTG Prototype

Prototype web (single-page app) pour lancer une OTA RP2040 via un **client MeshCore connecté en USB ou BLE**.

Le prototype utilise le protocole companion MeshCore et exécute la séquence OTA distante :

- **USB** : framing série companion (`<len + payload` / `>len + payload`)
- **BLE** : frames companion **brutes** sur le service UART MeshCore/Nordic (`6E400001...`)

En mode `USB`, il tente :

1. Connexion USB via **Web Serial**
2. Fallback connexion USB via **WebUSB (CDC bulk)**
3. OTA **binaire** (REQ custom `0x70`)
4. Fallback OTA **texte** (`start ota`, `ota begin`, `ota write`, `ota end`)

## Ce que fait le prototype

- Connexion USB série via Web Serial.
- Connexion BLE via Web Bluetooth.
- Handshake `APP_START` puis `DEVICE_QUERY`.
- Envoi OTA en **transport binaire** (si support cible), sinon fallback texte.
- **MD5 de bout en bout** dans les deux transports (binaire : payload `BEGIN` de 21 octets ; texte : `ota begin <size> <md5> [ack_every]`). La cible vérifie le MD5 au `ota end`.
- **Compression gzip automatique** des `.bin` bruts (le bootloader OTA arduino-pico décompresse nativement) — ~40-50 % de données en moins et moins de pression sur la partition LittleFS de staging.
- Affiche le `Plan OTA` sous la barre de progression, ainsi que le débit (ko/s) et une estimation du temps restant.
- **`ack_every` adaptatif** en mode binaire : les checkpoints `STATUS` s'espacent automatiquement (jusqu'à 32 chunks) quand le lien est propre, et se resserrent au premier rejet. Note : en transport binaire, le firmware supprime tous les ACK d'écriture ; la valeur `ack_every` envoyée dans le `BEGIN` ne sert que de cadence initiale côté client (et de compat avec le mode texte).
- Option `Preset OTA temporaire` :
  - envoie `tempradio` à la cible avant OTA
  - bascule le client USB sur le même preset
  - restaure le preset radio du client en fin d'OTA
- Reprise de session si `start ota` retourne `already running` (le `BEGIN` n'est pas renvoyé, y compris à offset 0).
- Annulation propre : l'abort est confirmé (retry + fallback texte) pour ne pas laisser de session armée sur la cible.
- Détection de stagnation : l'OTA s'arrête avec un message clair après plusieurs resynchronisations sans progression (lien asymétrique).
- Statistiques en fin d'OTA : durée et ratio d'échecs chunks.

## Limitations connues

- Si tu charges un `.uf2`, l'interface web reconstruit le `.bin` puis le recompresse en `.gz` avant l'envoi OTA quand le navigateur le supporte, sinon elle envoie le `.bin` extrait.
- Web Serial dépend du support navigateur/OS. Sur Android, le prototype force plutôt WebUSB.
- Le fallback WebUSB dépend des interfaces USB exposées par le firmware companion (CDC-ACM bulk IN/OUT requis).
- Si erreur `Unable to claim interface` : Android peut déjà attacher le driver CDC système sur l'interface série USB. Dans ce cas, WebUSB navigateur ne peut pas toujours la prendre.
- Le mode BLE dépend du support `Web Bluetooth` du navigateur et du companion BLE. Les trames sont supposées tenir dans une notification GATT (MTU suffisant).

## Lancer en local

Depuis `MeshCore` :

```bash
cd tools/web_ota_otg
python -m http.server 8080
```

Puis ouvrir :

- `http://127.0.0.1:8080` (desktop)
- ou l'URL locale équivalente depuis ton appareil (si même réseau).

## Usage

1. Choisir `USB` ou `BLE`.
2. Connecter le client MeshCore.
3. Cliquer `Connecter`.
4. Renseigner la cible OTA (pubkey hex, min 12 chars = préfixe 6 octets).
5. (Optionnel) renseigner `Mot de passe` pour faire un login avant OTA.
6. Sélectionner le firmware `.bin`, `.bin.gz` ou `.uf2`.
7. (Optionnel) activer `Preset OTA temporaire` et régler `freq,bw,sf,cr` (défaut : `869.4,250,5,5`).
8. Cliquer `Lancer l'OTA`.

## Réglages

L'option **Auto-ajuster** (activée par défaut) calcule chunk size, ack every, gap et timeouts à partir du preset radio courant (ou du preset OTA temporaire). Les valeurs manuelles ne servent que si l'auto-tuning est désactivé.

En manuel, points de départ raisonnables :

- `chunk size` : 96–132 en binaire (les tailles 69/85/101/117 évitent le padding AES), 64 en texte
- `ack every` : 4–8 (cadence initiale, adaptée ensuite automatiquement en binaire)
- `gap no-ack` : 20–50 ms
- `checkpoint timeout` : 1000–3000 ms selon le preset radio

## Performances et limites mesurées

Chiffres relevés sur le terrain (SF5/BW250, cible RP2040 directe, ~197 Ko compressés) :

- **~1 min 40 – 1 min 45 par transfert**, soit ~1,9 ko/s effectifs. C'est le
  plancher avec un companion au firmware standard, borné par deux constantes de
  celui-ci :
  - **~48 ms par paquet émis** (airtime ~14 ms + mécanique interne du
    dispatcher : polling de fin de TX, retournement TX→RX, carrier sense).
    Invariant quel que soit le duty cycle (testé 50/80/100 %) et le SF
    (SF6 est *pire* : l'airtime monte sans réduire la mécanique fixe).
  - **Trame série limitée à 176 octets** (`MAX_FRAME_SIZE`), soit un chunk
    binaire ≤ 136 octets (40 octets d'en-tête de requête). Au-delà, la trame
    est tronquée et chaque write est rejeté silencieusement par la cible.
- Le RTT série (soumission → `msg_sent`) est négligeable : ~6 ms.
- Le coût d'un checkpoint STATUS est de ~850 ms (file du companion + délais
  radio) ; le staging 16 Ko côté cible les limite à ~13 obligatoires par
  transfert (+ le churn adaptatif en cas de pertes RF).
- La ligne `Timing:` du journal donne la décomposition exacte de chaque
  transfert (RTT writes, checkpoints, backpressure code=3).

Pour descendre significativement sous ce plancher, il faudrait modifier le
firmware du companion (cadence TX du dispatcher, taille de trame série) — hors
périmètre de cet outil, qui se limite à la configuration runtime (tuning
d'airtime temporaire, tempradio).
