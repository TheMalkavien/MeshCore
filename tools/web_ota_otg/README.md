# MeshCore Web OTA OTG Prototype

Prototype web (single-page app) pour lancer une OTA **RP2040 ou ESP32** (Heltec V4/V3, Xiao S3/C3, ...) via un **client MeshCore connecté en USB, BLE ou TCP (companion WiFi)**.

Le prototype utilise le protocole companion MeshCore et exécute la séquence OTA distante :

- **USB** : framing série companion (`<len + payload` / `>len + payload`)
- **BLE** : frames companion **brutes** sur le service UART MeshCore/Nordic (`6E400001...`)
- **TCP** : même framing que l'USB (`<`/`>` + LE16), relayé par un **pont local** (voir *Mode TCP*). Les navigateurs n'ouvrent pas de socket TCP brute ; le pont fournit ce chaînon manquant **sans modifier le firmware companion**.

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
- **Compression gzip automatique** des `.bin` bruts — ~40-50 % de données en moins. Les deux formes (brute et gzip) sont préparées au lancement et la forme réellement envoyée est choisie d'après la capacité annoncée par la cible dans la réponse START (`gz=1`/`gz=0` ; absence du jeton = cible RP2040 historique qui décompresse nativement via le bootloader arduino-pico). Les cibles ESP32 décompressent en streaming via le tinfl du ROM ; celles qui ne le peuvent pas (`gz=0`) reçoivent le `.bin` brut automatiquement.
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

## Cibles supportées

- **RP2040** (Waveshare RP2040-LoRa, Xiao RP2040, Pico W, RAK11310) : staging LittleFS, image appliquée au reboot par le bootloader arduino-pico (gzip natif).
- **ESP32/S2/S3/C3** (Heltec V4/V3, Xiao S3/C3, ...) : écriture directe dans la partition OTA inactive (`Update.h`), bascule de partition de boot après vérification complète au `ota end`, décompression gzip en streaming via le ROM. Nécessite un schéma de partitions à deux slots OTA (cas des variants MeshCore ESP32 standard) et un firmware de cette branche déjà en place sur la cible.
- Sur ESP32, `start ota` arme la session OTA mesh (l'ancien portail WiFi ElegantOTA reste disponible en compilant avec `-D WIFI_OTA_ON_START`).

## Limitations connues

- Si tu charges un `.uf2`, l'interface web reconstruit le `.bin` puis prépare aussi sa forme gzip ; pour une cible ESP32, utilise directement le `firmware.bin` produit par PlatformIO.
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

## Mode TCP (companion WiFi) — pont local

Le companion WiFi expose son interface série sur une socket TCP brute (port `5000`
par défaut, cf. `SerialWifiInterface`). Un navigateur ne peut pas ouvrir de socket
TCP brute (et le mode TCP des companions n'expose pas de WebSocket). Le petit pont
`meshcore-ota-bridge` comble ce trou :

- il **sert cette même page** en local (`http://127.0.0.1:8080`) ;
- il expose `ws://127.0.0.1:8080/tcp?host=<ip>&port=<port>` et **relaie les octets**
  bruts ↔ la socket TCP du companion (aucun parsing, aucune modif firmware).

C'est un binaire Go **autonome, sans dépendance** (un seul `.exe` sous Windows,
équivalents macOS/Linux), buildé depuis ce dossier.

### Build

```bash
cd tools/web_ota_otg
go build -trimpath -ldflags "-s -w" -o meshcore-ota-bridge.exe .   # Windows natif
# cross-compile depuis n'importe quel OS :
GOOS=windows GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o meshcore-ota-bridge.exe .
GOOS=darwin  GOARCH=arm64 go build -trimpath -ldflags "-s -w" -o meshcore-ota-bridge-macos .
GOOS=linux   GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o meshcore-ota-bridge-linux .
```

`-trimpath` retire les chemins absolus de build (ex. `C:\Users\...`) du binaire ;
`-ldflags "-s -w"` retire la table de symboles. Les deux réduisent la surface
suspecte pour les antivirus (voir ci-dessous).

La page (`index.html` + `app.js`) est **embarquée** dans le binaire via `//go:embed` :
un seul fichier à distribuer. (En développement, `--web .` sert la page depuis le
disque pour itérer sans rebuild.)

### Antivirus : faux positif (`Trojan:Win32/*.A!ml`, etc.)

Windows Defender peut mettre le `.exe` en quarantaine avec un nom du type
`Trojan:Win32/Bearfoos.A!ml`. **C'est un faux positif connu**, pas une vraie
détection : le suffixe `!ml` signale une heuristique machine-learning (pas une
signature), et les petits binaires Go **non signés** qui ouvrent des sockets sont
un cas d'école de ce type d'alerte. Le code source est ici, entièrement auditable.

Ce qui est déjà fait côté code pour limiter le déclenchement :

- ouverture du navigateur via `explorer` (et non `rundll32`/`cmd`, des « LOLBins »
  massivement abusés par les vrais malwares et fortement pénalisés par l'heuristique) ;
- build recommandé avec `-trimpath -ldflags "-s -w"`.

Options pour l'utilisateur, de la plus simple à la plus robuste :

1. **Exclusion Defender** du dossier de build (vous compilez vous-même, vous faites
   confiance à la source) — PowerShell admin :
   `Add-MpPreference -ExclusionPath "C:\chemin\vers\tools\web_ota_otg"`
2. **Métadonnées de version** (signal de légitimité) : `versioninfo.json` est fourni.
   `go install github.com/josephspurrier/goversioninfo/cmd/goversioninfo@latest`,
   puis `go generate` (émet `resource.syso`) avant `go build`.
3. **Signaler le faux positif** à Microsoft : <https://www.microsoft.com/wdsi/filesubmission>
   (améliore la réputation du fichier pour tout le monde).
4. **Signature Authenticode** : seule solution qui *garantit* l'absence d'alerte ML,
   mais nécessite un certificat — surdimensionné pour cet usage.

### Lancer

```bash
./meshcore-ota-bridge.exe              # sert 127.0.0.1:8080 et ouvre le navigateur
./meshcore-ota-bridge.exe --lan        # aussi joignable depuis le LAN (ex. un téléphone)
./meshcore-ota-bridge.exe --addr 127.0.0.1:9000 --no-browser
```

- **Windows / macOS / Linux** : double-clic (ou lancement CLI) → le navigateur
  s'ouvre sur la page → choisir `TCP (companion WiFi)`, saisir `ip:port` du
  companion (ex. `192.168.4.1:5000`) → `Connecter TCP`.
- **Android sans PC dédié** : lancez le pont sur un PC du même WiFi avec `--lan`,
  puis ouvrez l'URL réseau affichée (`http://<ip_pc>:8080/`) depuis Chrome Android.
  Servie en `http://` sur le LAN, la page utilise `ws://` sans blocage
  *mixed-content*. (Un APK embarquant ce même pont pour un fonctionnement 100 %
  autonome sur téléphone est la suite logique, cf. le cœur Go partagé.)

## Usage

1. Choisir `USB`, `BLE` ou `TCP`.
2. Connecter le client MeshCore (en TCP : renseigner `ip:port` du companion).
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
