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
- **Chemin direct 0 saut** (`Forcer le chemin direct`, activé par défaut) : avant toute
  commande vers la cible — login compris — l'outil réécrit le contact côté companion avec
  un chemin à zéro saut. Sans ça, `BaseChatMesh` émet en DIRECT le long du chemin déjà
  mémorisé : un chemin multi-saut envoie les paquets vers le **premier relais**, pas vers
  la cible. C'est rédhibitoire avec le preset OTA temporaire, puisque les relais restent
  sur le preset standard et n'entendent plus rien. Le chemin d'origine est restauré en fin
  d'opération (case `Restaurer le chemin d'origine`). Le bouton `Vérifier le lien` fait la
  même chose puis sonde la cible : c'est le seul test du sens **retour**, dont le
  `out_path` vit dans le répéteur et n'est pas réécrivable à distance (le firmware ne le
  purge que sur un login reçu en flood).

  Attention en lisant le code : `out_path_len` n'est **ni** un nombre d'octets **ni** un
  nombre de sauts. C'est le champ packé `path_len` de `Packet.h` — bits 0-5 = nombre de
  sauts, bits 6-7 = taille de hash moins un — et la longueur utile vaut `sauts × taille`.
  Ainsi `0x40` (64) signifie « zéro saut, hashes de 2 octets », c'est-à-dire un chemin
  **direct**, pas 64 sauts. `0xFF` reste la sentinelle « aucun chemin connu » (elle encode
  une taille de 4, invalide, donc sans collision possible). Forcer le direct conserve les
  bits de taille, pour ne pas rétrograder le contact en hashes de 1 octet.
- **Amorçage du chemin retour**, automatique et sans mot de passe. Forcer le contact ne
  règle que le sens companion → cible : le répéteur, lui, répond le long de *son* `out_path`
  pour ce client, qu'aucune commande companion ne réécrit à distance. Le firmware laisse
  une seule prise : sur une requête reçue en **flood**, `simple_repeater` répond par un
  `PATH return` (`if (packet->isRouteFlood())` dans `onPeerDataRecv`) **avant** de consulter
  sa route mémorisée — la réponse revient donc même si celle-ci est périmée. Le companion,
  en la recevant, mémorise la route et renvoie automatiquement un chemin réciproque en
  direct (`Mesh.cpp` : *send a reciprocal return path to sender*), que le répéteur range
  dans `client->out_path`. Les deux sens sont alors alignés.

  L'outil déclenche donc, avant de forcer le direct : purge du chemin (`CMD_RESET_PATH`)
  puis **une requête binaire `OTA STATUS` en flood**, inoffensive. Une commande CLI texte
  ne ferait pas l'affaire : sur `TXT_MSG`, l'ACK comme la réponse passent par
  `client->out_path` dès qu'il est connu, flood ou pas. Un login reçu en flood marche aussi
  (il remet en plus `out_path_len` à `UNKNOWN`), mais il exige un mot de passe — d'où le
  choix de la requête binaire.

  L'amorçage est rejoué **sur le preset temporaire**, où aucun relais n'écoute : un flood y
  est forcément direct, donc l'alignement des deux sens y est garanti. Sur le preset
  standard le flood peut encore emprunter un relais ; c'est justement ce que l'outil
  détecte — s'il revient avec des sauts, la cible n'est pas joignable en direct et l'OTA
  sur preset temporaire ne pourra pas aboutir.
- **Verrou d'écran** pendant l'OTA (`navigator.wakeLock`) : en USB OTG sur téléphone,
  l'extinction de l'écran endormait l'onglet en plein transfert. Repris automatiquement
  quand la page revient au premier plan ; une confirmation est demandée si on ferme
  l'onglet pendant un transfert.
- **Réglages mémorisés** d'une session à l'autre (`localStorage`) : mode de connexion,
  hôte TCP, baudrate, dernière cible, preset OTA temporaire, paramètres avancés et état du
  panneau. Le mot de passe cible et le firmware ne sont **jamais** mémorisés.
- L'encart sous le sélecteur de firmware affiche la taille **réellement transmise** : un
  `.uf2`, un `.hex` ou un `.zip` sont convertis et compressés dès la sélection, donc le
  plan OTA compte les chunks du payload final, pas ceux du fichier source.
- Journal : bouton `Enregistrer` (téléchargement direct, le copier-coller de 200 000
  caractères étant impraticable sur mobile) et défilement automatique seulement si on est
  resté en bas, pour pouvoir relire pendant un transfert.
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
- Sur ESP32, l'OTA mesh est **opt-in** : la cible doit être compilée avec `-D MESH_LORA_OTA`. Sans ce flag, `start ota` garde son comportement d'origine (point d'accès WiFi + portail ElegantOTA) et les commandes `ota ...` répondent `Err - OTA unsupported`. À ajouter dans les `build_flags` de l'env PlatformIO de la cible, par exemple :

```ini
[env:heltec_v4_repeater]
build_flags =
  ...
  -D MESH_LORA_OTA
```

## Limitations connues

- Si tu charges un `.uf2`, l'interface web reconstruit le `.bin` puis prépare aussi sa forme gzip ; pour une cible ESP32, utilise directement le `firmware.bin` produit par PlatformIO.
- Pour une cible **nRF52840** (RAK3401, WisMesh Tag), charge le `firmware.zip` (le paquet DFU standard) ou le `firmware.hex` produit par PlatformIO : c'est la seule sortie de la toolchain nRF52, et l'interface la reconvertit en binaire implanté à `0x26000`. Un `.uf2` nRF52 (famille `0xADA52840`) est également accepté. Dans le `.zip`, l'outil lit le `manifest.json` et n'accepte qu'un paquet **applicatif seul** : un paquet contenant un SoftDevice ou un bootloader est refusé, ces images n'ayant rien à faire à `0x26000`. Ces cibles **exigent** la forme gzip (`gz=1`) : l'image brute ne tient pas à côté du firmware en cours d'exécution. Compte quelques secondes de plus sur `ota end`, le temps que le noeud décompresse l'image pour la vérifier avant de l'armer.
- Web Serial dépend du support navigateur/OS. Sur Android, le prototype force plutôt WebUSB.
- Le fallback WebUSB dépend des interfaces USB exposées par le firmware companion (CDC-ACM bulk IN/OUT requis).
- Si erreur `Unable to claim interface` : Android peut déjà attacher le driver CDC système sur l'interface série USB. Dans ce cas, WebUSB navigateur ne peut pas toujours la prendre.
- L'amorçage du chemin retour suppose que la cible répond aux **requêtes binaires**
  (`REQ_TYPE_OTA_BINARY`). Sur une cible trop ancienne pour ça, le sens retour ne peut être
  réparé que par un login reçu en flood : renseigne le mot de passe, la récupération de
  chemin du login s'en charge.
- Seuls les contacts de type **répéteur** sont proposés dans la liste : `simple_repeater`
  est le seul exemple qui implémente les commandes `ota ...` (ni le room server ni le
  capteur ne les ont). Pour une cible hors liste — un build maison, par exemple — renseigne
  sa pubkey dans `Pubkey manuelle`.
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
   La ligne `Chemin vers la cible` indique le chemin mémorisé par le companion :
   vert = direct 0 saut, orange = multi-saut ou inconnu.
5. Laisser `Forcer le chemin direct (0 saut)` coché, et cliquer `Vérifier le lien`
   pour confirmer que la cible répond sans relais avant de lancer quoi que ce soit.
6. (Optionnel) renseigner `Mot de passe` pour faire un login avant OTA.
7. Sélectionner le firmware `.bin`, `.bin.gz`, `.uf2`, ou `.hex` / `.zip` (nRF52).
8. (Optionnel) activer `Preset OTA temporaire` et régler `freq,bw,sf,cr` (défaut : `869.4,250,5,5`).
9. Cliquer `Lancer l'OTA`.

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
