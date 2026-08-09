# Migration distante vers le double OTA

Ce mécanisme conserve le bootloader série RP2040 déjà présent et intercale un
petit chargeur OTA LoRa immuable devant MeshCore. Le bootloader série ne valide
plus toute l'application MeshCore : il valide uniquement ce chargeur de 12 Kio,
qui valide et lance ensuite l'application. Une mise à jour LoRa peut donc
remplacer l'application sans invalider le CRC attendu par le bootloader série.

## Organisation de la flash RP2040

| Plage | Contenu |
| --- | --- |
| `0x10000000`–`0x10002fff` | bootloader série existant, jamais réécrit par cette migration |
| `0x10003000`–`0x10003fff` | secteur d'en-tête et de CRC du bootloader série |
| `0x10004000`–`0x10006fff` | chargeur OTA LoRa immuable (shim), 12 Kio |
| `0x10007000`–`0x1017efff` | application MeshCore remplaçable |
| `0x1017f000`–`0x101fefff` | LittleFS |
| `0x101ff000`–`0x101fffff` | EEPROM émulée |

Le fichier de commande LoRa propre à cette disposition s'appelle
`mcota2.cmd`. Il est volontairement incompatible avec l'ancien
`otacommand.bin`, afin qu'une ancienne commande résiduelle ne puisse pas
demander l'effacement du bootloader ou du shim.

## Cibles PlatformIO et artefacts

Construire l'installateur initial :

```text
pio run -e waveshare_rp2040_lora_repeater_dual_ota_installer
```

Le seul fichier RP2040 à transmettre au flasheur ESP32 pendant la migration est :

```text
.pio/build/waveshare_rp2040_lora_repeater_dual_ota_installer/firmware-esp32-installer.bin
```

Son fichier JSON voisin décrit le format, les adresses, les tailles et CRC ; il
sert à contrôler la construction et ne doit pas être téléversé.

Construire une mise à jour LoRa ultérieure :

```text
pio run -e waveshare_rp2040_lora_repeater_dual_ota_lora
```

Le fichier à fournir au mécanisme OTA LoRa est :

```text
.pio/build/waveshare_rp2040_lora_repeater_dual_ota_lora/firmware-lora.bin
```

Cette cible annonce `gz=0` : l'outil OTA actuel choisit donc automatiquement
la forme `.bin` brute. Elle exige aussi le MD5 de bout en bout fourni par cet
outil. Ne pas lui transmettre directement une forme `.bin.gz`.

> **Danger : ne jamais envoyer `firmware.bin` ni `firmware-lora.bin` au
> flasheur ESP32.** Ces fichiers commencent par l'application liée à
> `0x10007000`, sans le shim ni son manifeste d'installation. Le seul fichier
> accepté par le flasheur ESP32 pour cette migration est
> `firmware-esp32-installer.bin`.

## Prérequis indispensable côté ESP32

Le flasheur ESP32 doit d'abord être construit depuis la branche
`feature/rp2040-dual-ota-seal` du dépôt `RP2040_WIFI_BLE_OTA`, puis installé sur
l'ESP32 avec son OTA Wi-Fi habituel. Cette mise à jour concerne le firmware de
l'ESP32 lui-même, pas encore celui du RP2040.

Pour le companion ESP32-S3 Zero utilisé avec cette cible :

```text
pio run -e esp32s3-zero
```

Le fichier à envoyer à l'OTA Wi-Fi **de l'ESP32** est alors :

```text
.pio/build/esp32s3-zero/firmware.bin
```

Ce chemin est celui du dépôt `RP2040_WIFI_BLE_OTA`. Ne pas utiliser son
`firmware-combined.bin`, qui est une image complète app + LittleFS destinée à
un flashage intégral, ni aucun `firmware.bin` provenant du build RP2040 de
MeshCore.

Cette version reconnaît le manifeste `MC2INS01` de l'installateur. Elle écrit
l'image complète, mais demande au bootloader série de sceller uniquement les
12 Kio du shim. Avec un ancien flasheur, toute l'image serait scellée ; une mise
à jour LoRa suivante modifierait alors la zone couverte par ce CRC et le RP2040
refuserait de lancer le nouveau firmware.

Elle refuse aussi avant tout effacement un manifeste spécial invalide et un
`firmware-lora.bin` sélectionné par erreur dans l'interface Wi-Fi.

Avant la migration, vérifier que l'ESP32 mis à jour redémarre, que son interface
Wi-Fi est à nouveau joignable et que l'on dispose des deux binaires construits
localement. Sauvegarder aussi la configuration du nœud si possible et prévoir
une alimentation stable. La première installation n'est pas atomique : une
coupure pendant le premier effacement peut rendre l'application courante
inutilisable, même si le bootloader série reste intact.

## Séquence de migration distante

1. Mettre à jour le firmware de l'ESP32 via son OTA Wi-Fi classique avec la
   version issue de `feature/rp2040-dual-ota-seal`, puis confirmer que l'ESP32
   est revenu en ligne.
2. Depuis la CLI MeshCore du nœud encore fonctionnel, envoyer
   `start esp32ota`. La réponse attendue est
   `Waking UP ESP32Flasher to flash the firmware.`
3. Ouvrir l'interface Wi-Fi du flasheur ESP32 et sélectionner exclusivement
   `firmware-esp32-installer.bin` dans le répertoire de la cible `installer`.
4. Laisser l'écriture, le contrôle CRC, le scellement et le redémarrage aller
   jusqu'au bout sans couper l'alimentation ni fermer l'accès au flasheur.
5. Contrôler au minimum les messages suivants :

   ```text
   Installateur Dual OTA détecté: scellement limité au shim immuable (12 Kio).
   CRC calculé : 0x...
   Scellement du firmware...
   Scellement réussi.
   ```

   L'absence du premier message signifie que le format spécial n'a pas été
   reconnu. Dans ce cas, ne pas considérer la migration comme réussie et ne pas
   tenter d'OTA LoRa ; garder l'ESP32 joignable et réinstaller le bon flasheur
   avant de recommencer avec le même installateur.
6. Attendre le retour du nœud sur le mesh, puis vérifier son identité, sa
   configuration et sa date/version de build. Une build double OTA contient le
   marqueur `MLK_DUAL_OTA` dans `FIRMWARE_BUILD_DATE`.
7. Seulement après ces contrôles, tester une mise à jour LoRa avec
   `firmware-lora.bin` produit par la cible `dual_ota_lora`. Vérifier que le nœud
   revient avec la nouvelle version après son redémarrage.

Conserver une copie de l'installateur initial et d'un `firmware-lora.bin` connu
comme fonctionnel tant que le nœud reste distant.

## Reprise après incident et retour arrière

Après l'installation réussie du shim, une commande OTA LoRa reste présente
jusqu'à ce que l'application écrite soit relue et validée. Une coupure pendant
l'écriture provoque donc normalement une nouvelle tentative au redémarrage. Le
shim ne réécrit ni le bootloader série ni sa propre zone.

Si une mise à jour LoRa démarre encore mais que la nouvelle application pose
problème, renvoyer par LoRa le dernier `firmware-lora.bin` connu comme bon. Il
n'existe pas de seconde partition applicative A/B : ce fichier conservé est le
retour arrière.

Si aucune application valide ne peut être lancée, le shim demande le mode de
récupération UART et réveille l'ESP32 par GPIO22. Dès que l'interface du
flasheur est joignable, renvoyer **le même
`firmware-esp32-installer.bin` complet**, jamais le binaire LoRa seul.

Si la toute première installation échoue avant le message `Scellement réussi.`,
ne pas redémarrer volontairement ni laisser l'ESP32 s'endormir : conserver la
session du flasheur, corriger la cause et réessayer immédiatement l'installateur
complet. Sans accès au flasheur ESP32, une panne à ce moment critique peut
nécessiter une intervention physique UART, USB ou SWD.

Revenir à l'ancienne disposition `0x10004000` réintroduirait le défaut initial
de l'OTA LoRa et n'est pas une procédure de récupération prise en charge ici.
Ne pas improviser ce retour avec un `firmware.bin`. Pour une récupération
distante, réinstaller l'installateur double OTA ; pour supprimer aussi cette
disposition, prévoir un accès physique et une image explicitement préparée à
cet effet.
