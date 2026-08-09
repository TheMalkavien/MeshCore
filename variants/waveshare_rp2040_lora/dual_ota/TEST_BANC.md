# Test banc — validation avant déploiement du double OTA

Objectif : valider **sur un nœud de banc** (accès physique + Wi-Fi ESP32) les deux
hypothèses externes que le code du dépôt ne peut pas garantir seul, **avant**
tout déploiement sur un nœud distant :

- **H1 — équivalence CRC.** Le CRC scellé par le bootloader série
  (`image_header_ok`, calculé par le DMA sniffer du RP2040) doit être
  **byte-identique** au CRC-32 réfléchi logiciel (poly `0xedb88320`, init/xor
  `0xffffffff`) recalculé par le shim (`ota.c: serial_header_finalized`) et par
  l'app (`RP2040OTA.cpp: dualOTASerialHeaderFinalized`). Si faux : l'OTA LoRa
  est **silencieusement morte** après une migration pourtant réussie.
- **H2 — portée d'effacement.** Le flasheur série ESP32 historique doit effacer
  uniquement `ceil(taille_fichier / 4096)` secteurs, pas une région fixe plus
  grande. Si faux : l'étape 2 (12 Kio à `0x10004000`) **efface l'application** à
  `0x10007000`.

Un nœud de banc de la **même révision matérielle** que les nœuds cibles est
indispensable (même flasheur ESP32, mêmes broches boot/reset, même flash).

## Pré-requis banc

- Accès physique : USB/BOOTSEL **ou** SWD **ou** UART de secours, pour récupérer
  en cas d'échec (le but du banc est justement de pouvoir échouer sans risque).
- Console série RP2040 ouverte pour lire les traces `[OTA] …`
  (`RP2040_OTA_SERIAL_DEBUG=1`, activé par défaut).
- Accès Wi-Fi au flasheur ESP32 (procédure identique au terrain).
- `picotool` (ou l'outil de lecture flash de ton choix via SWD/USB) pour les
  vérifications mémoire directes ci-dessous.
- Un moyen d'envoyer les commandes CLI MeshCore au nœud (LoRa depuis un autre
  nœud, ou série).

## Étape 0 — Construire deux applications *distinctes*

Il faut pouvoir distinguer « app de l'étape 1 » de « app poussée par OTA LoRa ».

```text
pio run -e waveshare_rp2040_lora_repeater_dual_ota_migration
pio run -e waveshare_rp2040_lora_repeater_dual_ota_lora
```

Rendre la build LoRa reconnaissable : changer le nom ou un marqueur de version
(ex. `ADVERT_NAME` ou le suffixe `FIRMWARE_BUILD_DATE`) **avant** de builder la
cible `_lora`, pour que la version LoRa diffère visiblement de celle embarquée
dans `stage1`.

Contrôles pré-vol (repris du README) :

- `firmware-esp32-stage2-seal.bin` == **exactement 12288 octets** (`0x3000`) ;
- `migration_pair_crc32` identique dans les deux JSON ;
- `stage1.json.next_artifact` == `firmware-esp32-stage2-seal.bin` ;
- `stage2.json.previous_artifact` == `firmware-esp32-stage1.bin` ;
- `FLASH_SAFETY.txt` présent et cohérent.

## Étape 1 — Flasher stage1, vérifier bootable

1. `start esp32ota` sur le nœud → se connecter au flasheur Wi-Fi.
2. Téléverser **uniquement** `firmware-esp32-stage1.bin`, puis
   `CMD:PREPARE_FLASH` → `RP2040_SYNCED` → `CMD:START_FLASH` →
   `Scellement réussi.` → `Flashage terminé !`.
3. Attendre le retour du nœud sur le mesh. Noter **identité + version**
   (marqueur `MLK_DUAL_OTA`). C'est la référence « app étape 1 ».

**Contrôle intermédiaire (attendu, pas encore fini) :** à ce stade `start ota`
DOIT répondre `Err - finish ESP32 migration stage 2`
(le header série couvre encore shim+app → `serial_header_finalized()` = faux).
Si `start ota` répond déjà `OK`, **stop** : le header n'est pas dans l'état
attendu, ne pas continuer.

> ⚠️ N'effectuer AUCUNE OTA LoRa entre l'étape 1 et l'étape 2.

## Étape 2 — Flasher stage2 (scellement du shim seul)

1. Nouveau cycle complet : téléverser **uniquement**
   `firmware-esp32-stage2-seal.bin`, `CMD:PREPARE_FLASH` → `RP2040_SYNCED` →
   `CMD:START_FLASH` → `Scellement réussi.` → `Flashage terminé !`.
2. Attendre le retour sur le mesh.

### ✅ Vérification H2 — l'application a survécu

- Le nœud **redémarre sur la MÊME app qu'à l'étape 1** (même identité, même
  version `MLK_DUAL_OTA`). S'il part en recovery UART / ne revient pas, l'étape 2
  a probablement effacé l'app → **H2 est FAUSSE** (le flasheur efface une région
  fixe) : ne pas déployer, revoir le flasheur.
- Contrôle direct optionnel (`picotool save`/read) : le contenu de `0x10007000`
  est **inchangé** entre avant et après l'étape 2.

### ✅ Vérification H1 — CRC équivalent (deux méthodes, faire les deux)

**Méthode comportementale :** `start ota` DOIT maintenant répondre
`OK - OTA ready (… gz=0, nack=miss)`. Si ça répond encore
`Err - finish ESP32 migration stage 2` alors que l'étape 2 a réussi et que la
taille scellée est `0x3000`, **H1 est FAUSSE** (le CRC du bootloader n'est pas le
CRC-32 réfléchi standard) → l'OTA LoRa ne se débloquera jamais. Ne pas déployer.

**Méthode mémoire directe (preuve formelle) :** lire la flash et comparer.

```text
# En-tête série à 0x10003000 : vtor(u32 LE), size(u32 LE), crc32(u32 LE)
picotool save -r 0x10003000 0x10003010 header.bin
# Doit donner : vtor = 0x10004000, size = 0x00003000

# Shim scellé
picotool save -r 0x10004000 0x10007000 shim.bin
```

Puis vérifier que le CRC-32 réfléchi standard du shim == le mot `crc32` du header :

```python
import binascii
shim = open("shim.bin","rb").read()
assert len(shim) == 0x3000
print(hex(binascii.crc32(shim) & 0xffffffff))   # doit == crc32 lu dans header.bin (offset 8)
```

Égalité ⇒ **H1 vraie** : le CRC matériel du bootloader et le CRC logiciel du shim
coïncident. C'est la preuve indépendante du test comportemental.

## Étape 3 — OTA LoRa de bout en bout (preuve finale)

1. Pousser `firmware-lora.bin` (la build `_lora` **distincte** de l'étape 0) via
   le mécanisme OTA LoRa habituel (avec MD5 ; `gz=0`).
2. Suivre les traces `[OTA]` : staging → `OK - OTA staged, reboot now` →
   `reboot`.
3. Au redémarrage, le shim doit : trouver `mcota2.cmd`, vérifier la source (CRC +
   vecteurs), programmer `0x10007000`, relire (CRC + vecteurs), purger la
   commande, rebooter dans la **nouvelle** app.

**Attendu :** le nœud revient sur le mesh avec la **version LoRa distincte**
(≠ version étape 1). Cela valide en une passe : H1 (déblocage), la lecture
LittleFS par le shim (géométrie compatible), la programmation à la bonne adresse,
et la chaîne d'intégrité MD5+CRC.

## Étape 4 — Tests de robustesse (recommandés)

- **Coupure pendant l'écriture LoRa :** couper l'alim pendant la programmation du
  shim (étape 3.3). Au rétablissement, la commande étant conservée, le shim doit
  **reprendre** et finir, ou (si la source est jugée invalide) partir en recovery
  UART — **jamais** booter une app à moitié écrite.
- **Image LoRa volontairement mauvaise :** pousser un `.bin` tronqué / mauvais
  MD5. Attendu : rejet à `Update.end` (MD5) ou au `commitDualOTACommand`, l'app
  courante reste intacte.
- **Mauvais fichier au flasheur série (foot-gun) :** tenter de flasher
  `firmware.bin` (app @0x10007000) via l'ESP32 → hardfault attendu → **récupérable**
  via recovery UART. Confirme le filet `BOOT_REASON_NOIMG` + le rôle du marqueur
  `FLASH_SAFETY.txt`.
- **Recovery UART :** provoquer un état invalide et confirmer que le pouls GPIO 22
  + le magic watchdog ramènent bien le nœud dans le bootloader série et que
  l'ESP32 peut reflasher la paire stage1+stage2.

## Critères GO / NO-GO déploiement

| Contrôle | GO si |
| --- | --- |
| H1 comportemental | `start ota` = `OK …` après étape 2 |
| H1 mémoire | `crc32(shim)` logiciel == mot crc du header à `0x10003000` |
| H2 | app étape 1 intacte et démarrable après étape 2 |
| OTA LoRa E2E | nœud revient sur la version LoRa distincte après OTA |
| Robustesse | coupure/mauvaise image → jamais de boot d'app corrompue ; recovery UART OK |

Ne déployer sur un nœud **sans accès physique** qu'après un **GO sur les cinq
lignes**, sur un banc de la même révision matérielle.
