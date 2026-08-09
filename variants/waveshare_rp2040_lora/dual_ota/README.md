# Migration distante vers le double OTA

Cette migration conserve **sans aucune modification** le firmware historique du
flasheur ESP32 et le bootloader série déjà installé sur le RP2040. Elle demande
de flasher successivement deux fichiers RP2040 :

1. `firmware-esp32-stage1.bin` installe le shim et l'application MeshCore ;
2. `firmware-esp32-stage2-seal.bin` réécrit le même préfixe de 12 Kio et fait
   sceller uniquement le shim par le bootloader série.

Le flasheur ESP32 historique scelle toujours la taille exacte du fichier qu'il
vient d'écrire. Le premier passage fournit donc une image complète et
immédiatement démarrable. Le second passage, indispensable, réduit ensuite la
zone couverte par le CRC du bootloader aux 12 Kio immuables. L'application peut
alors être remplacée par LoRa sans invalider ce CRC.

Il n'est pas nécessaire de construire, mettre à jour ou redémarrer l'ESP32 avec
un nouveau firmware pour cette procédure.

## Organisation de la flash RP2040

| Plage | Contenu |
| --- | --- |
| `0x10000000`–`0x10002fff` | bootloader série existant, jamais réécrit par la migration |
| `0x10003000`–`0x10003fff` | en-tête et CRC gérés par le bootloader série |
| `0x10004000`–`0x10006fff` | shim OTA LoRa immuable, 12 Kio |
| `0x10007000`–`0x1017efff` | application MeshCore remplaçable |
| `0x1017f000`–`0x101fefff` | LittleFS |
| `0x101ff000`–`0x101fffff` | EEPROM émulée |

Le fichier de commande LoRa propre à cette disposition s'appelle
`mcota2.cmd`. Il est volontairement incompatible avec l'ancien
`otacommand.bin`, afin qu'une commande résiduelle de l'ancienne disposition ne
puisse pas demander l'effacement du bootloader ou du shim.

## Construire la paire de migration

Construire les deux étapes dans une seule invocation PlatformIO :

```text
pio run -e waveshare_rp2040_lora_repeater_dual_ota_migration
```

Les quatre fichiers utiles se trouvent dans :

```text
.pio/build/waveshare_rp2040_lora_repeater_dual_ota_migration/
```

La commande produit :

```text
firmware-esp32-stage1.bin
firmware-esp32-stage1.json
firmware-esp32-stage2-seal.bin
firmware-esp32-stage2-seal.json
```

Les deux fichiers `.bin` forment une paire indissociable. Ils doivent provenir
de **la même compilation** et du même répertoire de build. Ne pas relancer la
compilation entre les deux flashages et ne pas associer un `stage1` conservé
d'une compilation avec un `stage2` plus récent.

Avant de partir sur le nœud distant, vérifier au minimum que :

- `firmware-esp32-stage2-seal.bin` mesure exactement `12288` octets
  (`0x3000`) ;
- le champ `migration_pair_crc32` est identique dans les deux fichiers JSON ;
- `firmware-esp32-stage1.json` nomme bien
  `firmware-esp32-stage2-seal.bin` dans `next_artifact` ;
- `firmware-esp32-stage2-seal.json` nomme bien
  `firmware-esp32-stage1.bin` dans `previous_artifact`.

Les JSON servent seulement au contrôle de la paire. Ils ne doivent jamais être
téléversés vers l'ESP32.

## Préparation de la migration distante

Préparer les deux `.bin` avant de réveiller l'ESP32. Sauvegarder si possible la
configuration du nœud et utiliser une alimentation stable. Conserver la page
du flasheur ouverte et éviter toute attente inutile entre les deux passages :
l'idéal est de les terminer dans la même session Wi-Fi, avant la mise en veille
de l'ESP32.

Depuis la CLI MeshCore du nœud encore fonctionnel, réveiller le flasheur :

```text
start esp32ota
```

La réponse attendue est :

```text
Waking UP ESP32Flasher to flash the firmware.
```

Se connecter ensuite à l'interface Wi-Fi habituelle du flasheur ESP32. La
procédure ci-dessous utilise exactement les boutons et commandes du firmware
historique.

## Étape 1 : installer le shim et l'application

1. Sélectionner exclusivement `firmware-esp32-stage1.bin` et lancer son
   téléversement vers l'ESP32.
2. Attendre le message `Téléversement terminé avec succès.`
3. Cliquer sur le bouton de préparation du RP2040. L'interface envoie alors
   `CMD:PREPARE_FLASH`, force le RP2040 dans son bootloader série et lance la
   synchronisation.
4. Attendre `Le RP2040 est synchronisé et prêt.` ou l'événement
   `RP2040_SYNCED`.
5. Cliquer sur le bouton de lancement du flashage. L'interface envoie
   `CMD:START_FLASH`.
6. Ne rien déconnecter jusqu'aux messages `Scellement réussi.` puis
   `Flashage terminé ! L'appareil va redémarrer.`

À ce point, le nouveau shim et la nouvelle application sont présents et le
nœud peut redémarrer. Cependant, le header série scelle encore **toute**
l'image de l'étape 1. La migration n'est donc pas terminée.

> **Verrou impératif : aucune OTA LoRa ne doit être lancée entre l'étape 1 et
> l'étape 2.** Elle modifierait l'application encore couverte par le CRC du
> bootloader série ; au redémarrage suivant, ce dernier la refuserait.

## Étape 2 : limiter le scellement au shim

Le flashage de l'étape 1 termine par un `GO` et quitte le bootloader. Il faut
donc effectuer un **nouveau cycle complet**, même si l'interface ESP32 est
restée ouverte :

1. Rester connecté à la même session Wi-Fi et agir avant la mise en veille de
   l'ESP32. Si la page s'est rechargée après la fin de l'étape 1, attendre
   qu'elle soit de nouveau opérationnelle.
2. Si l'ESP32 n'est plus joignable mais que le nœud RP2040 est déjà revenu sur
   le mesh, envoyer de nouveau `start esp32ota`, puis rouvrir son interface.
3. Sélectionner exclusivement `firmware-esp32-stage2-seal.bin` provenant de la
   même compilation que le `stage1`, puis le téléverser vers l'ESP32.
4. Attendre de nouveau `Téléversement terminé avec succès.`
5. Cliquer **de nouveau** sur la préparation du RP2040 afin d'envoyer
   `CMD:PREPARE_FLASH`. Ne pas considérer le mode bootloader de l'étape 1 comme
   encore actif.
6. Attendre de nouveau `RP2040_SYNCED`.
7. Cliquer **de nouveau** sur le lancement du flashage afin d'envoyer
   `CMD:START_FLASH`.
8. Ne rien déconnecter jusqu'aux nouveaux messages `Scellement réussi.` puis
   `Flashage terminé ! L'appareil va redémarrer.`

Ce second fichier mesure exactement `0x3000` octets. Le flasheur historique
n'efface et ne réécrit ainsi que `0x10004000`–`0x10006fff` ; l'application de
l'étape 1, qui commence à `0x10007000`, est préservée. Son appel `SEAL` final
enregistre une taille de `0x3000` et le CRC de ce préfixe immuable.

Attendre ensuite le retour du nœud sur le mesh et contrôler son identité, sa
configuration et sa version. Une build double OTA porte le marqueur
`MLK_DUAL_OTA` dans `FIRMWARE_BUILD_DATE`. La première OTA LoRa ne doit être
tentée qu'après la réussite confirmée de cette étape 2.

## Fichiers interdits dans le flasheur ESP32

Le flasheur historique ne reconnaît pas le format d'un binaire et ne protège
pas contre une sélection erronée. Pour la migration ou une récupération série,
ne jamais lui envoyer :

- le `firmware.bin` générique du répertoire PlatformIO ;
- `firmware-lora.bin` ;
- un ancien `firmware-esp32-installer.bin` issu de la solution en un passage ;
- un fichier `.json`, `.uf2`, `.gz` ou tout fichier renommé manuellement ;
- `firmware-esp32-stage2-seal.bin` avant d'avoir terminé l'étape 1 ;
- un `stage1` et un `stage2` provenant de compilations différentes.

Les seuls fichiers acceptables, dans cet ordre, sont :

```text
firmware-esp32-stage1.bin
firmware-esp32-stage2-seal.bin
```

## Fenêtres critiques et reprise après incident

Cette première migration n'est pas atomique et il n'existe pas de partition
applicative A/B.

- Une coupure pendant l'étape 1 peut laisser l'application incomplète. Le
  bootloader série reste présent, mais la récupération distante dépend alors
  de l'ESP32, de son alimentation et de sa capacité à rester joignable et à
  piloter les broches de boot et de reset.
- Après un scellement réussi de l'étape 1, le nœud est normalement démarrable.
  Si l'étape 2 n'a pas encore commencé, elle peut être reprise ultérieurement,
  mais toute OTA LoRa reste interdite jusque-là.
- Une coupure pendant l'étape 2 peut endommager le shim ou son header. C'est la
  fenêtre la plus délicate : si le RP2040 ne peut plus démarrer, il ne pourra
  plus exécuter `start esp32ota` pour réveiller l'ESP32.

Si une erreur survient avant `Scellement réussi.`, ne pas laisser volontairement
l'ESP32 se mettre en veille et ne pas fermer une session encore utilisable :

- si l'étape fautive est connue avec certitude, remettre le RP2040 en
  bootloader avec `CMD:PREPARE_FLASH`, attendre `RP2040_SYNCED`, puis relancer
  le même fichier avec `CMD:START_FLASH` ;
- si l'état est incertain mais que l'ESP32 contrôle encore le bootloader,
  recommencer la paire complète, d'abord `stage1`, puis `stage2`, depuis la
  même compilation ;
- si l'ESP32 n'est plus accessible et que le RP2040 ne démarre plus, une
  intervention physique UART, USB ou SWD peut devenir nécessaire.

Après une migration réussie, une récupération complète par l'ESP32 historique
utilise elle aussi une nouvelle paire `stage1` puis `stage2`. Le binaire LoRa
seul n'est pas une image de récupération série.

## Mises à jour LoRa après la migration

Une fois l'étape 2 confirmée, construire une mise à jour LoRa avec :

```text
pio run -e waveshare_rp2040_lora_repeater_dual_ota_lora
```

Le fichier à fournir au mécanisme OTA LoRa est uniquement :

```text
.pio/build/waveshare_rp2040_lora_repeater_dual_ota_lora/firmware-lora.bin
```

Cette cible produit l'application brute liée à `0x10007000`. L'outil OTA actuel
annonce `gz=0` et utilise donc le `.bin` non compressé, avec son contrôle MD5 de
bout en bout.

Le shim conserve la commande d'une mise à jour LoRa jusqu'à ce que
l'application écrite soit relue et validée. Une coupure pendant cette écriture
provoque donc normalement une nouvelle tentative au redémarrage. Le shim ne
réécrit ni le bootloader série ni sa propre zone.

Conserver un `firmware-lora.bin` connu comme fonctionnel pour un retour arrière
par LoRa. En l'absence de partition A/B, ce fichier conservé constitue le moyen
de revenir à une application validée.
