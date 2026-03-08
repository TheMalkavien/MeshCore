# Version modifiee

Resume succinct des ecarts du fork `patch_public` par rapport a `upstream/main` (base `v1.14.0`).

## Reseau / repeater

- Retry conditionnel sur les floods relayes: si aucun autre repeater n'est entendu, le packet est re-emis automatiquement.
- Le mecanisme est maintenant applique a tous les floods relayes, avec stats dediees dans `stats-packets`: `relay.flood trk/ok/fail/retry/loss`.
- `set flood.advert.interval` accepte `1-168` heures (au lieu de `3-168`).
- Les reponses CLI OTA utilisent des delais plus courts pour accelerer les transferts distants.

## OTA RP2040

- Ajout d'un controleur OTA natif RP2040 avec commandes `start ota`, `ota begin`, `ota write`, `ota end`, `ota status`, `ota abort`.
- Support d'un transport OTA binaire dedie (requete custom `0x70`) avec fallback texte.
- Support OTA ajoute pour plusieurs cartes RP2040: Waveshare RP2040-LoRa, RAK11310, Raspberry Pi Pico W et Xiao RP2040.
- Sur certaines variantes Waveshare, `start ota` peut aussi reveiller un ESP32 flasher externe via un pin dedie.

## Outil web OTA

- Ajout du prototype `tools/web_ota_otg/` pour lancer une OTA distante depuis un navigateur.
- Connexion au companion via USB (`Web Serial` ou `WebUSB`) ou via BLE (`Web Bluetooth`).
- Le client web tente d'abord l'OTA binaire, puis bascule en OTA texte si necessaire.
- Support d'un preset radio OTA temporaire et d'un auto-reglage des parametres (`chunk`, `ack_every`, timeouts) selon le preset radio actif.

## Energie / USB (RP2040)

- Refactor low-power RP2040: reduction de conso sans vrai deep sleep, avec profils horloge/tension plus economes.
- En mode `powersaving`, l'USB peut etre coupe puis reactive a la demande.
- Boucle dispatcher optimisee avec `WFI` en idle pour reduire la consommation CPU.
- Le mode low-power tient compte des travaux en attente (dont les retries flood) avant de couper l'activite.

## ADC / batterie / radio

- Pin batterie Waveshare RP2040 LoRa corrige sur `PIN_VBAT_READ = 26` (GPIO26 / ADC0).
- `adc.multiplier` devient gerable dynamiquement cote board; les prefs ADC invalides sont nettoyees de facon moins aggressive.
- Correctif SX1262 TCXO en `1.8V` sur RP2040 LoRa, avec activation du regulateur DCDC.
- Ajout de hooks board pour mieux coordonner USB / OTA / mode low-power.

## Configuration par defaut

- Preset radio FR applique par defaut: `869.618 / BW 62.5 / SF8 / CR8`.
- Sur le repeater de ce fork, le mot de passe admin par defaut est force a `123456`.

## Commandes ajoutees

- `usb on`: reactive l'USB et restaure le profil actif si la board RP2040 etait en mode eco.
- `usb off`: coupe l'USB pour reduire la consommation.
- `usb status`: retourne l'etat USB courant (`on` / `off`).
- `ota begin/write/end/status/abort`: sequence OTA distante detaillee pour RP2040.

## Builds / environnements ajoutes

- `waveshare_rp2040_lora_repeater_lowpower`: repeater RP2040 LoRa avec profil basse consommation.
- `waveshare_rp2040_lora_repeater_ota_esp32`: variante RP2040 avec reveil d'un ESP32 flasher externe.
- `heltec_v4_repeater_wifi`: repeater Heltec V4 avec Wi-Fi.
- `Heltec_Wireless_Tracker_companion_radio_wifi`: companion radio Heltec Tracker avec Wi-Fi.
