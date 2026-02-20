# Version modifiee

Resume tres succinct des ecarts par rapport au firmware de base (`main` / `v1.13.0`).

## Reseau / repeater

- Ajout d'un retry conditionnel sur les floods de groupe: si aucun autre repeater n'est entendu, le packet est re-emis automatiquement (jusqu'a la limite configuree).
- Ajout d'indicateurs de retry dans les stats packets: `relay.grp trk/ok/fail/retry/loss`.
- Ajustement de la commande existante `set flood.advert.interval`: plage valide `1-168` heures (au lieu de `3-168`).

## Energie / USB (RP2040)

- Refactor low-power: mode economie centre sur la reduction de conso sans vrai deep sleep RP2040.
- En mode powersaving, l'USB est coupe apres inactivite puis peut etre reactive a la demande.
- Boucle dispatcher optimisee avec `WFI` en idle (baisse de conso CPU).

## ADC / batterie

- Pin batterie change sur RP2040 LoRa: `PIN_VBAT_READ = 26` (GPIO26 / ADC0).
- Multiplicateur ADC gerable dynamiquement cote board (au lieu d'une valeur strictement fixe).
- Validation des prefs ADC assouplie: conservation des valeurs elevees utiles, reset uniquement si valeur invalide/corrompue.

## Radio / materiel

- Correctif SX1262 TCXO en `1.8V` sur RP2040 LoRa (+ regulateur DCDC active).
- Ajout de hooks IRQ radio pour mieux coordonner la reception avec le mode low-power.
- Support OTA via reveil d'un ESP32 flasher (quand le pin dedie est defini).

## Configuration par defaut

- Preset radio FR applique par defaut (`869.618 / BW 62.5 / SF8 / CR8`).
- Mot de passe admin repeater par defaut fixe a `123456`.

## Commandes ajoutees

- `usb on`: reactive l'USB (reattach au host, et retour profil actif si low-power RP2040).
- `usb off`: coupe l'USB pour reduire la consommation.
- `usb status`: retourne l'etat USB courant (`on` / `off`).
