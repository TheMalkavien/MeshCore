# MeshCore OTA Android

Application Android autonome équivalente à `tools/web_ota_otg/main.go` :

- sert localement la même interface Web OTA dans une `WebView` ;
- expose le même endpoint WebSocket `/tcp` ;
- relaie sans modification les octets WebSocket vers la socket TCP du companion MeshCore ;
- ouvre le sélecteur de fichiers Android pour les firmwares `.bin`, `.bin.gz` et `.uf2`.
- garde l'écran allumé pendant que l'application est ouverte afin de ne pas interrompre une OTA.

Le téléphone et le companion doivent être sur le même réseau Wi-Fi. Dans l'application,
indiquer l'adresse du companion, généralement `192.168.4.1:5000`, puis toucher
**Connecter TCP**.

## Compiler l'APK

Le script n'utilise ni Gradle ni dépendance externe. Il s'appuie seulement sur un JDK
et sur les Android SDK Build Tools déjà installés :

```powershell
cd tools\web_ota_android
.\build.ps1
```

L'APK signé pour installation locale est produit dans
`build\meshcore-ota-android.apk`. La clé de développement est générée une fois dans
`signing\` afin que les APK suivantes puissent mettre l'application à jour. Cette clé
n'est pas versionnée : conserve-la si tu distribues plusieurs versions.

## Portée

Cette APK reproduit la fonction du programme Go : le mode **TCP (companion WiFi)**.
Elle choisit donc TCP au démarrage. Les API Web Serial, WebUSB et Web Bluetooth de la
page d'origine ne sont pas fournies par la `WebView` Android ; les modes USB/BLE
restent visibles pour conserver l'interface commune, mais ne sont pas pris en charge
par cette première APK.
