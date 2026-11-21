[CmdletBinding()]
param(
    [string]$AndroidSdk = $env:ANDROID_SDK_ROOT
)

$ErrorActionPreference = "Stop"
$project = $PSScriptRoot
$repo = (Resolve-Path (Join-Path $project "..\..")).Path
$webSource = Join-Path $repo "tools\web_ota_otg"
$build = Join-Path $project "build"

if (-not $AndroidSdk) {
    $AndroidSdk = $env:ANDROID_HOME
}
if (-not $AndroidSdk) {
    $AndroidSdk = Join-Path $env:LOCALAPPDATA "Android\Sdk"
}
if (-not (Test-Path $AndroidSdk)) {
    throw "Android SDK introuvable. Définis ANDROID_SDK_ROOT ou passe -AndroidSdk."
}

$buildToolsDir = Get-ChildItem (Join-Path $AndroidSdk "build-tools") -Directory |
    Sort-Object { [version]$_.Name } -Descending |
    Select-Object -First 1
$platformDir = Get-ChildItem (Join-Path $AndroidSdk "platforms") -Directory |
    Where-Object { Test-Path (Join-Path $_.FullName "android.jar") } |
    Sort-Object Name -Descending |
    Select-Object -First 1
if (-not $buildToolsDir -or -not $platformDir) {
    throw "Android SDK incomplet : platform et build-tools sont requis."
}

$aapt2 = Join-Path $buildToolsDir.FullName "aapt2.exe"
$aapt = Join-Path $buildToolsDir.FullName "aapt.exe"
$d8 = Join-Path $buildToolsDir.FullName "d8.bat"
$zipalign = Join-Path $buildToolsDir.FullName "zipalign.exe"
$apksigner = Join-Path $buildToolsDir.FullName "apksigner.bat"
$androidJar = Join-Path $platformDir.FullName "android.jar"
$javac = (Get-Command javac -ErrorAction Stop).Source
$keytool = (Get-ChildItem (Join-Path $env:ProgramFiles "Java") -Recurse -Filter keytool.exe |
    Sort-Object FullName -Descending |
    Select-Object -First 1).FullName
if (-not $keytool) {
    throw "keytool introuvable dans le JDK Java."
}

Remove-Item $build -Recurse -Force -ErrorAction SilentlyContinue
$null = New-Item $build -ItemType Directory
$classes = New-Item (Join-Path $build "classes") -ItemType Directory
$dex = New-Item (Join-Path $build "dex") -ItemType Directory
$assets = New-Item (Join-Path $build "assets") -ItemType Directory
Copy-Item (Join-Path $webSource "index.html") $assets.FullName
Copy-Item (Join-Path $webSource "app.js") $assets.FullName

$compiledResources = Join-Path $build "resources.zip"
& $aapt2 compile --dir (Join-Path $project "res") -o $compiledResources
if ($LASTEXITCODE -ne 0) { throw "Échec aapt2 compile." }

$unsignedApk = Join-Path $build "unsigned.apk"
& $aapt2 link `
    -o $unsignedApk `
    -I $androidJar `
    --manifest (Join-Path $project "AndroidManifest.xml") `
    --min-sdk-version 26 `
    --target-sdk-version 35 `
    --version-code 1 `
    --version-name "1.0.0" `
    -A $assets.FullName `
    $compiledResources
if ($LASTEXITCODE -ne 0) { throw "Échec aapt2 link." }

$javaSources = Get-ChildItem (Join-Path $project "src") -Recurse -Filter *.java |
    ForEach-Object FullName
$javacArgs = @(
    "--release", "11",
    "-encoding", "UTF-8",
    "-classpath", $androidJar,
    "-d", $classes.FullName
) + @($javaSources)
& $javac $javacArgs
if ($LASTEXITCODE -ne 0) { throw "Échec javac." }

$classFiles = Get-ChildItem $classes.FullName -Recurse -Filter *.class |
    ForEach-Object FullName
$d8Args = @(
    "--release",
    "--min-api", "26",
    "--lib", $androidJar,
    "--output", $dex.FullName
) + @($classFiles)
& $d8 $d8Args
if ($LASTEXITCODE -ne 0) { throw "Échec d8." }

Push-Location $dex.FullName
try {
    & $aapt add $unsignedApk "classes.dex"
    if ($LASTEXITCODE -ne 0) { throw "Échec ajout classes.dex." }
} finally {
    Pop-Location
}

$alignedApk = Join-Path $build "aligned.apk"
& $zipalign -f 4 $unsignedApk $alignedApk
if ($LASTEXITCODE -ne 0) { throw "Échec zipalign." }

$signing = Join-Path $project "signing"
$keystore = Join-Path $signing "meshcore-ota-debug.keystore"
if (-not (Test-Path $keystore)) {
    $null = New-Item $signing -ItemType Directory -Force
    & $keytool -genkeypair -noprompt `
        -keystore $keystore `
        -storepass android `
        -alias androiddebugkey `
        -keypass android `
        -dname "CN=MeshCore OTA Android,O=MeshCore,C=FR" `
        -keyalg RSA `
        -keysize 2048 `
        -validity 10000
    if ($LASTEXITCODE -ne 0) { throw "Échec création clé de signature." }
}

$apk = Join-Path $build "meshcore-ota-android.apk"
& $apksigner sign `
    --ks $keystore `
    --ks-pass pass:android `
    --key-pass pass:android `
    --out $apk `
    $alignedApk
if ($LASTEXITCODE -ne 0) { throw "Échec signature APK." }

& $apksigner verify --verbose --print-certs $apk
if ($LASTEXITCODE -ne 0) { throw "La vérification de signature a échoué." }

Write-Host ""
Write-Host "APK créé : $apk"
