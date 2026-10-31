#!/usr/bin/env bash
#
# Stamps the web tool with the current date and time, shown in its header so
# that anyone can tell whether the page they have loaded is the latest. Run it
# before committing any change to index.html or app.js.
#
# It updates the three places that must agree: OTA_TOOL_BUILD in app.js, the
# 'ota-tool-build' meta in index.html (a mismatch means a cached app.js), and
# the '?v=' that makes browsers fetch the new app.js.
set -euo pipefail
cd "$(dirname "$0")"

stamp=$(date "+%Y-%m-%d %H:%M")
cache=$(date "+%Y%m%d-%H%M")

sed -i -E "s/^const OTA_TOOL_BUILD = \"[^\"]*\";/const OTA_TOOL_BUILD = \"$stamp\";/" app.js
sed -i -E "s/(<meta name=\"ota-tool-build\" content=\")[^\"]*(\">)/\1$stamp\2/" index.html
sed -i -E "s/(src=\"\.\/app\.js\?v=)[^\"]*(\")/\1$cache\2/" index.html

grep -q "^const OTA_TOOL_BUILD = \"$stamp\";" app.js
grep -q "content=\"$stamp\"" index.html
grep -q "app.js?v=$cache\"" index.html
echo "web_ota_otg stamped $stamp"
