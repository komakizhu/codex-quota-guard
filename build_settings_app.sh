#!/bin/bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
BUNDLE="$APP_DIR/CodexQuotaGuardSettings.app"
ICONSET="$APP_DIR/CodexQuotaGuard.iconset"
mkdir -p "$BUNDLE/Contents/MacOS" "$BUNDLE/Contents/Resources"
xcrun swiftc \
  -parse-as-library \
  -sdk "$(xcrun --sdk macosx --show-sdk-path)" \
  -target arm64-apple-macosx15.0 \
  -framework SwiftUI \
  -framework AppKit \
  "$APP_DIR/CodexQuotaGuardSettings.swift" \
  -o "$BUNDLE/Contents/MacOS/CodexQuotaGuardSettings"
cp "$APP_DIR/CodexQuotaGuardSettings-Info.plist" "$BUNDLE/Contents/Info.plist"
rm -rf "$ICONSET"
mkdir -p "$ICONSET"
ICON_BUILDER="$APP_DIR/.make_app_icon"
xcrun swiftc "$APP_DIR/make_app_icon.swift" -o "$ICON_BUILDER"
for spec in \
  "16 16x16" "32 16x16@2x" \
  "32 32x32" "64 32x32@2x" \
  "128 128x128" "256 128x128@2x" \
  "256 256x256" "512 256x256@2x" \
  "512 512x512" "1024 512x512@2x"; do
  set -- $spec
  "$ICON_BUILDER" "$ICONSET/icon_$2.png" "$1"
done
iconutil -c icns -o "$BUNDLE/Contents/Resources/AppIcon.icns" "$ICONSET"
rm -f "$ICON_BUILDER"
codesign --force --deep --sign - "$BUNDLE" >/dev/null
echo "Built $BUNDLE"
