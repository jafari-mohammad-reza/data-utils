#!/bin/bash

set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 <platform> <processor> <architect> <name> <path> [options]

Arguments:
  platform   : linux, darwin, windows
  processor  : amd, arm
  architect  : 32, 64
  name       : output binary name
  path       : path to main package (relative to cmd/)

Options:
  --vendor   : Use vendored dependencies
  --upx      : Compress binary with UPX
  --debug    : Include debug symbols
  --tags     : Build tags (comma-separated)

Examples:
  $0 linux amd 64 myapp server
  $0 darwin arm 64 myapp cli --vendor --upx
EOF
    exit 1
}

[[ $# -lt 5 ]] && usage

PLATFORM="$1"
PROCESSOR="$2"
ARCHITECT="$3"
NAME="$4"
BUILD_PATH="$5"
shift 5

USE_VENDOR=false
USE_UPX=false
DEBUG_MODE=false
BUILD_TAGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --vendor) USE_VENDOR=true ;;
        --upx) USE_UPX=true ;;
        --debug) DEBUG_MODE=true ;;
        --tags) BUILD_TAGS="$2"; shift ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
    shift
done

case "$PLATFORM" in
    linux|darwin|windows) ;;
    *) echo "Invalid platform: $PLATFORM"; exit 1 ;;
esac

case "$PROCESSOR-$ARCHITECT" in
    amd-64) GOARCH="amd64" ;;
    amd-32) GOARCH="386" ;;
    arm-64) GOARCH="arm64" ;;
    arm-32) GOARCH="arm" ;;
    *) echo "Invalid processor/architect: $PROCESSOR/$ARCHITECT"; exit 1 ;;
esac

if [[ "$USE_VENDOR" == "true" ]]; then
    echo "Vendoring dependencies..."
    go mod vendor
    MOD_FLAG="-mod=vendor"
else
    MOD_FLAG="-mod=readonly"
fi

LDFLAGS="-extldflags '-static'"
if [[ "$DEBUG_MODE" == "false" ]]; then
    LDFLAGS="-s -w $LDFLAGS"
fi

BUILD_TIME=$(date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS="$LDFLAGS -X main.buildTime=$BUILD_TIME -X main.gitCommit=$GIT_COMMIT"

BUILD_ARGS=(
    -a
    -ldflags="$LDFLAGS"
    -trimpath
    -installsuffix cgo
    -o "$NAME"
)

[[ -n "$BUILD_TAGS" ]] && BUILD_ARGS+=(-tags "$BUILD_TAGS")

echo "Building $NAME for $PLATFORM/$GOARCH..."
CGO_ENABLED=0 GOOS="$PLATFORM" GOARCH="$GOARCH" \
    go build $MOD_FLAG "${BUILD_ARGS[@]}" "./cmd/$BUILD_PATH"

if [[ "$USE_VENDOR" == "true" ]]; then
    rm -rf vendor/
fi

if [[ "$USE_UPX" == "true" ]] && command -v upx &>/dev/null; then
    echo "Compressing with UPX..."
    upx --best --lzma "$NAME" 2>/dev/null || upx -9 "$NAME"
fi

SIZE=$(du -h "$NAME" | cut -f1)
echo "Built: $NAME ($SIZE)"

if [[ "$PLATFORM" == "$(go env GOOS)" ]] && [[ "$GOARCH" == "$(go env GOARCH)" ]]; then
    file "$NAME"
    [[ "$PLATFORM" == "linux" ]] && ldd "$NAME" 2>&1 | grep -q "not a dynamic executable" && echo "✓ Static binary"
fi
