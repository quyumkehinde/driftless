#!/bin/sh
# Installs the latest driftless release binary.
#
#   curl -fsSL https://raw.githubusercontent.com/quyumkehinde/driftless/main/scripts/install.sh | sh
#
# Environment:
#   DRIFTLESS_VERSION   release tag to install (default: latest), e.g. v1.0.1
#   DRIFTLESS_INSTALL   install directory (default: /usr/local/bin)
set -eu

REPO="quyumkehinde/driftless"
INSTALL_DIR="${DRIFTLESS_INSTALL:-/usr/local/bin}"
VERSION="${DRIFTLESS_VERSION:-}"

err() { printf 'install: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || err "missing required tool: $1"; }

need curl
need tar

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$os" in
  linux|darwin) ;;
  *) err "unsupported OS: $os (prebuilt binaries exist for linux and darwin)" ;;
esac

arch=$(uname -m)
case "$arch" in
  x86_64|amd64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *) err "unsupported architecture: $arch" ;;
esac

if [ -z "$VERSION" ]; then
  VERSION=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
    | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -n1)
  [ -n "$VERSION" ] || err "could not determine latest release"
fi
ver="${VERSION#v}"

archive="driftless_${ver}_${os}_${arch}.tar.gz"
base="https://github.com/$REPO/releases/download/$VERSION"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

printf 'Downloading driftless %s (%s/%s)...\n' "$VERSION" "$os" "$arch"
curl -fsSL -o "$tmp/$archive" "$base/$archive"
curl -fsSL -o "$tmp/checksums.txt" "$base/checksums.txt"

expected=$(grep " $archive\$" "$tmp/checksums.txt" | cut -d' ' -f1)
[ -n "$expected" ] || err "$archive not found in checksums.txt"
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp/$archive" | cut -d' ' -f1)
else
  actual=$(shasum -a 256 "$tmp/$archive" | cut -d' ' -f1)
fi
[ "$expected" = "$actual" ] || err "checksum mismatch for $archive"

tar -xzf "$tmp/$archive" -C "$tmp" driftless

# macOS tags browser downloads with the quarantine attribute, which makes
# Gatekeeper refuse unsigned binaries. curl does not set it, but clear it
# anyway in case the archive was fetched another way.
if [ "$os" = darwin ] && command -v xattr >/dev/null 2>&1; then
  xattr -d com.apple.quarantine "$tmp/driftless" 2>/dev/null || true
fi

if [ -w "$INSTALL_DIR" ]; then
  install -m 0755 "$tmp/driftless" "$INSTALL_DIR/driftless"
else
  printf 'Installing to %s requires sudo.\n' "$INSTALL_DIR"
  sudo install -m 0755 "$tmp/driftless" "$INSTALL_DIR/driftless"
fi

printf 'Installed %s/driftless\n' "$INSTALL_DIR"
"$INSTALL_DIR/driftless" version || true
