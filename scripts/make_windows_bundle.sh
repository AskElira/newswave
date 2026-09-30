#!/bin/sh
# Build ~/Desktop/newswave-windows.zip from the COMMITTED windows-port HEAD (git archive), so .env, .venv,
# data/ and .armed can never leak. Usage: scripts/make_windows_bundle.sh [output.zip]
set -eu
REPO="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-$HOME/Desktop/newswave-windows.zip}"
REF="${BUNDLE_REF:-windows-port}"
git -C "$REPO" archive --format=zip --prefix=newswave/ -o "$OUT" "$REF"
# the bundle must never carry secrets or runtime state
if unzip -Z1 "$OUT" | grep -E '(^|/)(\.env$|\.env\.[^e]|\.armed$|[^/]*\.db(-[^/]*)?$|\.venv/|data/)'; then
  rm -f "$OUT"; echo "REFUSED: secret or runtime file in bundle" >&2; exit 1
fi
echo "built $OUT ($(du -h "$OUT" | cut -f1)) from $REF $(git -C "$REPO" rev-parse --short "$REF")"
