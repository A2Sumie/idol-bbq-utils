#!/bin/sh
set -eu

TOOLS_DIR="${TOOLS_DIR:-/app/tools}"
BIN_DIR="${TOOLS_DIR}/bin"
BILIUP_VENV="${TOOLS_DIR}/biliup-venv"
YT_DLP_URL="${YT_DLP_URL:-https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux}"

mkdir -p "$BIN_DIR"
export PATH="$BIN_DIR:$PATH"

echo "Updating deno to stable..."
if [ -x "$BIN_DIR/deno" ]; then
    DENO_INSTALL="$TOOLS_DIR" "$BIN_DIR/deno" upgrade stable --output "$BIN_DIR/deno"
else
    curl -fsSL https://deno.land/install.sh | DENO_INSTALL="$TOOLS_DIR" sh
fi
"$BIN_DIR/deno" --version | sed -n '1p'

echo "Updating yt-dlp to stable@latest..."
tmp_file="$(mktemp "$BIN_DIR/yt-dlp.XXXXXX")"
trap 'rm -f "$tmp_file"' EXIT
curl -fsSL "$YT_DLP_URL" -o "$tmp_file"
chmod +x "$tmp_file"
mv "$tmp_file" "$BIN_DIR/yt-dlp"
"$BIN_DIR/yt-dlp" --version

echo "Refreshing biliup toolchain..."
if [ ! -x "$BILIUP_VENV/bin/python" ]; then
    python3 -m venv "$BILIUP_VENV"
fi
"$BILIUP_VENV/bin/python" -m pip install --upgrade pip setuptools wheel
"$BILIUP_VENV/bin/python" -m pip install --upgrade biliup
ln -sf ../biliup-venv/bin/biliup "$BIN_DIR/biliup"
ln -sf ../biliup-venv/bin/python "$BIN_DIR/biliup-python"
"$BIN_DIR/biliup" --help >/dev/null
