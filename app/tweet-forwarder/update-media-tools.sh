#!/bin/sh
set -eu

TOOLS_DIR="${TOOLS_DIR:-/app/tools}"
BIN_DIR="${TOOLS_DIR}/bin"
BILIUP_VENV="${TOOLS_DIR}/biliup-venv"
GALLERY_DL_VENV="${TOOLS_DIR}/gallery-dl-venv"
YT_DLP_URL="${YT_DLP_URL:-https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux}"
BILIUP_PACKAGE="${BILIUP_PACKAGE:-biliup}"
TOOL_PYTHON_VERSION="${TOOL_PYTHON_VERSION:-3.12}"
UV_INSTALL_URL="${UV_INSTALL_URL:-https://astral.sh/uv/install.sh}"
UV_PYTHON_INSTALL_DIR="${UV_PYTHON_INSTALL_DIR:-${TOOLS_DIR}/uv-python}"

mkdir -p "$BIN_DIR"
export PATH="$BIN_DIR:$PATH"
export UV_PYTHON_INSTALL_DIR

ensure_uv() {
    if [ ! -x "$BIN_DIR/uv" ]; then
        curl -LsSf "$UV_INSTALL_URL" | UV_INSTALL_DIR="$BIN_DIR" sh
    fi
    "$BIN_DIR/uv" --version
}

ensure_tool_venv() {
    venv_dir="$1"
    ensure_uv
    recreate_venv=0
    if [ ! -x "$venv_dir/bin/python" ]; then
        recreate_venv=1
    elif ! "$venv_dir/bin/python" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)' >/dev/null 2>&1; then
        recreate_venv=1
    fi

    if [ "$recreate_venv" = "1" ]; then
        rm -rf "$venv_dir"
        "$BIN_DIR/uv" python install "$TOOL_PYTHON_VERSION"
        "$BIN_DIR/uv" venv --python "$TOOL_PYTHON_VERSION" "$venv_dir"
    fi
    "$venv_dir/bin/python" --version
}

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
YT_DLP_VERSION="$("$BIN_DIR/yt-dlp" --version)"
printf '%s\n' "$YT_DLP_VERSION"

echo "Refreshing gallery-dl toolchain..."
ensure_tool_venv "$GALLERY_DL_VENV"
"$BIN_DIR/uv" pip install --python "$GALLERY_DL_VENV/bin/python" --upgrade gallery-dl
ln -sf ../gallery-dl-venv/bin/gallery-dl "$BIN_DIR/gallery-dl"
"$BIN_DIR/gallery-dl" --version

echo "Refreshing biliup toolchain..."
ensure_tool_venv "$BILIUP_VENV"
"$BIN_DIR/uv" pip install --python "$BILIUP_VENV/bin/python" --upgrade "$BILIUP_PACKAGE"
YT_DLP_PY_PACKAGE="${YT_DLP_PY_PACKAGE:-https://github.com/yt-dlp/yt-dlp/archive/refs/tags/${YT_DLP_VERSION}.tar.gz}"
"$BIN_DIR/uv" pip install --python "$BILIUP_VENV/bin/python" --upgrade "$YT_DLP_PY_PACKAGE"
ln -sf ../biliup-venv/bin/biliup "$BIN_DIR/biliup"
printf '%s\n' '#!/bin/sh' "exec \"$BILIUP_VENV/bin/python\" \"\$@\"" > "$BIN_DIR/biliup-python"
chmod +x "$BIN_DIR/biliup-python"
"$BILIUP_VENV/bin/python" - <<'PY'
from biliup.plugins.bili_webup import BiliBili, BiliWeb, Data

assert BiliBili and BiliWeb and Data
PY
"$BIN_DIR/biliup" --help >/dev/null
"$BILIUP_VENV/bin/python" -m yt_dlp --version
