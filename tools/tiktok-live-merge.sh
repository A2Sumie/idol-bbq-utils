#!/usr/bin/env bash
# Merge a tiktok-live session dir into one continuous file.
# Audio-only (ao) parts get a rendered 22/7 logo video track matching the
# session's video geometry, so the merged timeline has no black holes.
#
# Usage (inside container or any host with ffmpeg/ffprobe):
#   tiktok-live-merge.sh <session-dir> [logo-image]
# Output: <session-dir>/merged.mkv (+ per-part normalized .merge.mp4 files removed on success)
set -Eeuo pipefail

DIR="${1:?usage: tiktok-live-merge.sh <session-dir> [logo-image]}"
LOGO="${2:-/app/assets/227-logo.jpg}"
MANIFEST="$DIR/manifest.json"
FFMPEG="${FFMPEG:-ffmpeg}"
FFPROBE="${FFPROBE:-ffprobe}"

[ -f "$MANIFEST" ] || { echo "no manifest: $MANIFEST" >&2; exit 2; }
[ -f "$LOGO" ] || { echo "logo not found: $LOGO" >&2; exit 2; }

has_video() { [ -n "$("$FFPROBE" -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 "$1" 2>/dev/null)" ]; }

# Target canvas = geometry of the first part that has video (fallback 640x1280).
W=640; H=1280
for p in "$DIR"/part*.mkv; do
  [ -e "$p" ] || continue
  if has_video "$p"; then
    IFS=x read -r W H <<<"$("$FFPROBE" -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x "$p")"
    break
  fi
done
echo "[merge] canvas ${W}x${H}"

LIST="$DIR/.merge-concat.txt"
: > "$LIST"
for p in "$DIR"/part*.mkv; do
  [ -e "$p" ] || continue
  base="${p%.mkv}"
  if has_video "$p"; then
    echo "[merge] $(basename "$p"): has video, normalize remux"
    "$FFMPEG" -hide_banner -loglevel error -y -i "$p" \
      -c:v libx264 -preset veryfast -crf 18 -pix_fmt yuv420p -r 25 \
      -vf "scale=${W}:${H}:force_original_aspect_ratio=decrease,pad=${W}:${H}:(ow-iw)/2:(oh-ih)/2,setsar=1" \
      -c:a aac -b:a 128k -ar 48000 -ac 2 "$base.merge.mp4"
  else
    echo "[merge] $(basename "$p"): audio-only, rendering logo video"
    "$FFMPEG" -hide_banner -loglevel error -y -loop 1 -i "$LOGO" -i "$p" \
      -filter_complex "[0:v]scale=${W}:${H}:force_original_aspect_ratio=increase,crop=${W}:${H},gblur=sigma=25[bg];[0:v]scale='min(iw,${W})*0.6':-1[fg];[bg][fg]overlay=(W-w)/2:(H-h)/2,setsar=1[v]" \
      -map "[v]" -map 1:a -c:v libx264 -preset veryfast -crf 18 -pix_fmt yuv420p -r 25 \
      -c:a aac -b:a 128k -ar 48000 -ac 2 -shortest "$base.merge.mp4"
  fi
  printf "file '%s'\n" "$(basename "$base.merge.mp4")" >> "$LIST"
done

OUT="$DIR/merged.mkv"
# Re-encode-free concat is fragile across mixed sources; concat demuxer + copy
# works here because every normalized part is h264/yuv420p/25fps + aac/48k/stereo.
(cd "$DIR" && "$FFMPEG" -hide_banner -loglevel error -y -f concat -safe 0 -i ".merge-concat.txt" -c copy "$OUT")

dur_out="$("$FFPROBE" -v error -show_entries format=duration -of csv=p=0 "$OUT")"
echo "[merge] wrote $OUT duration=${dur_out}s"
rm -f "$LIST" "$DIR"/part*.merge.mp4
