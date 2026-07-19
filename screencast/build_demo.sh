#!/usr/bin/env bash
# Assembles the individual VHS segments into a single demo.mp4.
#
# The segments are recorded at different heights (each tape is sized to its own
# output, so nothing is cropped or scrolls off). ffmpeg's concat demuxer needs
# identical dimensions, so every segment is padded top-aligned onto a common
# 1500x760 canvas in the terminal's background colour first.
#
# Each segment is preceded by a generated title card so the video reads as a
# guided tour rather than seven disconnected terminal clips.
#
# Usage:  bash screencast/build_demo.sh
set -euo pipefail

cd "$(dirname "$0")/.."
OUT=screencast
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

W=1500
H=760
BG=0x1e1e2e          # Catppuccin Mocha base — matches the recordings
FG=0xcdd6f4          # Catppuccin Mocha text
ACCENT=0x89b4fa      # Catppuccin Mocha blue
FONT=/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf
CARD_SECS=2.5

# segment file | title | subtitle
segments=(
  "segment1-stack.mp4|1. The stack|five services, health-gated startup"
  "segment2-streaming.mp4|2. Streaming|Kafka partitions, consumer lag, live features"
  "segment3-predict.mp4|3. Scoring|cached history changes the verdict"
  "segment4-blue-green.mp4|4. Blue-green deploy|cutover under load, zero downtime"
  "segment5-performance.mp4|5. Performance|5000 requests against a 100 ms budget"
  "segment6-resilience.mp4|6. Graceful degradation|Redis dies, the API keeps answering"
  "segment7-container-tests.mp4|7. Hardening and tests|non-root, healthy, 7 passing"
)

card() {   # card <index> <title> <subtitle>
  local out="$WORK/card$1.mp4" title="$2" sub="$3"
  ffmpeg -v error -y -f lavfi -i "color=c=$BG:s=${W}x${H}:d=$CARD_SECS:r=12" \
    -vf "drawtext=fontfile=$FONT:text='$title':fontcolor=$ACCENT:fontsize=52:x=(w-text_w)/2:y=(h/2)-70,\
drawtext=fontfile=$FONT:text='$sub':fontcolor=$FG:fontsize=28:x=(w-text_w)/2:y=(h/2)+10" \
    -c:v libx264 -pix_fmt yuv420p -r 12 "$out"
  echo "file '$out'" >> "$WORK/list.txt"
}

pad() {    # pad <index> <segment file>
  local out="$WORK/seg$1.mp4"
  ffmpeg -v error -y -i "$OUT/$2" \
    -vf "pad=$W:$H:0:0:color=$BG" \
    -c:v libx264 -pix_fmt yuv420p -r 12 "$out"
  echo "file '$out'" >> "$WORK/list.txt"
}

i=0
for entry in "${segments[@]}"; do
  IFS='|' read -r file title sub <<< "$entry"
  i=$((i + 1))
  printf 'building %d/%d  %s\n' "$i" "${#segments[@]}" "$file"
  card "$i" "$title" "$sub"
  pad  "$i" "$file"
done

ffmpeg -v error -y -f concat -safe 0 -i "$WORK/list.txt" -c copy "$OUT/demo.mp4"

dur=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$OUT/demo.mp4")
size=$(du -h "$OUT/demo.mp4" | cut -f1)
printf '\ndemo.mp4  %.1fs  %s\n' "$dur" "$size"
