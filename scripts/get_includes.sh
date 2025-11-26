#!/usr/bin/env bash
set -euo pipefail

# Fetch include targets referenced by include=yes rows and emit an augmented
# metadata file (same columns as find_ledgers.sh output).
#
# Input format (tab-separated):
#   raw_url  size  include_flag  include_targets_csv
# Output format (tab-separated; same as input):
#   raw_url  size  include_flag  include_targets_csv

usage() {
  cat <<'EOF'
Usage: get_includes.sh <input_meta> [output_meta]

Requires: curl, jq, python3
Token: uses GITHUB_TOKEN/GH_TOKEN or scripts/.github_token (recommended).

Logic:
  - Read metadata (find_ledgers.sh output).
  - Collect include targets from include=yes rows.
  - If the target is not already present in the input, fetch it via the GitHub
    Contents API (same repo/commit as the including file) and append it.
  - Deduplicate on basename+size; merge include targets on duplicate keys.
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

INPUT="$1"
OUTPUT="${2:-}"

[[ -f "$INPUT" ]] || { echo "Input not found: $INPUT" >&2; exit 1; }
[[ "$INPUT" == "-" ]] && { echo "Input file required (no stdin) for two-pass processing." >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }
}

require_cmd curl
require_cmd jq
require_cmd python3

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOKEN_FILE="$SCRIPT_DIR/.github_token"
TOKEN="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
if [[ -z "$TOKEN" && -f "$TOKEN_FILE" ]]; then
  TOKEN="$(tr -d '\r\n' < "$TOKEN_FILE")"
fi

auth_header=()
if [[ -n "$TOKEN" ]]; then
  auth_header=( -H "Authorization: Bearer $TOKEN" )
else
  echo "Note: proceeding without a token; GitHub API rate limits will be very low." >&2
fi

url_encode_path() {
  python3 - <<'PY' "$1"
import sys, urllib.parse
print(urllib.parse.quote(sys.argv[1]))
PY
}

tmpdir="$(mktemp -d)"
cleanup() { rm -rf "$tmpdir"; }
trap cleanup EXIT INT TERM

resolve_path() {
  local base_dir="$1" target="$2"
  if [[ "$target" == /* ]]; then
    echo "${target#/}"
  elif [[ -z "$base_dir" || "$base_dir" == "." ]]; then
    echo "$target"
  else
    echo "$base_dir/$target"
  fi
}

declare -A rec_url rec_size rec_targets rec_owner rec_repo rec_sha rec_path rec_dir rec_incflag
declare -A path_to_id
declare -A key_to_id
records=0
input_rows=0
include_yes_rows=0
targets_declared=0
targets_unique=0
targets_present=0
targets_fetched=0
targets_failed=0
dedup_dropped=0

add_or_merge_record() {
  local url="$1" size="$2" include_flag="$3" targets="$4" owner="$5" repo="$6" sha="$7" path="$8" dir="$9"
  local base key id
  base="${path##*/}"
  key="${base}|${size}"
  if [[ -n "${key_to_id[$key]:-}" ]]; then
    id="${key_to_id[$key]}"
    dedup_dropped=$((dedup_dropped + 1))
    path_to_id["$owner|$repo|$sha|$path"]="$id"
    if [[ -n "$targets" ]]; then
      if [[ -n "${rec_targets[$id]:-}" ]]; then
        rec_targets["$id"]="${rec_targets[$id]},${targets}"
      else
        rec_targets["$id"]="$targets"
      fi
    fi
    rec_incflag["$id"]="$include_flag"
    rec_owner["$id"]="$owner"
    rec_repo["$id"]="$repo"
    rec_sha["$id"]="$sha"
    rec_path["$id"]="$path"
    rec_dir["$id"]="$dir"
    rec_url["$id"]="$url"
    rec_size["$id"]="$size"
  else
    id="$records"
    records=$((records + 1))
    key_to_id[$key]="$id"
    path_to_id["$owner|$repo|$sha|$path"]="$id"
    rec_url["$id"]="$url"
    rec_size["$id"]="$size"
    rec_incflag["$id"]="$include_flag"
    rec_targets["$id"]="$targets"
    rec_owner["$id"]="$owner"
    rec_repo["$id"]="$repo"
    rec_sha["$id"]="$sha"
    rec_path["$id"]="$path"
    rec_dir["$id"]="$dir"
  fi
}

fetch_include_target() {
  local owner="$1" repo="$2" sha="$3" path="$4"
  local encoded_path api body http_code size raw_url
  encoded_path="$(url_encode_path "$path")"
  api="https://api.github.com/repos/${owner}/${repo}/contents/${encoded_path}?ref=${sha}"
  body="$tmpdir/body.$$"
  http_code="$(curl -sS -w '%{http_code}' -o "$body" -H "Accept: application/vnd.github+json" "${auth_header[@]}" "$api")" || http_code=""
  if [[ "$http_code" != "200" ]]; then
    rm -f "$body"
    return 1
  fi
  size="$(jq -r '.size // empty' "$body" 2>/dev/null || true)"
  rm -f "$body"
  [[ -z "$size" ]] && size="unknown"
  raw_url="https://raw.githubusercontent.com/${owner}/${repo}/${sha}/${path}"
  add_or_merge_record "$raw_url" "$size" "no" "" "$owner" "$repo" "$sha" "$path" "${path%/*}"
  return 0
}

declare -A include_targets_needed

while IFS=$'\t' read -r url size include_flag include_targets; do
  [[ -z "$url" ]] && continue
  input_rows=$((input_rows + 1))
  rel="${url#https://raw.githubusercontent.com/}"
  IFS='/' read -r owner repo sha path_rest <<<"$rel"
  path="${rel#${owner}/${repo}/${sha}/}"
  dir="${path%/*}"
  add_or_merge_record "$url" "$size" "$include_flag" "$include_targets" "$owner" "$repo" "$sha" "$path" "$dir"

  if [[ "$include_flag" == "yes" && -n "$include_targets" ]]; then
    include_yes_rows=$((include_yes_rows + 1))
    IFS=',' read -ra targets_arr <<<"$include_targets"
    for tgt in "${targets_arr[@]}"; do
      tgt="${tgt#"${tgt%%[![:space:]]*}"}"
      tgt="${tgt%"${tgt##*[![:space:]]}"}"
      [[ -z "$tgt" ]] && continue
      targets_declared=$((targets_declared + 1))
      resolved="$(resolve_path "$dir" "$tgt")"
      include_targets_needed["$owner|$repo|$sha|$resolved"]=1
    done
  fi
done < "$INPUT"

targets_unique=${#include_targets_needed[@]}

# Count targets already present.
for key in "${!include_targets_needed[@]}"; do
  if [[ -n "${path_to_id[$key]:-}" ]]; then
    targets_present=$((targets_present + 1))
  fi
done

# Fetch missing include targets.
for key in "${!include_targets_needed[@]}"; do
  if [[ -n "${path_to_id[$key]:-}" ]]; then
    continue
  fi
  IFS='|' read -r owner repo sha path <<<"$key"
  if fetch_include_target "$owner" "$repo" "$sha" "$path"; then
    targets_fetched=$((targets_fetched + 1))
  else
    targets_failed=$((targets_failed + 1))
    echo "WARN: failed to fetch include target ${owner}/${repo}/${path} at ${sha}" >&2
  fi
done

emit() {
  local url="$1" size="$2" inc="$3" targets="$4"
  if [[ -n "$OUTPUT" ]]; then
    printf "%s\t%s\t%s\t%s\n" "$url" "$size" "$inc" "$targets" >> "$OUTPUT"
  else
    printf "%s\t%s\t%s\t%s\n" "$url" "$size" "$inc" "$targets"
  fi
}

[[ -n "$OUTPUT" ]] && : > "$OUTPUT"

for id in "${!rec_url[@]}"; do
  emit "${rec_url[$id]}" "${rec_size[$id]}" "${rec_incflag[$id]}" "${rec_targets[$id]}"
done

output_rows=${#rec_url[@]}

if [[ -n "$OUTPUT" ]]; then
  echo "Augmented metadata written to $OUTPUT"
fi

echo "=== get_includes summary ===" >&2
echo "Input rows:           $input_rows" >&2
echo "Deduped (dropped):    $dedup_dropped" >&2
echo "Output rows:          $output_rows" >&2
echo "--- include stats ---" >&2
echo "Include=yes rows:     $include_yes_rows" >&2
echo "Targets seen:         $targets_declared (uniq: $targets_unique)" >&2
echo "Targets already had:  $targets_present" >&2
echo "Targets fetched:      $targets_fetched" >&2
echo "Targets fetch failed: $targets_failed" >&2
