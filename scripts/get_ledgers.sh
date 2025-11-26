#!/usr/bin/env bash
set -euo pipefail

# Downloads public Beancount ledgers into the regression resources folder.
# - Keeps a flat destination directory.
# - Only applies a slug to filenames when the basename would collide.
# - Adds a trailing source comment to each downloaded file.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_LIST="$SCRIPT_DIR/public_ledger_sources.txt"
DEST="/mnt/c/Users/jacob-walker/Documents/GitHub/beancount-jdbc/jdbc/src/test/resources/regression/ledgers"
META="$DEST/.ledger_meta"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }
}

require_cmd curl
require_cmd awk
require_cmd cmp
require_cmd mktemp

[[ -f "$SOURCE_LIST" ]] || { echo "Source list not found: $SOURCE_LIST" >&2; exit 1; }
mkdir -p "$DEST"
touch "$META"

TMPDIR="$(mktemp -d)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT INT TERM

# Read URLs from the source list (ignoring comments/blank lines).
mapfile -t URLS < <(grep -E '^(https?|ftp)://' "$SOURCE_LIST" | tr -d '\r')
if [[ ${#URLS[@]} -eq 0 ]]; then
  echo "No sources to process in $SOURCE_LIST" >&2
  exit 1
fi

# Count basenames to detect collisions.
declare -A BASENAME_COUNT
for url in "${URLS[@]}"; do
  base="${url##*/}"
  BASENAME_COUNT["$base"]=$(( ${BASENAME_COUNT["$base"]:-0} + 1 ))
done

# Build a slug: GitHub raw URLs -> owner-repo-basename; fallback to sanitized full path.
slug_for_url() {
  local url="$1" base="$2" host rest owner repo slug trimmed
  trimmed="${url#*://}"          # drop scheme
  host="${trimmed%%/*}"
  rest="${trimmed#*/}"
  if [[ "$host" == "raw.githubusercontent.com" ]]; then
    owner="${rest%%/*}"
    rest="${rest#*/}"
    repo="${rest%%/*}"
    if [[ -n "$owner" && -n "$repo" ]]; then
      slug="${owner}-${repo}-${base}"
      slug="${slug//[^A-Za-z0-9_.-]/-}"
      slug="$(echo "$slug" | tr 'A-Z' 'a-z' | sed -E 's/-+/-/g; s/^-//; s/-$//')"
      printf "%s" "$slug"
      return
    fi
  fi
  slug="${trimmed%/"$base"}"  # drop basename
  slug="${slug//\//-}"        # replace / with -
  slug="${slug//[^A-Za-z0-9_.-]/-}"
  slug="$(echo "$slug" | tr 'A-Z' 'a-z' | sed -E 's/-+/-/g; s/^-//; s/-$//')"
  printf "%s-%s" "$slug" "$base"
}

lookup_meta() {
  local url="$1" etag='' last=''
  while IFS='|' read -r m_url m_etag m_last; do
    [[ "$m_url" == "$url" ]] || continue
    etag="$m_etag"
    last="$m_last"
    break
  done < "$META"
  printf "%s|%s" "$etag" "$last"
}

update_meta() {
  local url="$1" etag="$2" last="$3" tmp="$TMPDIR/meta.$$"
  awk -v url="$url" -v etag="$etag" -v last="$last" 'BEGIN{FS=OFS="|"} {if($1==url){next} print} END{print url, etag, last}' "$META" > "$tmp"
  mv "$tmp" "$META"
}

parse_header() {
  local header_file="$1" key="$2"
  sed -n "s/^$key:[[:space:]]*//Ip" "$header_file" | head -n1 | tr -d '\r'
}

for url in "${URLS[@]}"; do
  base="${url##*/}"
  dest_name="$base"
  if (( ${BASENAME_COUNT["$base"]:-0} > 1 )); then
    dest_name="$(slug_for_url "$url" "$base")"
    echo "COLLISION: $base -> $dest_name"
  fi

  dest_path="$DEST/$dest_name"

  IFS='|' read -r old_etag old_last <<<"$(lookup_meta "$url")"
  header_tmp="$TMPDIR/headers.$$"
  body_tmp="$TMPDIR/body.$$"

  echo "FETCH: $url"
  echo "  -> $dest_name"

  curl_args=(-sSL -D "$header_tmp" -w '%{http_code}')
  [[ -n "$old_etag" ]] && curl_args+=(-H "If-None-Match: $old_etag")
  [[ -n "$old_last" ]] && curl_args+=(-H "If-Modified-Since: $old_last")

  http_code="$(curl "${curl_args[@]}" -o "$body_tmp" "$url")"

  if [[ "$http_code" == "304" ]]; then
    echo "SKIP (not modified): $dest_name"
    rm -f "$body_tmp" "$header_tmp"
    continue
  fi

  if [[ "$http_code" != 2* ]]; then
    echo "ERROR: HTTP $http_code for $url (target $dest_name)" >&2
    rm -f "$body_tmp" "$header_tmp"
    continue
  fi

  if [[ ! -s "$body_tmp" ]]; then
    echo "WARN: Empty download for $url" >&2
    rm -f "$body_tmp" "$header_tmp"
    continue
  fi

  printf '\n; Source: %s\n' "$url" >> "$body_tmp"

  action="ADD"
  if [[ -f "$dest_path" ]]; then
    if cmp -s "$body_tmp" "$dest_path"; then
      echo "SKIP (unchanged content): $dest_name"
      rm -f "$body_tmp" "$header_tmp"
      continue
    fi
    action="UPDATE"
  fi

  mv "$body_tmp" "$dest_path"

  new_etag="$(parse_header "$header_tmp" 'ETag')"
  new_last="$(parse_header "$header_tmp" 'Last-Modified')"
  update_meta "$url" "$new_etag" "$new_last"

  echo "$action: $dest_name"
  rm -f "$header_tmp"
done

echo "✅ Ledger sync complete. Files in: $DEST"
