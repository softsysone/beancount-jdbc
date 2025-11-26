#!/usr/bin/env bash
set -euo pipefail

# Searches GitHub for public Beancount ledger files (.beancount) using the
# official search API and prints raw file URLs (one per line).

usage() {
  cat <<'EOF'
Usage: find_ledgers.sh [options]

Find public Beancount ledgers on GitHub via the Search API (default query:
extension:beancount) and print the corresponding raw URLs.

Options:
  -q, --query       Search query (default: extension:beancount)
  -o, --output      Write URLs to a file instead of stdout
  -s, --include-size Include file size (adds one contents API call per result)
  -i, --check-include Flag include directives (adds one contents API call per result and emits include targets)
  -p, --per-page    Results per page (1-100, default: 100)
  -m, --max-pages   Maximum pages to fetch (default: 10; API caps at 1000 results)
  -t, --token       GitHub token (overrides GITHUB_TOKEN/GH_TOKEN)
  -h, --help        Show this help text
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOKEN_FILE="$SCRIPT_DIR/.github_token"

# Use extension-based search because the GitHub v3 search API does not support
# globbing on path (path:*.beancount). The web UI accepts that, but the API
# returns zero results. extension:beancount reliably finds *.beancount files.
QUERY="extension:beancount"
OUTPUT_DEST=""
PER_PAGE=100
MAX_PAGES=10
TOKEN="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
INCLUDE_SIZE=false
CHECK_INCLUDE=false

# Fallback to token file in the script directory if env vars are unset.
if [[ -z "$TOKEN" && -f "$TOKEN_FILE" ]]; then
  TOKEN="$(tr -d '\r\n' < "$TOKEN_FILE")"
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    -q|--query)
      QUERY="$2"; shift 2;;
    -o|--output)
      OUTPUT_DEST="$2"; shift 2;;
    -s|--include-size)
      INCLUDE_SIZE=true; shift;;
    -i|--check-include)
      CHECK_INCLUDE=true; shift;;
    -p|--per-page)
      PER_PAGE="$2"; shift 2;;
    -m|--max-pages)
      MAX_PAGES="$2"; shift 2;;
    -t|--token)
      TOKEN="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1;;
  esac
done

require_cmd curl
require_cmd jq
if [[ "$CHECK_INCLUDE" == true ]]; then
  require_cmd base64
fi

if ! [[ "$PER_PAGE" =~ ^[0-9]+$ && "$PER_PAGE" -ge 1 && "$PER_PAGE" -le 100 ]]; then
  echo "Invalid per-page value: $PER_PAGE (must be 1-100)" >&2
  exit 1
fi

if ! [[ "$MAX_PAGES" =~ ^[0-9]+$ && "$MAX_PAGES" -ge 1 ]]; then
  echo "Invalid max-pages value: $MAX_PAGES (must be >=1)" >&2
  exit 1
fi

if [[ -n "$OUTPUT_DEST" ]]; then
  : > "$OUTPUT_DEST"
fi

emit() {
  local url="$1" size="${2:-}" include_flag="${3:-}" include_targets="${4:-}"
  if [[ -n "$OUTPUT_DEST" ]]; then
    if [[ "$INCLUDE_SIZE" == true && "$CHECK_INCLUDE" == true ]]; then
      printf "%s\t%s\t%s\t%s\n" "$url" "$size" "$include_flag" "$include_targets" >> "$OUTPUT_DEST"
    elif [[ "$INCLUDE_SIZE" == true ]]; then
      printf "%s\t%s\n" "$url" "$size" >> "$OUTPUT_DEST"
    elif [[ "$CHECK_INCLUDE" == true ]]; then
      printf "%s\t%s\t%s\n" "$url" "$include_flag" "$include_targets" >> "$OUTPUT_DEST"
    else
      echo "$url" >> "$OUTPUT_DEST"
    fi
  else
    if [[ "$INCLUDE_SIZE" == true && "$CHECK_INCLUDE" == true ]]; then
      printf "%s\t%s\t%s\t%s\n" "$url" "$size" "$include_flag" "$include_targets"
    elif [[ "$INCLUDE_SIZE" == true ]]; then
      printf "%s\t%s\n" "$url" "$size"
    elif [[ "$CHECK_INCLUDE" == true ]]; then
      printf "%s\t%s\t%s\n" "$url" "$include_flag" "$include_targets"
    else
      echo "$url"
    fi
  fi
}

TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT INT TERM

API="https://api.github.com/search/code"
headers_file="$TMP_DIR/headers"
body_file="$TMP_DIR/body"
declare -A SEEN
page=1
total=0
auth_header=()

if [[ -z "$TOKEN" ]]; then
  echo "Note: using unauthenticated requests (low rate limit). Set GITHUB_TOKEN for better coverage." >&2
else
  auth_header=( -H "Authorization: Bearer $TOKEN" )
fi

fetch_metadata() {
  local contents_url="$1" tmp="$TMP_DIR/contents.$$" http_code size_val include_flag="unknown" include_targets=""

  http_code="$(curl -sS -w '%{http_code}' -o "$tmp" -H "Accept: application/vnd.github+json" "${auth_header[@]}" "$contents_url")" || {
    rm -f "$tmp"
    echo -e "\t\t"
    return 1
  }

  if [[ "$http_code" != "200" ]]; then
    rm -f "$tmp"
    echo -e "\t\t"
    return 1
  fi

  size_val="$(jq -r '.size // empty' "$tmp" 2>/dev/null || true)"

  if [[ "$CHECK_INCLUDE" == true ]]; then
    local content_b64 encoding decode_tmp
    content_b64="$(jq -r '.content // empty' "$tmp" 2>/dev/null || true)"
    encoding="$(jq -r '.encoding // empty' "$tmp" 2>/dev/null || true)"
    if [[ "$encoding" == "base64" && -n "$content_b64" ]]; then
      decode_tmp="$TMP_DIR/decoded.$$"
      printf '%s' "$content_b64" | tr -d '\r\n' | base64 -d > "$decode_tmp" 2>/dev/null || true
      if [[ -s "$decode_tmp" ]]; then
        include_targets="$(
          awk '
            BEGIN { FS="\""; out=""; }
            /^[[:space:]]*[;#]/ { next }
            /^[[:space:]]*include[[:space:]]+"[^"]+"/ {
              p=$2
              if (!seen[p]++) {
                if (out != "") out = out "," p; else out = p
              }
            }
            END { print out }
          ' "$decode_tmp"
        )"
        if [[ -n "$include_targets" ]]; then
          include_flag="yes"
        else
          include_flag="no"
        fi
      fi
      rm -f "$decode_tmp"
    fi
  fi

  rm -f "$tmp"
  echo -e "${size_val}\t${include_flag}\t${include_targets}"
}

while (( page <= MAX_PAGES )); do
  echo "Fetching page $page ..." >&2

  curl_args=(
    -sS
    -w '%{http_code}'
    -D "$headers_file"
    -o "$body_file"
    -H "Accept: application/vnd.github+json"
  )
  if [[ -n "$TOKEN" ]]; then
    curl_args+=( -H "Authorization: Bearer $TOKEN" )
  fi
  curl_args+=(
    --get
    --data-urlencode "q=$QUERY"
    --data-urlencode "per_page=$PER_PAGE"
    --data-urlencode "page=$page"
    "$API"
  )

  http_code="$(curl "${curl_args[@]}")" || { echo "ERROR: curl failed for page $page" >&2; exit 1; }

  if [[ "$http_code" != "200" ]]; then
    message="$(jq -r '.message // empty' "$body_file" 2>/dev/null || true)"
    echo "ERROR: GitHub API returned HTTP $http_code${message:+ - $message}" >&2
    exit 1
  fi

  page_count="$(jq '.items | length' "$body_file")"
  if ! [[ "$page_count" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Unexpected response structure on page $page" >&2
    exit 1
  fi

  if (( page_count == 0 )); then
    echo "No more results after page $((page - 1))." >&2
    break
  fi

  while IFS=$'\t' read -r repo path sha contents_url; do
    [[ -n "$repo" && -n "$path" && -n "$sha" ]] || continue
    key="${repo}/${path}"
    if [[ -n "${SEEN[$key]:-}" ]]; then
      continue
    fi
    SEEN["$key"]=1
    raw_url="https://raw.githubusercontent.com/${repo}/${sha}/${path}"
    size_val=""
    include_flag=""
    include_targets=""
    if [[ ( "$INCLUDE_SIZE" == true || "$CHECK_INCLUDE" == true ) && -n "$contents_url" ]]; then
      IFS=$'\t' read -r size_val include_flag include_targets <<<"$(fetch_metadata "$contents_url")"
    fi
    if [[ "$INCLUDE_SIZE" == true && -z "$size_val" ]]; then
      size_val="unknown"
    fi
    if [[ "$CHECK_INCLUDE" == true && -z "$include_flag" ]]; then
      include_flag="unknown"
    fi
    emit "$raw_url" "$size_val" "$include_flag" "$include_targets"
    total=$(( total + 1 ))
  done < <(jq -r '.items[] | [.repository.full_name, .path, .sha, .url] | @tsv' "$body_file")

  rate_remaining="$(sed -n 's/^X-RateLimit-Remaining:[[:space:]]*//Ip' "$headers_file" | head -n1 | tr -d '\r')"
  rate_reset="$(sed -n 's/^X-RateLimit-Reset:[[:space:]]*//Ip' "$headers_file" | head -n1 | tr -d '\r')"
  if [[ "$rate_remaining" =~ ^[0-9]+$ && "$rate_remaining" -le 1 && "$rate_reset" =~ ^[0-9]+$ ]]; then
    now_epoch="$(date +%s)"
    wait_sec=$(( rate_reset - now_epoch + 2 ))
    if (( wait_sec > 0 )); then
      echo "Rate limit nearly exhausted; sleeping for ${wait_sec}s until reset." >&2
      sleep "$wait_sec"
    fi
  fi

  if (( page_count < PER_PAGE )); then
    echo "Last page reached with $page_count results." >&2
    break
  fi

  page=$(( page + 1 ))
done

echo "Discovered $total unique ledger URLs." >&2
