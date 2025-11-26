#!/usr/bin/env bash
set -euo pipefail

# Launch DBeaver pointed at a specific ledger and SQL file (e.g., diffs.sql).
# Usage: dbeaver_dive.sh [--verbose] <ledger_path> <sql_file>

quiet=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --verbose)
      quiet=0
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--verbose] <ledger_path> <sql_file>" >&2
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
    *)
      break
      ;;
  esac
done

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 [--verbose] <ledger_path> <sql_file>" >&2
  exit 1
fi

LEDGER_PATH="$(cd -- "$(dirname -- "$1")" && pwd)/$(basename "$1")"
SQL_FILE="$(cd -- "$(dirname -- "$2")" && pwd)/$(basename "$2")"

if [[ ! -f "$LEDGER_PATH" ]]; then
  echo "Ledger not found: $LEDGER_PATH" >&2
  exit 1
fi
if [[ ! -f "$SQL_FILE" ]]; then
  echo "SQL file not found: $SQL_FILE" >&2
  exit 1
fi
BASE_NAME="$(basename "$LEDGER_PATH")"
CONN_NAME="${BASE_NAME%.*}"
BASELINE_PATH="$(cd -- "$(dirname -- "$LEDGER_PATH")/../bean_sql" && pwd)/${CONN_NAME}.sqlite"
if [[ ! -f "$BASELINE_PATH" ]]; then
  echo "Baseline sqlite not found: $BASELINE_PATH" >&2
  exit 1
fi

# Use repo-local workspace so test runs remain isolated.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE="$ROOT_DIR/tools/dbeaver"
DRIVER_CFG="$WORKSPACE/dbeaver-beancount-testing-driver.xml"
VARS_FILE="$WORKSPACE/dbeaver-vars.properties"

# Derive the driver version from Version.java so the driver XML resolves the latest jar.
VERSION_JAVA="$ROOT_DIR/jdbc/src/main/java/com/beancount/jdbc/Version.java"
BEANCOUNT_JDBC_VERSION="$(
  python3 - <<'PY' "$VERSION_JAVA"
import re, sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
def grab(name, default=None):
    m = re.search(rf'{name}\s*=\s*"?([A-Za-z0-9._-]+)"?;', text)
    return m.group(1) if m else default
major = grab("MAJOR")
minor = grab("MINOR")
patch = grab("PATCH")
qualifier = grab("QUALIFIER", "").strip('"').strip("'")
if not (major and minor and patch):
    sys.exit("Could not parse version fields from Version.java")
full = f"{major}.{minor}.{patch}"
if qualifier:
    full = f"{full}-{qualifier}"
print(full)
PY
)"
export BEANCOUNT_JDBC_VERSION

# Pick the URL path style based on which DBeaver binary we are calling.
DB_BIN="$(command -v dbeaver || true)"
DB_BIN_RESOLVED="$(readlink -f "$DB_BIN" 2>/dev/null || echo "$DB_BIN")"
LEDGER_URL_PATH="$LEDGER_PATH"
BASELINE_URL_PATH="$BASELINE_PATH"
if [[ "$DB_BIN_RESOLVED" == *.exe || "$DB_BIN_RESOLVED" == /mnt/c/* || "$DB_BIN_RESOLVED" == /c/* ]]; then
  # Windows DBeaver: use Windows-style path.
  if command -v wslpath >/dev/null 2>&1; then
    LEDGER_URL_PATH="$(wslpath -m "$LEDGER_PATH")"
    BASELINE_URL_PATH="$(wslpath -m "$BASELINE_PATH")"
  elif command -v cygpath >/dev/null 2>&1; then
    LEDGER_URL_PATH="$(cygpath -m "$LEDGER_PATH")"
    BASELINE_URL_PATH="$(cygpath -m "$BASELINE_PATH")"
  fi
fi

# Ensure workspace exists before writing variables.
mkdir -p "$WORKSPACE"

# Export variables for DBeaver (-vars) using lowercase keys that mirror the script vars.
cat > "$VARS_FILE" <<EOF
script_dir=$SCRIPT_DIR
root_dir=$ROOT_DIR
workspace=$WORKSPACE
driver_cfg=$DRIVER_CFG
vars_file=$VARS_FILE
ledger_dir=$(cd "$(dirname "$LEDGER_PATH")" && pwd -P)
ledger_path=$LEDGER_PATH
ledger_url_path=$LEDGER_URL_PATH
baseline_path=$BASELINE_PATH
baseline_url_path=$BASELINE_URL_PATH
sql_file=$SQL_FILE
version_java=$VERSION_JAVA
beancount_jdbc_version=$BEANCOUNT_JDBC_VERSION
base_name=$BASE_NAME
conn_name=$CONN_NAME
datasource=$CONN_NAME
db_bin=$DB_BIN
db_bin_resolved=$DB_BIN_RESOLVED
EOF

# Run DBeaver using the testing workspace + testing driver config,
# creating a connection for the first ledger and opening iffs.sql in the GUI.

if [[ $quiet -eq 1 ]]; then
  dbeaver-ce \
    -data "$WORKSPACE" \
    -f "$SQL_FILE" \
    -con "driver=SQLite|name=${CONN_NAME}-sqlite|database=$BASELINE_URL_PATH|create=true|connect=true|openConsole=false" \
    -con "driver=Beancount JDBC (Testing)|name=$CONN_NAME|url=jdbc:beancount:$LEDGER_URL_PATH|create=true|connect=true|openConsole=false" \
    -vars "$VARS_FILE" \
    -vmargs -Ddbeaver.drivers.configuration-file="$DRIVER_CFG" \
    >/dev/null 2>&1
else
  dbeaver-ce \
    -data "$WORKSPACE" \
    -f "$SQL_FILE" \
    -con "driver=SQLite|name=${CONN_NAME}-sqlite|database=$BASELINE_URL_PATH|create=true|connect=true|openConsole=false" \
    -con "driver=Beancount JDBC (Testing)|name=$CONN_NAME|url=jdbc:beancount:$LEDGER_URL_PATH|create=true|connect=true|openConsole=false" \
    -vars "$VARS_FILE" \
    -vmargs -Ddbeaver.drivers.configuration-file="$DRIVER_CFG"
fi
