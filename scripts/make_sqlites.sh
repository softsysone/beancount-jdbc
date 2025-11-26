#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd -- "$script_dir/../../../../.." && pwd)"
ledgers_dir="$project_root/jdbc/src/test/resources/regression/ledgers"
baselines_dir="$project_root/jdbc/src/test/resources/regression/bean_sql"
sql_script="$project_root/third_party/beancount/experiments/sql/sql.py"
debug=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    -d|--debug)
      debug=1
      shift
      ;;
    --)
      shift
      break
      ;;
    *)
      break
      ;;
  esac
done
mkdir -p "$baselines_dir"
python_bin="python3"
if [ -x "$project_root/.venv/bin/python3" ]; then
  python_bin="$project_root/.venv/bin/python3"
fi
export PYTHONPATH="$project_root/third_party"
export BEANCOUNT_FLAGS="--no-cache"
for ledger_path in "$ledgers_dir"/*; do
  ledger="$(basename "$ledger_path")"
  [[ -f "$ledger_path" ]] || continue
  case "$ledger" in
    *.beancount|*.bean)
      name="${ledger%.*}"
      target="$baselines_dir/${name}.sqlite"
      temp_target="$(mktemp -p "$baselines_dir" "${name}.XXXXXX.sqlite")"
      rm -f "$temp_target"
      echo "Generating ${name}.sqlite from $ledger"
      "$python_bin" - "$sql_script" "$ledger_path" "$temp_target" "$debug" <<'PY'
import runpy
import sys
import logging

script_path, ledger_path, target_path, debug_flag = sys.argv[1:5]
debug = debug_flag == "1"

if not debug:
    # Preconfigure logging so sql.py's basicConfig(log.INFO) is a no-op.
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

context = runpy.run_path(script_path)
command = context["main"]

try:
    # Invoke Click command directly since sql.py has no __main__ guard.
    command.main(
        args=[ledger_path, target_path],
        prog_name="bean-sql",
        standalone_mode=False,
    )
except SystemExit as exc:
    if exc.code:
        raise
PY
      if [ ! -f "$temp_target" ]; then
          echo "Error: Failed to create temporary database for $ledger" >&2
          exit 1
      fi
      # Copy instead of rename because files under /mnt/c can be marked read-only by Windows.
      cp "$temp_target" "$target"
      rm -f "$temp_target"
      if [ ! -f "$target" ]; then
          echo "Error: Failed to update $target" >&2
          exit 1
      fi
      echo "OK: ${name}.sqlite"
      ;;
    *)
      echo "SKIP (unsupported extension): $ledger"
      ;;
  esac
done
rm -f "$ledgers_dir"/*.picklecache 2>/dev/null || true
echo "Done. Baselines up to date in $baselines_dir"
