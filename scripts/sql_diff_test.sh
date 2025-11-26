#!/usr/bin/env bash
set -euo pipefail

# Run bean-sql vs JDBC diffs using org.hsqldb:sqltool and jdbc/src/test/resources/sql/diffs.sql.
# Supports WSL and Windows Git Bash. Defaults expect:
#   - tools/sqltool/sqltool-*.jar
#   - jdbc/build/libs/beancount-jdbc-*.jar + jdbc/build/runtime-libs

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd -- "$script_dir/.." && pwd)"

ledgers_dir="${LEDGERS_DIR:-$project_root/jdbc/src/test/resources/regression/ledgers}"
diffs_sql="${DIFFS_SQL:-$project_root/jdbc/src/test/resources/sql/diffs.sql}"
dbeaver_dive_script="$project_root/scripts/dbeaver_dive.sh"
workspace_dir_default="$project_root/tools/sqltool"  # matches ${workspace} usage in diffs.sql
workspace_dir="${WORKSPACE_DIR:-$workspace_dir_default}"

sqltool_jar="${SQLTOOL_JAR:-}"
if [[ -z "$sqltool_jar" ]]; then
  sqltool_jar="$(ls "$project_root"/tools/sqltool/sqltool-*.jar 2>/dev/null | head -n 1 || true)"
fi
version_java="$project_root/jdbc/src/main/java/com/beancount/jdbc/Version.java"
python_bin="python3"
command -v python3 >/dev/null 2>&1 || python_bin="python"
beancount_version="$(
  "$python_bin" - <<'PY' "$version_java"
import pathlib, re, sys
text = pathlib.Path(sys.argv[1]).read_text()
def grab(name):
    m = re.search(rf'{name}\s*=\s*"?([A-Za-z0-9._-]+)"?;', text)
    if not m:
        sys.exit(1)
    return m.group(1)
major, minor, patch = grab("MAJOR"), grab("MINOR"), grab("PATCH")
qualifier_match = re.search(r'QUALIFIER\s*=\s*"?([A-Za-z0-9._-]*)"?;', text)
qualifier = qualifier_match.group(1) if qualifier_match else ""
ver = f"{major}.{minor}.{patch}"
if qualifier:
    ver = f"{ver}-{qualifier}"
print(ver)
PY
)"

jdbc_jar_default="$project_root/jdbc/build/libs/beancount-jdbc-${beancount_version}.jar"
jdbc_jar="${JDBC_JAR:-$jdbc_jar_default}"
runtime_libs="${RUNTIME_LIBS:-$project_root/jdbc/build/runtime-libs}"

if [[ -z "$sqltool_jar" || ! -f "$sqltool_jar" ]]; then
  echo "Missing sqltool jar (expected tools/sqltool/sqltool-*.jar). Set SQLTOOL_JAR or download first." >&2
  exit 1
fi
if [[ -z "$jdbc_jar" || ! -f "$jdbc_jar" ]]; then
  echo "Missing JDBC jar ($jdbc_jar). Building with ./gradlew :jdbc:jar :jdbc:copyRuntimeLibs ..." >&2
  if ! (cd "$project_root" && ./gradlew :jdbc:jar :jdbc:copyRuntimeLibs); then
    echo "Gradle build failed; aborting." >&2
    exit 1
  fi
  if [[ ! -f "$jdbc_jar" ]]; then
    echo "JDBC jar still missing after build: $jdbc_jar" >&2
    exit 1
  fi
fi
if [[ ! -d "$runtime_libs" ]]; then
  echo "Missing runtime libs. Populate with: ./gradlew :jdbc:copyRuntimeLibs" >&2
  exit 1
fi
if [[ ! -f "$diffs_sql" ]]; then
  echo "Missing diffs.sql at $diffs_sql" >&2
  exit 1
fi

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    cp_sep=';'
    to_native() { command -v cygpath >/dev/null 2>&1 && cygpath -m "$1" || printf '%s' "$1"; }
    ;;
  *)
    cp_sep=':'
    to_native() { printf '%s' "$1"; }
    ;;
esac

to_uri() {
  local path="$1"
  # Assumes to_native already normalized slashes.
  if [[ "$path" =~ ^[A-Za-z]:/ ]]; then
    printf 'file:///%s' "$path"
  else
    printf 'file://%s' "$path"
  fi
}

classpath_parts=()
classpath_parts+=("$(to_native "$sqltool_jar")")
classpath_parts+=("$(to_native "$jdbc_jar")")
for jar in "$runtime_libs"/*.jar; do
  [[ -f "$jar" ]] && classpath_parts+=("$(to_native "$jar")")
done
classpath="$(IFS="$cp_sep"; echo "${classpath_parts[*]}")"

java_bin="${JAVA_BIN:-java}"
filter_ledger=""
show_output=0
show_plan=0
skip_plugins=0
skip_passed_file=""
skip_ledgers_file=""
declare -A skip_passed_map=()
declare -A skip_manual_map=()
interrupted=0

on_interrupt() {
  interrupted=1
  echo "Interrupted; stopping after current ledger..." >&2
}
trap on_interrupt INT

while [[ $# -gt 0 ]]; do
  case "$1" in
    -l|--ledger)
      filter_ledger="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: sql_diff_test.sh [-l ledger_name]

Runs diffs.sql via SQLTool against each ledger baseline. Options:
  -l, --ledger   Only run for a single ledger (name without extension)
      --show-output   Print full SQLTool output for each ledger
      --show-plan     Allow Calcite codegen/plan dump (defaults off)
EOF
      exit 0
      ;;
    --show-output)
      show_output=1
      shift
      ;;
    --show-plan)
      show_plan=1
      shift
      ;;
    --skip-plugins)
      skip_plugins=1
      shift
      ;;
    --skip-passed)
      skip_passed_file="$2"
      shift 2
      ;;
    --skip-ledgers)
      skip_ledgers_file="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

load_skip_file() {
  local file="$1"
  local -n map_ref="$2"
  if [[ -f "$file" ]]; then
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      map_ref["$line"]=1
    done <"$file"
  fi
}

[[ -n "$skip_passed_file" ]] && load_skip_file "$skip_passed_file" skip_passed_map
[[ -n "$skip_ledgers_file" ]] && load_skip_file "$skip_ledgers_file" skip_manual_map

status=0
for ledger_path in "$ledgers_dir"/*; do
  if [[ "$interrupted" -eq 1 ]]; then
    status=130
    break
  fi
  [[ -f "$ledger_path" ]] || continue
  case "$ledger_path" in
    *.beancount|*.bean) ;;
    *) continue ;;
  esac
  ledger_file="$(basename "$ledger_path")"
  ledger_name="${ledger_file%.*}"
  [[ -n "$filter_ledger" && "$ledger_name" != "$filter_ledger" ]] && continue

  if [[ -n "$skip_ledgers_file" && -n "${skip_manual_map[$ledger_name]:-}" ]]; then
    echo "Skip $ledger_name: listed in $skip_ledgers_file"
    continue
  fi
  if [[ -n "$skip_passed_file" && -n "${skip_passed_map[$ledger_name]:-}" ]]; then
    echo "Skip $ledger_name: previously passed (from $skip_passed_file)"
    continue
  fi

  # Detect plugin usage (ignore commented lines).
  has_plugin=0
  if grep -Ei '^[[:space:]]*plugin[[:space:]]' "$ledger_path" | grep -Ev '^[[:space:]]*[#;]' >/dev/null; then
    has_plugin=1
  fi
  if [[ "$skip_plugins" -eq 1 && "$has_plugin" -eq 1 ]]; then
    echo "Skip $ledger_name: plugin detected (skip-plugins enabled)"
    continue
  fi

  baseline="$project_root/jdbc/src/test/resources/regression/bean_sql/${ledger_name}.sqlite"
  if [[ ! -f "$baseline" ]]; then
    echo "Skip $ledger_name: missing baseline $baseline" >&2
    continue
  fi

  ledger_native="$(to_native "$ledger_path")"
  workspace_native="$(to_native "$workspace_dir")"
  ledger_url="jdbc:beancount:$(to_uri "$ledger_native")"

  echo "== $ledger_name =="
  output_file="$(mktemp)"
  error_file="$(mktemp)"
  java_props=(
    "-Dworkspace=$workspace_native"
    "-Ddatasource=$ledger_name"
  )
  if [[ $show_plan -eq 0 ]]; then
    java_props+=(
      "-Dcalcite.debug=false"
      "-Dcalcite.debug.codegen=false"
    )
  fi
  if ! "$java_bin" "${java_props[@]}" -cp "$classpath" org.hsqldb.cmdline.SqlTool \
      --driver=com.beancount.jdbc.BeancountDriver \
      --inlineRc url="$ledger_url" \
      -Pworkspace="$workspace_native" \
      -Pdatasource="$ledger_name" \
      "$diffs_sql" >"$output_file" 2>"$error_file"; then
    echo "SQLTool failed for $ledger_name"
    [[ -s "$output_file" ]] && cat "$output_file"
    [[ -s "$error_file" ]] && cat "$error_file"
    if grep -qiE 'failed to load ledger|validation' "$error_file"; then
      echo "$ledger_file failed validation; JDBC does not support this plugin/ledger yet. Skipping dive."
      rm -f "$output_file" "$error_file"
      continue
    fi
    if [[ -x "$dbeaver_dive_script" ]]; then
      echo "Launching DBeaver for load failure on $ledger_name via dbeaver_dive.sh..."
      "$dbeaver_dive_script" "$ledger_path" "$diffs_sql" || true
    fi
    rm -f "$output_file" "$error_file"
    exit 1
  fi

  [[ $show_output -eq 1 ]] && cat "$output_file"

  if grep -Eq '^[[:alnum:] _-]+\s+(table|view)\s+[1-9][0-9]*\s*$' "$output_file"; then
    echo "Differences found for $ledger_name"
    [[ $show_output -eq 1 ]] || cat "$output_file"
    if [[ "$has_plugin" -eq 1 ]]; then
      echo "$ledger_file uses a plugin; JDBC driver does not support plugins, so discrepancies are expected. Skipping DBeaver dive."
      rm -f "$output_file" "$error_file"
      continue
    fi
    if [[ -x "$dbeaver_dive_script" ]]; then
      echo "Launching DBeaver for $ledger_name via dbeaver_dive.sh..."
      "$dbeaver_dive_script" "$ledger_path" "$diffs_sql" || true
    else
      echo "dbeaver_dive.sh not found or not executable at $dbeaver_dive_script" >&2
    fi
    rm -f "$output_file" "$error_file"
    exit 1
  else
    echo "OK: no diffs"
    if [[ -n "$skip_passed_file" ]]; then
      mkdir -p "$(dirname "$skip_passed_file")"
      (printf "%s\n" "$ledger_name"; printf "%s\n" "${!skip_passed_map[@]}") | sort -u >"$skip_passed_file"
      skip_passed_map["$ledger_name"]=1
    fi
  fi
  rm -f "$output_file" "$error_file"
done

exit "$status"
