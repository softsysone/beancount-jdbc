#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <ledger_dir> <jdbc_export_dir>" >&2
  exit 1
fi
LEDGER_DIR=$1
JDBC_DIR=$2
SCRIPT=$(dirname "$0")/diff_with_beansql.py

PASSED=0
FAILED=0

run_case() {
  local ledger=$1
  local export_dir=$2
  local bean_db=$3
  echo "Running regression for ${ledger}..."
  if python3 "$SCRIPT" "$ledger" "$export_dir" "$bean_db"; then
    PASSED=$((PASSED+1))
  else
    FAILED=$((FAILED+1))
  fi
}

run_case "$LEDGER_DIR/multi_commodity.beancount" "$JDBC_DIR/multi_commodity" "$JDBC_DIR/multi_commodity.sqlite"
run_case "$LEDGER_DIR/booking_lifo.beancount" "$JDBC_DIR/booking_lifo" "$JDBC_DIR/booking_lifo.sqlite"
run_case "$LEDGER_DIR/booking_strict.beancount" "$JDBC_DIR/booking_strict" "$JDBC_DIR/booking_strict.sqlite"
run_case "$LEDGER_DIR/display_context.beancount" "$JDBC_DIR/display_context" "$JDBC_DIR/display_context.sqlite"

if [[ $FAILED -eq 0 ]]; then
  echo "All $PASSED regression cases passed."
else
  echo "$FAILED regression cases failed." >&2
  exit 1
fi
