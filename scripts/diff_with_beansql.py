#!/usr/bin/env python3
"""Compare JDBC output with bean-sql for regression accuracy."""

import argparse
import os
import sqlite3
import subprocess
import sys
from collections import OrderedDict
from pathlib import Path

REQUIRED_TABLES = [
    "entry",
    "transactions_detail",
    "transactions",
    "postings",
    "open_detail",
    "close_detail",
    "pad_detail",
    "balance_detail",
]


def run(cmd, cwd=None):
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command {' '.join(cmd)} failed with code {result.returncode}:\n{result.stderr}"
        )
    return result.stdout


def build_bean_sql_db(ledger_path: Path, output: Path):
    script = Path(__file__).resolve().parents[1] / "third_party" / "beancount" / "experiments" / "sql" / "sql.py"
    cmd = [
        sys.executable,
        str(script),
        str(ledger_path),
        str(output),
    ]
    run(cmd)


def fetch_table_rows(db_path: Path, table: str):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
    ordered = [OrderedDict(row) for row in rows]
    return ordered


def fetch_jdbc_rows(jdbc_dir: Path, table: str):
    db_path = jdbc_dir / f"{table}.csv"
    if not db_path.exists():
        raise FileNotFoundError(f"Missing JDBC export for {table} at {db_path}")
    rows = []
    with db_path.open() as handle:
        headers = handle.readline().rstrip().split(",")
        for line in handle:
            values = line.rstrip().split(",")
            rows.append(OrderedDict(zip(headers, values)))
    return rows


def diff_tables(bean_rows, jdbc_rows, table):
    if len(bean_rows) != len(jdbc_rows):
        return f"{table}: row count mismatch bean={len(bean_rows)} jdbc={len(jdbc_rows)}"
    for idx, (bean_row, jdbc_row) in enumerate(zip(bean_rows, jdbc_rows), start=1):
        if list(bean_row.keys()) != list(jdbc_row.keys()):
            return f"{table}: column order mismatch at row {idx}"
        for key in bean_row.keys():
            bean_value = bean_row[key]
            jdbc_value = jdbc_row[key]
            if str(bean_value) != str(jdbc_value):
                return (
                    f"{table}: value mismatch row {idx} col {key}:"
                    f" bean={bean_value!r} jdbc={jdbc_value!r}"
                )
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ledger", type=Path, help="Path to ledger file")
    parser.add_argument("jdbc_export", type=Path, help="Directory with JDBC CSV exports")
    parser.add_argument("bean_sql_db", type=Path, help="SQLite file for bean-sql output")
    args = parser.parse_args()

    build_bean_sql_db(args.ledger, args.bean_sql_db)

    failures = []
    for table in REQUIRED_TABLES:
        try:
            bean_rows = fetch_table_rows(args.bean_sql_db, table)
            jdbc_rows = fetch_jdbc_rows(args.jdbc_export, table)
        except Exception as exc:
            failures.append(f"{table}: {exc}")
            continue
        diff = diff_tables(bean_rows, jdbc_rows, table)
        if diff:
            failures.append(diff)

    if failures:
        print("Mismatch detected:")
        for failure in failures:
            print(f" - {failure}")
        sys.exit(1)

    print("bean-sql and JDBC outputs match for all tables")


if __name__ == "__main__":
    main()
