# Bean-sql Regression Suite

Use `scripts/diff_with_beansql.py` to compare JDBC CSV exports with bean-sql's SQLite output for a single ledger, or `scripts/run_regression_suite.sh` to run the curated set (multi-commodity, booking modes, strict errors, display context).

1. Export each JDBC table for the ledger into `exports/<case>/<table>.csv`.
2. Run `python3 scripts/diff_with_beansql.py regression_ledgers/<case>.beancount exports/<case> exports/<case>.sqlite`.
3. For the full suite: `bash scripts/run_regression_suite.sh regression_ledgers exports`.

Failures indicate mismatches between the JDBC driver and bean-sql for that ledger; investigate the reported table/row differences.
