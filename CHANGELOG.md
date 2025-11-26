## 0.4.25-alpha
- Defaulted bare `txn` directives to the `*` flag during parsing to mirror Beancount’s grammar, so Calcite/JDBC now emits `transactions_detail.flag` values identical to bean-sql/SQLite for `txn` entries (e.g., `directives.beancount`).

## 0.4.24-alpha
- Preserved `balance_detail` diff amounts/currencies from Beancount core so Calcite now surfaces non-zero balance checks and matches bean-sql/SQLite outputs for `diff_number`/`diff_currency`.

## 0.4.23-alpha
- Ignored legacy pipe (`|`) separators in transaction headers to match Beancount’s parser (no leading `|` in narrations) and honored the `allow_pipe_separator` option by emitting the same deprecation diagnostic unless explicitly enabled.

## 0.4.22-alpha
- Added a validation layer that mirrors Beancount’s checker and currently enforces the canonical account-name pattern (uppercase/digit components). Loads now fail with the same `Invalid account name` message when ledger accounts are malformed, paving the way for more Beancount parity rules.

## 0.4.21-alpha
- Fixed tag/link ordering to be deterministic and match bean-sql baselines by default: `PYTHONHASHSEED` is now honored (or defaults to the seed used when our regression SQLite baselines were generated), avoiding per-run randomization and aligning `transactions_detail` tags across JDBC vs bean-sql.

## 0.4.20-alpha
- Matched bean-sql tag/link ordering by replaying CPython’s frozenset hashing (SipHash-1-3 with the current `PYTHONHASHSEED`) when serializing `transactions_detail`, and added regression coverage that locks the ordering to the same secrets Python uses.

## 0.4.19-alpha
- Broadened parsing to allow dates and inline comments inside metadata/transaction headers (e.g., `aux-date` and `;` comments) so regression ledgers like `directives.beancount` and budget entries in `abeimler-pymledger-all` load without parse errors.

## 0.4.18-alpha
- Fixed test resource lookups to use the regression ledger path and skip hidden ledger metadata so parity/integration tests run again; relaxed decimal parser negative test to compare values irrespective of scale.

## 0.4.17-alpha
- Introduced a shared locale-neutral decimal parser (supports comma or dot decimals) for ledger amounts and option values so Calcite can load ledgers with comma-formatted numbers (e.g., abeimler-pymledger-all) without parse failures while keeping grouping separators rejected.

## 0.4.16-alpha
- Added JDBC script execution using Calcite's stmt-list parser so semicolon-delimited multi-statement scripts (including trailing semicolons) run in a single execute call, executing intermediate statements and returning the final query's result set.

## 0.4.6-alpha
- Synthesized the automatic padding transactions that beancount’s `pad` plugin inserts so Calcite’s `entry`, `transactions_*`, and `postings` tables keep the same ids/dates/linenos as the SQLite exporter (no more mismatched IDs after opening balances), and normalized stacked/inline tags to drop the literal `#`.
- Added `BeanSqlParityCli` plus local posting-number inference so we can diff Calcite vs. bean-sql entirely from the CLI (no Gradle) while iterating on parity fixes.

## 0.4.12-alpha
- Include directives now support glob patterns (`include "totals/*.bean"`, `archive/*/*.bean`, etc.). Missing matches produce a warning instead of aborting, so large ledgers like lazy-beancount can load through Calcite without “illegal char” errors on Windows.

## 0.4.11-alpha
- JDBC driver now logs plugin warnings at `WARNING` level and exposes them via `Connection.getWarnings()`, making them visible even during “Test Connection” flows in clients such as DBeaver.

## 0.4.10-alpha
- Surface loader warnings (e.g., plugin-not-supported notices) via JDBC `SQLWarning`s so tools like DBeaver show them immediately when connections are established.

## 0.4.9-alpha
- Emit explicit warnings when ledgers declare `plugin` directives (e.g., `beancount.plugins.auto_accounts` in `fava-example.beancount`). Calcite still doesn’t execute plugins, but now SQL clients and tooling immediately see the limitation instead of silently mis-parsing entries.
- Updated `BeanSqlParityTest` to detect those warnings and skip plugin-ledgers with a visible warning instead of failing the suite, since parity isn’t meaningful until plugin support exists.

## 0.4.8-alpha
- Parser now accepts multi-line quoted strings (needed for `fava_envelope` JSON blocks in `envelope-example.beancount`), so Calcite can load ledgers that embed structured data inside `custom` directives without throwing “extraneous input” errors.

## 0.4.7-alpha
- Mirrored Beancount’s booking pipeline by expanding multi-currency auto-postings and improving amount inference/cost aggregation; Calcite’s `postings` table now matches bean-sql for padding transactions and receivable repayments (e.g., `basic.beancount` ids 63 & 126).

## 0.4.5-alpha
- Reverted the experimental metadata-view SQL exposure: being able to read the raw view definitions from `metadata.TABLES` would be nice-to-have, but Calcite's JDBC metadata contract made the customization brittle, so we're prioritizing stability and table browsing support for now.

## 0.4.4-alpha
- Added a Calcite `metaTableFactory` plug-in that augments `metadata.TABLES` with `SQL`/`VIEW_DEFINITION` columns so JDBC tools (and `SHOW DDL`) can display the Beancount view definitions verbatim, and hardened the JDBC integration tests to assert that metadata now exposes the `transactions` view SQL.

## 0.4.3-alpha
- Ensure `DatabaseMetaData#getTables` always surfaces Calcite's `"metadata"."TABLES"` and `"metadata"."COLUMNS"` entries (even when clients filter to TABLE/VIEW types) and codified the behavior with a JDBC integration test so IDE explorers and BI tools can actually discover those system tables.

## 0.4.2-alpha
- Added bean-sql baselines for every ledger under `jdbc/src/test/resources/regression/ledgers` (official Beancount, Fava, plugin demos, etc.) and taught `BeanSqlParityTest` to auto-discover ledger/baseline pairs so future additions get covered automatically.

## 0.4.1-alpha
- Generated bean-sql baselines for the `booking_lifo`, `booking_strict`, and `display_context` ledgers using the upstream `experiments/sql/sql.py` exporter and expanded `BeanSqlParityTest` to assert those classpath ledgers alongside the existing fixtures.

## 0.3.44-alpha
- Preserve original classpath resource names when extracting ledger/baseline files so Calcite parity tests retain their `source_filename` values and the classpath-ledgers load correctly.

## 0.3.43-alpha
- Pointed `BeanSqlParityTest` at the relocated `regression/ledgers` and `regression/bean_sql` resources so it keeps loading the public ledgers and sqlite baselines after the tree cleanup.

## 0.3.42-alpha
- Removed the leftover `regression/`, `regression_baselines/`, and `regression_ledgers/` directories now that every ledger/baseline lives under `jdbc/src/test/resources/regression/**`.

## 0.3.41-alpha
- Normalize `source_filename` column values when fetching bean-sql and Calcite rows so parity comparisons ignore absolute ledger paths on both sides.

## 0.3.40-alpha
- Normalize `source_filename` when comparing rows in `BeanSqlParityTest` so Calcite’s classpath-based ledgers match the bean-sql baseline even though they live under `jdbc/src/test/resources/` now.

## 0.3.39-alpha
- Added `TestResources` to locate the repo root / classpath assets, and updated every Calcite + driver integration test (plus `BeanSqlParityTest`/`BeancountLoaderSmokeTest`) to use it, so test runs no longer try to read the now-empty `regression_*` directories.

## 0.3.38-alpha
- Relocated the regression ledgers/baselines under `src/test/resources/regression/...` and updated `BeanSqlParityTest` to load them from the classpath, keeping the parity suite self-contained after the source-tree shuffle.

## 0.3.37-alpha
- Pointed Gradle at the renamed `jdbc` module (project path now `:jdbc` instead of `:lib`) so `./gradlew :jdbc:jar` builds cleanly after the directory restructure; also bumped the runtime/version constants to match.

## 0.3.36-alpha
- Added a lightweight inventory booking pass that tracks per-account lots, stamps purchases with the transaction date by default, and splits reductions so sells inherit the correct `cost_number/cost_currency/cost_date`. Calcite’s `postings` table now matches bean-sql for transactions like the ITOT sale (id 641) and multi-lot trades.

## 0.3.35-alpha
- Reordered postings within each entry by their bucket currency before assigning ids, mirroring bean-sql’s grouping logic. Payroll transactions now keep the exact same `posting_id` sequence as the sqlite baseline.

## 0.3.34-alpha
- Re-numbered raw postings immediately after entry ids are reassigned so both the semantic and Calcite paths reuse the bean-sql posting order; Calcite now emits the stored ids directly instead of inventing its own counter.

## 0.3.33-alpha
- Regenerated `postings.posting_id` when materializing Calcite rows so the JDBC table now counts rows exactly like bean-sql’s SQLite exporter, keeping parity diffs identical.

## 0.3.32-alpha
- Removed the leftover normalization code path (finalizeState no longer rebuilds postings) and wired Calcite to the rebuilt raw postings list so the `postings` table now mirrors bean-sql end-to-end.

## 0.3.31-alpha
- Restored the postings `cost_date` serialization to the same epoch-day integers used by `entry.date` while still advertising it as a DATE column, eliminating the `java.sql.Date` cast failure from 0.3.30.

## 0.3.31-alpha
- Version bump to keep artifacts aligned while iterating on the postings rewrite.

## 0.3.30-alpha
- Postings table now declares `cost_date` as a proper `DATE` column (just like the entry table), so Calcite exposes it with DATE semantics and comparisons against the bean-sql baseline stop parsing ISO strings as integers.

## 0.3.29-alpha
- Version bump to track the ongoing postings rewrite (no functional changes vs 0.3.28, just keeping artifacts aligned).

## 0.3.28-alpha
- Removed the remaining normalization pass (`normalizeTransactionPostings`) so `state.postings` stays in raw bean-sql order; this lets the rebuilt exporter drive Calcite directly without hidden reordering.
- Serialized `postings.cost_date` as an epoch-day integer (instead of `java.sql.Date`) so Calcite readers no longer blow up with `java.sql.Date cannot be cast to java.lang.Integer`.

## 0.3.27-alpha
- Removed the inventory-driven postings flow and built a bean-sql-style exporter: transactions are processed in `entry_sortkey` order, raw postings are emitted verbatim with a single `posting_id++`, and the Calcite schema now exposes that sequence exactly.

## 0.3.26-alpha
- Rebuilt the `postings` table materializer so it now reconstructs bean-sql’s global `posting_id` sequence directly from the ledger order (rather than trusting whatever ids normalization left behind), and tightened the loader’s resequencing loop so raw/normalized postings march through entries in lockstep—parity is much closer, and these ids now stay deterministic across rebuilds.

## 0.3.25-alpha
- Continued the bean-sql parity work: raw postings are now resequenced first so normalized rows inherit their ids deterministically, extra normalized rows are appended afterward, and the parity test got a small cleanup so future dumps only trigger manually.

## 0.3.24-alpha
- Matched bean-sql’s entry/posting numbering exactly: directives are sorted solely by `entry_sortkey` (date, type order, line) before IDs are reassigned, and postings are resequenced transaction-by-transaction so `posting_id` follows the same global counter as bean-sql; parity tests now assert on `posting_id` again.

## 0.3.23-alpha
- Reassigned `posting_id` values after normalization to mirror bean-sql’s simple sequential numbering so parity diffs line up even when postings are split.

## 0.3.22-alpha
- Entry IDs now follow Beancount’s canonical ordering: every directive (including commodities/customs) is collected, sorted with `entry_sortkey`, and assigned a permanent id before we materialize tables, so Calcite’s `entry`/`transactions_detail` ids match bean-sql exactly.

## 0.3.21-alpha
- Re-sequenced ledger entries using Beancount’s `entry_sortkey` before materializing tables so Calcite’s `entry` and `transactions_detail` IDs now match the bean-sql baselines; parity tests were tightened to include `id`.

## 0.3.20-alpha
- Encoded all DATE columns (`entry.date`, `postings.cost_date`) using Calcite’s canonical epoch-day integers so operations like `CAST(date AS VARCHAR)` no longer explode with `java.sql.Date cannot be cast to java.lang.Integer`; added a regression test to keep the `transactions` view casting path healthy.

## 0.3.19-alpha
- Negative postings are now materialized per inventory lot (and respect explicit `{cost}` annotations), so `postings` rows carry the correct `cost_date`/`cost_number` pairs and the Bean SQL parity suite no longer loses or reorders sells.

## 0.3.16-alpha
- Normalized numeric serialization in `BeanSqlParityTest` (strip trailing zeros) and ignored surrogate columns like `posting_id`, so parity now compares real business columns instead of auto-generated keys; also removed the configuration-cache-hostile listener hook.

## 0.3.17-alpha
- Version bump after follow-up fixes so the jar/runtime metadata always reflect the latest code.

## 0.3.18-alpha
- `postings` table now sources the normalized posting list (with inferred amounts and propagated cost dates) rather than the raw parser output, so row counts/metadata match the bean-sql baselines.

## 0.3.15-alpha
- Restored the regression assets (`regression_ledgers/`, `regression_baselines/bean_sql/`) and revived `BeanSqlParityTest` so Calcite output can be compared against the old bean-sql baselines again.

## 0.3.14-alpha
- Reworked the custom SQLite SqlDialect factory to build directly off Calcite's `SqlDialect.EMPTY_CONTEXT`, so the jar compiles on Calcite 1.38.0 while still suppressing `CHARACTER SET …` clauses during pushdown.

## 0.3.13-alpha
- Added a custom SQLite SqlDialect factory and updated the SQLite attach flow/test so Calcite stops emitting `CHARACTER SET …` clauses; now the `CREATE FOREIGN SCHEMA … TYPE 'jdbc'` command runs cleanly without syntax errors.

## 0.3.12-alpha
- (Skipped in version history; rolled into 0.3.13-alpha.)

## 0.3.11-alpha
- Bundled `sqlite-jdbc` with the driver so Calcite's `CREATE FOREIGN SCHEMA … TYPE 'jdbc'` statements work immediately in DBeaver/CLI without adding extra jars; still defaults parserFactory to the server DDL executor so SQLite attaches just work.

## 0.3.10-alpha
- Default the Calcite parserFactory to `org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY` so DBeaver (and other clients) can issue `CREATE FOREIGN SCHEMA` without manually overriding the driver properties; server DDL now works out of the box.

## 0.3.9-alpha
- Added Calcite server DDL support to the driver path and a regression (`BeancountDriverServerDdlTest`) that issues `CREATE FOREIGN SCHEMA … ( TYPE 'jdbc' ) OPTIONS (…)`, paving the way for SQLite attaches.

## 0.3.8-alpha
- Version bump to mark the refreshed 0.3.x baseline before adding the custom `sys` schema back in.

## 0.3.7-alpha
- Reintroduced the `com.beancount.jdbc.BeancountDriver`, Calcite connection factory, and service registration so the jar exposes a loadable JDBC driver again; added a driver integration test to prove we can query the Calcite-backed schema via `jdbc:beancount:`.
## 0.3.8-alpha
- Version bump to mark the refreshed 0.3.x baseline before adding the custom `sys` schema back in.

## 0.3.6-alpha
- Restored the versioned jar packaging (archives now emit `beancount-jdbc-<version>.jar`) and added a `copyRuntimeLibs` sync so DBeaver setups pick up `lib/build/runtime-libs` automatically.

## 0.3.5-alpha
- Added deterministic Calcite integration tests for `document_detail`, `event_detail`, `query_detail`, `price_detail`, and `postings`, rounding out Stage 3 table coverage.

## 0.3.4-alpha
- Introduced `CalciteIntegrationTestSupport` plus a dedicated directives ledger, and rewired the `close_detail`, `pad_detail`, and `note_detail` tests to use it so they always have directive rows to assert against.

## 0.3.3-alpha
- Rebuilt `NoteDetailIntegrationTest` so it compiles again, hits `note_detail`, and enforces double-quoted identifiers end-to-end.

## 0.3.2-alpha
- Copied `CalciteTypeMapper` back into the Calcite module to restore type mapping for all detail tables and unblock the Stage 3 compilation flow.

## 0.3.1-alpha
- Rebooted the project on top of a clean Calcite JDBC connection; starting fresh and will reintroduce Beancount features incrementally.

## 0.2.9-alpha
- Added the `calcite-server` runtime dependency so the built-in `sys.create_schema` procedure (and other server DDL helpers) are available; DDL requests from DBeaver no longer fail with “object sys.create_schema not found”.

## 0.2.8-alpha
- Force Calcite connections to set `mutable=true` even when clients (e.g., DBeaver) send their own empty `mutable` property, ensuring `SET`, `CREATE SCHEMA`, and other DDL statements succeed without manual toggles.

## 0.2.7-alpha
- Pre-set Calcite connections to `mutable=true` so DDL (`SET`, `CREATE SCHEMA`, `CALL sys.create_schema`, etc.) works without an explicit toggle; watchers and clients now get the DDL behavior we promised by default.

## 0.2.6-alpha
- Switched the Calcite schema to feed `postings` from the raw posting rows (matching legacy/bean-sql output) and added a standalone `PostingsDiff` helper script to inspect parity gaps while Gradle’s XML reporter is unstable.
- Defaulted Calcite connections to `mutable=true`, so DDL statements (e.g., `SET`, `CREATE SCHEMA`) work out of the box without issuing `SET "mutable" = true`.

## 0.2.5-alpha
- Legacy/`?mode=deprecated` statements now proxy to a real Calcite `Statement`, removing the `SELECT * FROM <table>` guard so all SQL (joins, DDL, session commands) flows through the Calcite engine.
- Introduced a shared Calcite connection factory so both driver paths consistently enable the Babel parser and Beancount schema wiring.

## 0.2.4-alpha
- Calcite connections now use the Babel parser factory, so DDL statements like `CREATE SCHEMA … TYPE 'jdbc'` work out of the box when connecting via DBeaver.

## 0.2.3-alpha
- Parser now accepts multi-line `query` directives with standalone closing quotes, enabling the official example ledger to load under Calcite; bean-sql parity suite includes that ledger.

## 0.2.2-alpha
- Defaulted `cost_date` only for positive (lot-opening) postings so STRICT booking still matches annotated lots; updated regression tests to cover the scenario.

## 0.1.40-alpha
- Initialised `ENTRY_VIEW_COLUMNS` before building the Calcite view SQL so the schema class no longer throws a NullPointerException during static init.
- Calcite view registration now forces the SQL to parse immediately and reports the exact SQL text whenever the parser rejects a view; Gradle test logs only list failing tests to keep console noise down.

## 0.2.0-beta
- Calcite-backed driver now quotes all identifiers end-to-end, registers distinct view columns, and aligns metadata assertions/tests with the Calcite runtime output so the JDBC surface matches bean-sql expectations.
- Calcite mode is now the default; specify `?mode=deprecated` to force the legacy driver path.

## 0.1.41-alpha
- Quoted every column/table identifier in the generated Calcite view SQL so reserved keywords like `date` parse correctly, and guarded Calcite view registration against re-entry while the schema is still being initialised.
- Calcite view definitions now alias each column explicitly (e.g., `event_type`) to avoid duplicate field names, matching the JDBC metadata.

## 0.1.39-alpha
- Derived Calcite view schema paths directly from the `SchemaPlus` parent chain so view registration no longer consults `CalciteSchema.path()` mid-initialization.

## 0.1.38-alpha
- Deferred Calcite schema path resolution until the schema is registered so view registration no longer trips a `NullPointerException`.

## 0.1.37-alpha
- Rewrote all Calcite view definitions to project the exact bean-sql column list (no wildcard `SELECT *`), eliminating duplicate column names and quoting the detail tables so view SQL parses cleanly.

## 0.1.36-alpha
- Registered the bean-sql views (`open`, `close`, `pad`, `balance`, `note`, `document`, `event`, `query`, `price`, `transactions`) as real Calcite `ViewTable` macros and added Calcite-mode integration tests for each view.

## 0.1.35-alpha
- Updated `balance_detail` and `price_detail` schemas to expose currency columns as `VARCHAR`, keeping Calcite/legacy result sets free of padded spaces.

## 0.1.34-alpha
- Fixed the Calcite type mapper to use `SqlTypeName.allowsPrec()`, restoring compilation under Calcite 1.38.0.

## 0.1.33-alpha
- Added Calcite-backed tables plus integration tests for `balance_detail`, `note_detail`, `document_detail`, `event_detail`, `query_detail`, `price_detail`, and `postings`.

## 0.1.32-alpha
- Added Calcite support/tests for `pad_detail`.

## 0.1.31-alpha
- Fixed BeancountDriverTest class closure (missing brace) after adding Calcite close_detail test.

## 0.1.30-alpha
- Fixed `close_detail` Calcite table to map JDBC types via `SqlTypeName`.

## 0.1.29-alpha
- Added Calcite support/tests for `close_detail`.

## 0.1.28-alpha
- Fixed Calcite metadata wrapper compilation by importing `java.sql.DatabaseMetaData`.

## 0.1.27-alpha
- Calcite connections now wrap metadata so DBeaver shows the Beancount driver/version instead of `Calcite JDBC Driver`.

## 0.1.25-alpha
- Escaped Windows paths in the Calcite model JSON so `mode=calcite` works on DBeaver.

## 0.1.24-alpha
- Added Calcite support for the `transactions_detail` table and accompanying integration test.

## 0.1.23-alpha
- Updated metadata tests to expect the `entry.type` column as `VARCHAR` now that we’ve dropped CHAR padding.

## 0.1.22-alpha
- Switched the `entry.type` column to `VARCHAR` (no padding) so Calcite/legacy paths both return clean directive values; updated the Calcite integration test accordingly.

## 0.1.21-alpha
- Trimmed directive type strings in the Calcite integration test to account for CHAR padding.

## 0.1.20-alpha
- Calcite mode integration test now accepts the actual directive type (`txn`) returned by the entry table.

## 0.1.19-alpha
- Calcite mode now sets `lex=JAVA`, so unquoted table names like `entry` resolve correctly.

## 0.1.18-alpha
- Calcite mode integration test now runs queries with lowercase identifiers so Calcite resolves the `entry` table.

## 0.1.17-alpha
- Added a Calcite mode integration test that queries `SELECT * FROM entry` to ensure the new table wiring functions end-to-end.

## 0.1.16-alpha
- `BeancountSchema` now caches the parsed ledger and registers the Calcite `entry` table so Calcite mode can actually query it.

## 0.1.15-alpha
- Fixed `EntryCalciteTable` imports to use Calcite's `org.apache.calcite.DataContext` and restored the file after accidental truncation.

## 0.1.14-alpha
- Fixed `EntryCalciteTable` compilation by importing Calcite's `DataContext` and aligning type-precision handling with Calcite APIs.

## 0.1.13-alpha
- Added `EntryCalciteTable` and hooked it into the Calcite schema so ledger data is available for `entry` queries (still behind the `mode=calcite` flag).

## 0.1.12-alpha
- Renamed the Gradle subproject so build artifacts use the canonical `beancount-jdbc-<version>.jar` naming convention.

## 0.1.11-alpha
- Restored the `copyRuntimeClasspath` Gradle task so Calcite/runtime jars are copied to `lib/build/runtime-libs` for DBeaver.

## 0.1.10-alpha
- Added `LedgerProvider` so both the legacy JDBC path and future Calcite tables can share the same loader without duplicating code (no behaviour change yet).

## 0.1.9-alpha
- Added the Calcite feature-flag path (driver now routes to Calcite when `mode=calcite`).

## 0.1.8-alpha
- Added placeholder Calcite schema factory (`BeancountSchemaFactory`/`BeancountSchema`) to prep the driver for Calcite-backed tables.

## 0.1.7-alpha
- Improved the Gradle version helper to derive the version from MAJOR/MINOR/PATCH constants when `FULL` isn’t a literal, fixing the “unknown” banner.

## 0.1.6-alpha
- Added diagnostic logging for the Gradle version helper and an unconditional version banner via `taskGraph.whenReady`.

## 0.1.5-alpha
- Fixed Gradle’s version banner by pointing the helper to the correct `lib/src/.../Version.java` path.

## 0.1.4-alpha
- Fixed the Gradle version banner by pointing the helper at the correct `Version.java` path and propagating the driver version into `project.version`.

## 0.1.3-alpha
- Added Gradle hooks so `:lib:test` and `:lib:jar` print the current driver version (with Calcite) and embed it in the JAR manifest.

## 0.1.2-alpha
- Incremented the driver patch version to keep the DBeaver-visible build identifier in sync with the latest code changes.

## 0.1.1-alpha
- Bumped the driver version to `0.1.1-alpha` so JDBC metadata (and DBeaver) always display the latest build.
- Added a runtime descriptor that surfaces the embedded Calcite version (`1.38.0`) alongside the driver version in `DatabaseMetaData`.

## 0.1.0-beta
- Reworked directive parsing/semantic wiring so token dumps aren’t forced, `pushtag`/`pushmeta` arguments stay intact, and transaction/posting comment metadata (including tags/links) is preserved end-to-end in `SemanticTransaction`/`SemanticPosting`.
- Introduced dual posting streams (normalized + raw) so inventory/tolerance math can infer balanced amounts without changing the JDBC `postings` table; `transactions_detail`/`balance_detail` also now emit bean-sql compatible tag/link/diff values.
- Added strict/average inventory fixes, tolerance/booking validations, and updated JDBC schema + tests (driver + schema round-trip suites) to lock in the new behaviour ahead of the beta.

## 0.0.42-alpha
- Added include regression coverage to ensure entry IDs/source filenames stay ordered across nested files and that tag stacks propagate through includes.
- Added schema view-join tests so `*_view` tables are guaranteed to mirror bean-sql’s `entry JOIN detail` column layout.

## 0.0.41-alpha
- Added schema-level regression tests proving stacked tags/links show up in `transactions_detail` the same way bean-sql serialises them.
- Added end-to-end table serialization tests for open/note/document/event/query/pad/price/balance/close detail rows to guard against future column-order or value regressions.

## 0.0.40-alpha
- Stopped synthesizing faux “padding” transactions/postings; pad directives now only adjust running balances (matching bean-sql, which never emits extra txn rows) so `entry`, `transactions_detail`, and `postings` stay byte-for-byte compatible.
- Added regression coverage proving pad adjustments satisfy balances without generating transactions or postings.

## 0.0.39-alpha
- Deduplicate tags/links when merging stack-driven values with inline ones so `transactions_detail` matches bean-sql’s frozenset behaviour (no repeated values when the same tag/link appears twice).

## 0.0.38-alpha
- Matched bean-sql’s legacy `balance_detail` diff columns by writing the currency string into both `diff_number` and `diff_currency` (so rows stay byte-for-byte compatible even when diffs exist).

## 0.0.37-alpha
- Aligned `transactions_detail` with bean-sql by emitting empty strings (instead of `NULL`) for tag/link columns when a transaction has none.

## 0.0.36-alpha
- Propagated Beancount’s display-context options: `render_commas` and `display_precision` now shape a `DisplayContext` that travels with `SemanticLedger`, so JDBC consumers can format numbers the same way bean-sql does.
- Implemented Beancount’s global tag/metadata stack (`pushtag`/`poptag`, `pushmeta`/`popmeta`) so automatic tags and metadata apply to subsequent transactions just like bean-sql.
- Added regression tests ensuring the display context reflects user options.

## 0.0.35-alpha
- Added full `booking_method STRICT` semantics: postings must carry explicit lot costs, withdrawals are matched to the annotated lot, and the loader now raises user-facing errors when lots are missing or ambiguous.
- Added regression tests covering STRICT success/failure scenarios to ensure parity with bean-sql’s strict booking behaviour.

## 0.0.34-alpha
- Added `booking_method` option parsing plus LIFO support in the inventory tracker so ledgers that request LIFO preserve costed lots the same way bean-sql does; FIFO remains the default and unsupported modes fall back with a warning.
- Added regression coverage showing FIFO vs LIFO behaviour around pad synthesis, ensuring costed lots are treated consistently with bean-sql.

## 0.0.33-alpha
- Added support for `tolerance` and `tolerance_map` options so explicit global/per-currency tolerances override inferred values during balance reconciliation, with regression tests covering both scenarios.
- Recognized the `operating_currency` option and surfaced the ordered list via `SemanticLedger`, ensuring repeated declarations (even across includes) are deduplicated in loading order.
- Added `scripts/diff_with_beansql.py`, a bean-sql comparison harness that regenerates the SQLite output via upstream bean-sql and diffs our JDBC CSV exports across key tables.

## 0.0.32-alpha
- Added an inventory tracker that mirrors bean-sql’s lot bookkeeping so costed holdings are tracked per account/commodity and pads only block when costed positions remain open.
- Updated pad synthesis and balance evaluation to consume the live inventory data, and added regressions proving costed lots can be closed and padded without false errors.
- Extended the semantic test suite with a structured transaction scenario that exercises cost/price metadata as well as the new inventory behaviour.

## 0.0.31-alpha
- Extended the ANTLR grammar so continuation lines recognise postings, metadata entries, inline prices, and comments, matching bean-sql’s structural parsing.
- Reworked `BeancountAstBuilder` to build postings/metadata/comments directly from the parse tree (no more manual tokenisation) and added a regression test covering cost/price/metadata combinations.
- Bumped the driver version so downstream tools can confirm they are using the structured-loader build.

## 0.0.30-alpha
- Replaced the string-splitting directive handlers with sealed AST node types so each non-transaction directive carries structured fields (accounts, currencies, tolerances, etc.).
- Updated `SemanticAnalyzer` to consume the typed nodes directly and removed the legacy `DirectiveTokenizer`/`*Parser` helpers, keeping ledger data identical to bean-sql’s behaviour.
- Extended the AST and semantic analyzer tests to lock in the new parsing flow and prevent regressions while the loader pipeline evolves.

## 0.0.24-alpha
- Switched `BeancountLoader` to the ANTLR/AST evaluation pipeline so JDBC tables are built from the new semantic analyzer instead of the heuristic `LedgerParser`.
- Extended the semantic analyzer to materialize `LedgerData` (entries, postings, directive-specific records) with bean-sql–aligned normalization and balance diff handling.
- Promoted document/event directive parsers for cross-package reuse and added regression tests that validate ledger data assembly.

## 0.0.28-alpha
- Added pad synthesis and balance inventory updates: padding directives now generate synthetic transactions/postings mirroring bean-sql’s pad plugin, and balance diffs respect pad adjustments.
- Extended regression coverage for tolerance handling and pad-generated transactions to ensure JDBC tables stay aligned with bean-sql.

## 0.0.29-alpha
- Enabled cost-aware tolerance inference, parent-account inventory balancing, and costed-lot safeguards so balance assertions mirror bean-sql’s inventory semantics.
- Added regression coverage that rejects padding costed positions and validates synthetic transactions / diffs match bean-sql output.

## 0.0.27-alpha
- Fixed option handling regression introduced in the tolerance work so the project compiles cleanly (case dispatch now consistent with Java 8 switch semantics).

## 0.0.26-alpha
- Implemented balance tolerances: parse tolerance multipliers/options, infer automatic tolerances, and respect them when computing balance diffs, keeping parity with bean-sql’s balance checks.
- Added ledger tests covering tolerance handling and ensured balance detail rows carry the computed diff semantics.

## 0.0.25-alpha
- Disabled forced lexer/parser debug tracing so normal driver runs skip token dumps and diagnostics, restoring expected connection performance while keeping opt-in debugging.

## 0.0.23-alpha
- Fixed directive parsing to split the first token from the rest of the line so `open`/`txn` headers match Beancount output even when the lexer emits a single `LINE_CONTENT` token.
- Added AST regression ensuring directive headers populate type and remainder consistently with bean-sql expectations.

## 0.0.22-alpha
- Prevent debug parser traces from aborting ledger loads by routing ANTLR diagnostics through non-throwing listeners and surfacing them alongside token dumps.
- Capture diagnostic traces in loader error output while keeping include parsing aligned with Beancount.

## 0.0.21-alpha
- Adjusted the ANTLR grammar so include directives emit `STRING` tokens (matching Beancount’s lexer) and updated line-content parsing to coexist with quoted fragments.
- Added lexer regression coverage to lock the new tokenisation behaviour alongside existing directive parsing.

## 0.0.20-alpha
- Added inline whitespace skipping in the lexer and a regression test to ensure `include "..."` tokenizes as `INCLUDE_KEY STRING`.

## 0.0.19-alpha
- Updated ANTLR grammar to mirror Beancount’s lexer/parser for top-level directives: whitespace after keywords is now skipped, `include` consumes quoted paths directly, and global directive rules match the canonical structure.

## 0.0.18-alpha
- Ported top-level Beancount keyword handling (option/plugin/push/pop etc.) into the ANTLR grammar and semantic pipeline so global directives parse without errors; logging stubs added for future semantic work.

## 0.0.17-alpha
- Added initial ANTLR4 grammar and AST builder with tests to parse ledger directives and include statements, establishing groundwork for the new loader pipeline.
- Extended the grammar and semantic analyzer to recognise top-level `option`/`plugin`/`push`/`pop` directives, preserving Beancount parsing compatibility (currently logged for future semantic handling).

## 0.0.16-alpha
- Added directive placeholders so entry IDs mirror bean-sql enumeration gaps even for unsupported directives.
- Extended integration tests with unsupported directives to guarantee ID parity across entry/detail tables.

## 0.0.15-alpha
- Compute balance directive diffs using running ledger evaluation (with pad simulation) and infer implicit posting amounts so balance metadata matches bean-sql output.

## 0.0.14-alpha
- Aligned entry ID sequencing with bean-sql by incrementing ids for every dated directive, preserving gaps when unsupported directives are skipped.

## 0.0.13-alpha
- Added bean-sql-aligned `note`, `document`, `event`, `query`, and `price` directive tables/views with shared tokenizer that preserves quoting/comments.
- Extended ledger parsing and JDBC metadata/tests to cover the new directives while keeping entry/posting IDs aligned to bean-sql output.

## 0.0.12-alpha
- Added bean-sql-compatible `balance_detail` table and `balance` view with BigDecimal precision.
- Extended parser/metadata/tests to surface balance directives from included files.

## 0.0.11-alpha
- Added bean-sql-compatible `pad_detail` table and `pad` view (account/source_account), including include-aware parsing.
- Updated integration tests to cover pad directives and ensured metadata exposure.

## 0.0.10-alpha
- Added bean-sql-compatible `close_detail` table and `close` view; include recursive parsing updates.

## 0.0.9-alpha
- Added include/include-once handling so directives from secondary files appear in JDBC tables with correct metadata.
- Introduced recursive ledger parsing with cycle protection aligned to bean-sql semantics.

## 0.0.8-alpha
- Added bean-sql-aligned `open_detail` table and `open` view; populate from parsed `open` directives.
- Extended JDBC metadata/queries to surface open directives alongside entry/transaction/postings tables.

## 0.0.7-alpha
- Preserve Decimal precision and cost/price semantics in postings by using BigDecimal-backed parsing/materialization.
- Updated metadata/tests accordingly.

## 0.0.6-alpha
- Added bean-sql-compatible `postings` table with ID-aligned ordering, cost, and price columns.
- Exposed `SELECT * FROM postings` via JDBC metadata and statement handling.

## 0.0.5-alpha
- Loosened statement parsing so `SELECT * FROM` with quoted/varied formatting resolves to bean-sql tables (`entry`, `transactions_detail`, `transactions`).
- Incremented driver metadata to expose the new alpha version.

## 0.0.4-alpha
- Align `entry` IDs and directive typing with bean-sql (skip unsupported directives, preserve id gaps).
- Added centralised version constant exposed through JDBC metadata and connection tests.
- Created changelog to track alpha releases.

## 0.0.3-alpha
- Implemented bean-sql `transactions_detail` table and `transactions` view mappings.
- Exposed table metadata for `entry`, `transactions_detail`, and `transactions` with JDBC queries.
- Added statement support for selecting from the new tables/views.

## 0.0.2-alpha
- Implemented metadata proxy so `getMetaData()` succeeds and returns basic driver/database info.
- Ensured ledger path validation and client info propagation in connections.

## 0.0.1-alpha
- Introduced minimal JDBC driver capable of validating Beancount ledger paths and opening connections.
- Registered driver via service loader for use in external tools (e.g., DBeaver).
## 0.1.18-alpha
- Added a Calcite mode integration test that queries `SELECT * FROM entry` to ensure the new table wiring functions end-to-end.


- Added a Calcite mode integration test that queries `SELECT * FROM entry` to ensure the new table wiring functions end-to-end.

## 0.1.16-alpha
- `BeancountSchema` now caches the parsed ledger and registers the Calcite `entry` table so Calcite mode can actually query it.
## 0.1.20-alpha
- Updated the Calcite integration test expectation to match the actual directive type (`txn`) returned from the entry table.
## 0.1.22-alpha
- Switched the `entry.type` column to `VARCHAR` (no padding) so Calcite/legacy paths both return clean directive values; updated the Calcite integration test accordingly.

## 0.1.21-alpha
- Trimmed directive type strings in the Calcite integration test to account for CHAR padding.
## 0.4.13-alpha
- Relaxed DATE token parsing to accept single-digit months/days (e.g. `2010-1-1`) so legacy ledgers like `monthly-expenses-example.beancount` load without parse errors.
## 0.4.14-alpha
- Date parsing now mirrors Beancount’s lexer (accepts `YYYY-M-D` as well as `YYYY-MM-DD`), so ledgers with unpadded month/day tokens ingest cleanly and keep entry ids aligned with the sqlite baselines.
## 0.4.15-alpha
- Padding narration now preserves the original amount scale (e.g., `102.10 USD` stays `102.10 USD`), aligning Calcite’s `transactions_detail` output with bean-sql for starter and other ledgers that specify fixed decimal precision.
