# Changelog

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
