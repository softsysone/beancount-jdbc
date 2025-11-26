-- Mount the SQLite database created with bean-sql 
CREATE FOREIGN SCHEMA sqlite
  TYPE 'jdbc'
  OPTIONS (
    jdbcDriver 'org.sqlite.JDBC',
    jdbcUrl    'jdbc:sqlite:C:/Users/jacob-walker/Documents/GitHub/beancount-jdbc/jdbc/src/test/resources/regression/bean_sql/${datasource}.sqlite',
    sqlDialectFactory 'com.beancount.jdbc.calcite.sqlite.BeancountSqliteDialectFactory'
  );

-- Compare Table Row Counts
WITH
"sqlite_balance"             AS (SELECT 'balance'              AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."balance"),
"beancount_balance"          AS (SELECT 'balance'              AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."balance"),
"sqlite_balance_detail"      AS (SELECT 'balance_detail'       AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."balance_detail"),
"beancount_balance_detail"   AS (SELECT 'balance_detail'       AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."balance_detail"),
--
"sqlite_close"               AS (SELECT 'close'                AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."close"),
"beancount_close"            AS (SELECT 'close'                AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."close"),
"sqlite_close_detail"        AS (SELECT 'close_detail'         AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."close_detail"),
"beancount_close_detail"     AS (SELECT 'close_detail'         AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."close_detail"),
--
"sqlite_document"            AS (SELECT 'document'             AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."document"),
"beancount_document"         AS (SELECT 'document'             AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."document"),
"sqlite_document_detail"     AS (SELECT 'document_detail'      AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."document_detail"),
"beancount_document_detail"  AS (SELECT 'document_detail'      AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."document_detail"),
--
"sqlite_entry"               AS (SELECT 'entry'                AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."entry"),
"beancount_entry"            AS (SELECT 'entry'                AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."entry"),
--
"beancount_event"			 AS (SELECT 'event'                AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."event"),
"beancount_event_detail"	 AS (SELECT 'event_detail'         AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."event_detail"),
--
"sqlite_note"                AS (SELECT 'note'                 AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."note"),
"beancount_note"             AS (SELECT 'note'                 AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."note"),
"sqlite_note_detail"         AS (SELECT 'note_detail'          AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."note_detail"),
"beancount_note_detail"      AS (SELECT 'note_detail'          AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."note_detail"),
--
"sqlite_open"                AS (SELECT 'open'                 AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."open"),
"beancount_open"             AS (SELECT 'open'                 AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."open"),
"sqlite_open_detail"         AS (SELECT 'open_detail'          AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."open_detail"),
"beancount_open_detail"      AS (SELECT 'open_detail'          AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."open_detail"),
--
"sqlite_pad"                 AS (SELECT 'pad'                  AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."pad"),
"beancount_pad"              AS (SELECT 'pad'                  AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."pad"),
"sqlite_pad_detail"          AS (SELECT 'pad_detail'           AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."pad_detail"),
"beancount_pad_detail"       AS (SELECT 'pad_detail'           AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."pad_detail"),
--
"sqlite_postings"            AS (SELECT 'postings'             AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."postings"),
"beancount_postings"         AS (SELECT 'postings'             AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."postings"),
--
"sqlite_price"               AS (SELECT 'price'                AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."price"),
"beancount_price"            AS (SELECT 'price'                AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."price"),
"sqlite_price_detail"        AS (SELECT 'price_detail'         AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."price_detail"),
"beancount_price_detail"     AS (SELECT 'price_detail'         AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."price_detail"),
--
"beancount_query"			 AS (SELECT 'query'                AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."query"),
"beancount_query_detail"	 AS (SELECT 'query_detail'         AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."query_detail"),
--
"sqlite_transactions"        AS (SELECT 'transactions'         AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."transactions"),
"beancount_transactions"     AS (SELECT 'transactions'         AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."transactions"),
"sqlite_transactions_detail" AS (SELECT 'transactions_detail'  AS "tbl", COUNT(*) AS "sqlite_total" FROM "sqlite"."transactions_detail"),
"beancount_transactions_detail" AS (SELECT 'transactions_detail' AS "tbl", COUNT(*) AS "beancount_total" FROM "beancount"."transactions_detail")
--
SELECT "sqlite_balance"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_balance" JOIN "beancount_balance" ON "sqlite_balance"."tbl" = "beancount_balance"."tbl"
UNION ALL SELECT "sqlite_balance_detail"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_balance_detail" JOIN "beancount_balance_detail" ON "sqlite_balance_detail"."tbl" = "beancount_balance_detail"."tbl"
UNION ALL SELECT "sqlite_close"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_close" JOIN "beancount_close" ON "sqlite_close"."tbl" = "beancount_close"."tbl"
UNION ALL SELECT "sqlite_close_detail"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_close_detail" JOIN "beancount_close_detail" ON "sqlite_close_detail"."tbl" = "beancount_close_detail"."tbl"
UNION ALL SELECT "sqlite_document"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_document" JOIN "beancount_document" ON "sqlite_document"."tbl" = "beancount_document"."tbl"
UNION ALL SELECT "sqlite_document_detail"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_document_detail" JOIN "beancount_document_detail" ON "sqlite_document_detail"."tbl" = "beancount_document_detail"."tbl"
UNION ALL SELECT "sqlite_entry"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_entry" JOIN "beancount_entry" ON "sqlite_entry"."tbl" = "beancount_entry"."tbl"
UNION ALL SELECT "sqlite_note"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_note" JOIN "beancount_note" ON "sqlite_note"."tbl" = "beancount_note"."tbl"
UNION ALL SELECT "sqlite_note_detail"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_note_detail" JOIN "beancount_note_detail" ON "sqlite_note_detail"."tbl" = "beancount_note_detail"."tbl"
UNION ALL SELECT "sqlite_open"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_open" JOIN "beancount_open" ON "sqlite_open"."tbl" = "beancount_open"."tbl"
UNION ALL SELECT "sqlite_open_detail"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_open_detail" JOIN "beancount_open_detail" ON "sqlite_open_detail"."tbl" = "beancount_open_detail"."tbl"
UNION ALL SELECT "sqlite_pad"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_pad" JOIN "beancount_pad" ON "sqlite_pad"."tbl" = "beancount_pad"."tbl"
UNION ALL SELECT "sqlite_pad_detail"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_pad_detail" JOIN "beancount_pad_detail" ON "sqlite_pad_detail"."tbl" = "beancount_pad_detail"."tbl"
UNION ALL SELECT "sqlite_postings"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_postings" JOIN "beancount_postings" ON "sqlite_postings"."tbl" = "beancount_postings"."tbl"
UNION ALL SELECT "sqlite_price"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_price" JOIN "beancount_price" ON "sqlite_price"."tbl" = "beancount_price"."tbl"
UNION ALL SELECT "sqlite_price_detail"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_price_detail" JOIN "beancount_price_detail" ON "sqlite_price_detail"."tbl" = "beancount_price_detail"."tbl"
UNION ALL SELECT "sqlite_transactions"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_transactions" JOIN "beancount_transactions" ON "sqlite_transactions"."tbl" = "beancount_transactions"."tbl"
UNION ALL SELECT "sqlite_transactions_detail"."tbl", "sqlite_total", "beancount_total" FROM "sqlite_transactions_detail" JOIN "beancount_transactions_detail" ON "sqlite_transactions_detail"."tbl" = "beancount_transactions_detail"."tbl";
