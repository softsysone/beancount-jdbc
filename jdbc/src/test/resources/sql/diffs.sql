/*************************************************************
 * Differences between jdbc and bean-sql sqlite tables/views *
 * - Run as a whole for counts                               *
 * - Run individual CTEs for details                         *
 *************************************************************/

CREATE FOREIGN SCHEMA sqlite
  TYPE 'jdbc'
  OPTIONS (
    jdbcDriver 'org.sqlite.JDBC',
    jdbcUrl    'jdbc:sqlite:${workspace}/../../jdbc/src/test/resources/regression/bean_sql/${datasource}.sqlite',
    sqlDialectFactory 'com.beancount.jdbc.calcite.sqlite.BeancountSqliteDialectFactory'
  );

WITH entry_diffs AS (

	WITH b AS (
	  SELECT
	    "id",
	    "date",
	    "type",
	--    "source_filename",
	    "source_lineno"
	  FROM "beancount"."entry"
	),
	s AS (
	  SELECT
	    "id",
	    "date",
	    "type",
	--    "source_filename",
	    "source_lineno"
	  FROM "sqlite"."entry"
	)
	SELECT
	  COALESCE(b."id", s."id")                    AS "id",
	  b."date"            AS "bc_date",           s."date"            AS "sqlite_date",
	  b."type"            AS "bc_type",           s."type"            AS "sqlite_type",
	--  b."source_filename" AS "bc_source_filename",s."source_filename" AS "sqlite_source_filename",
	  b."source_lineno"   AS "bc_source_lineno",  s."source_lineno"   AS "sqlite_source_lineno"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."type" not in ('event','query') and
		( b."date"            IS DISTINCT FROM s."date"
	   OR b."type"            IS DISTINCT FROM s."type"
	--   OR b."source_filename" IS DISTINCT FROM s."source_filename"
	   OR b."source_lineno"   IS DISTINCT FROM s."source_lineno")
	ORDER BY 1

), transactions_detail_diffs AS (	

	WITH
	"b" AS (
	  SELECT
	    "id",
	    "flag",
	    "payee",
	    "narration",
	    "tags",
	    "links"
	  FROM "beancount"."transactions_detail"
	),
	--	
	"s" AS (
	  SELECT
	    "id",
	    "flag",
	    "payee",
	    "narration",
	    "tags",
	    "links"
	  FROM "sqlite"."transactions_detail"
	  )
	--
	SELECT
	  COALESCE("b"."id", "s"."id") AS "id",
	  "b"."flag"      AS "bc_flag",      "s"."flag"      AS "sqlite_flag",
	  "b"."payee"     AS "bc_payee",     "s"."payee"     AS "sqlite_payee",
	  "b"."narration" AS "bc_narration", "s"."narration" AS "sqlite_narration",
	  "b"."tags"      AS "bc_tags",      "s"."tags"      AS "sqlite_tags",
	  "b"."links"     AS "bc_links",     "s"."links"     AS "sqlite_links"
	FROM "b"
	FULL OUTER JOIN "s" ON "b"."id" = "s"."id"
	WHERE ("b"."flag"      IS DISTINCT FROM "s"."flag")
	   OR ("b"."payee"     IS DISTINCT FROM "s"."payee")
	   OR ("b"."narration" IS DISTINCT FROM "s"."narration")
	   OR ("b"."tags"      IS DISTINCT FROM "s"."tags")
	   OR ("b"."links"     IS DISTINCT FROM "s"."links")
	ORDER BY 1
	
), postings_diffs AS (

	WITH b AS (
	  SELECT
	    "posting_id",
	    "id",
	    "flag",
	    "account",
	    "number",
	    "currency",
	    "cost_number",
	    "cost_currency",
	    "cost_date",
	    "cost_label",
	    "price_number",
	    "price_currency"
	  FROM "beancount"."postings"
	),
	s AS (
	  SELECT
	    "posting_id",
	    "id",
	    "flag",
	    "account",
	    "number",
	    "currency",
	    "cost_number",
	    "cost_currency",
	    "cost_date",
	    "cost_label",
	    "price_number",
	    "price_currency"
	  FROM "sqlite"."postings"
	)
	SELECT
	  COALESCE(b."posting_id", s."posting_id")    AS "posting_id",
	  COALESCE(b."id", s."id")                    AS "id",
	  b."flag"          AS "bc_flag",             s."flag"          AS "sqlite_flag",
	  b."account"       AS "bc_account",          s."account"       AS "sqlite_account",
	  b."number"        AS "bc_number",           s."number"        AS "sqlite_number",
	  b."currency"      AS "bc_currency",         s."currency"      AS "sqlite_currency",
	  b."cost_number"   AS "bc_cost_number",      s."cost_number"   AS "sqlite_cost_number",
	  b."cost_currency" AS "bc_cost_currency",    s."cost_currency" AS "sqlite_cost_currency",
	  b."cost_date"     AS "bc_cost_date",        s."cost_date"     AS "sqlite_cost_date",
	  b."cost_label"    AS "bc_cost_label",       s."cost_label"    AS "sqlite_cost_label",
	  b."price_number"  AS "bc_price_number",     s."price_number"  AS "sqlite_price_number",
	  b."price_currency"AS "bc_price_currency",   s."price_currency"AS "sqlite_price_currency"
	FROM b
	 FULL OUTER JOIN s 	ON 	b."posting_id" = s."posting_id" 
	 					and b."id" = s."id"
	WHERE b."id"            IS DISTINCT FROM s."id"
	   OR b."flag"          IS DISTINCT FROM s."flag"
	   OR b."account"       IS DISTINCT FROM s."account"
	   OR b."number"        IS DISTINCT FROM s."number"
	   OR b."currency"      IS DISTINCT FROM s."currency"
	   OR b."cost_number"   IS DISTINCT FROM s."cost_number"
	   OR b."cost_currency" IS DISTINCT FROM s."cost_currency"
	   OR b."cost_date"     IS DISTINCT FROM s."cost_date"
	   OR b."cost_label"    IS DISTINCT FROM s."cost_label"
	   OR b."price_number"  IS DISTINCT FROM s."price_number"
	   OR b."price_currency"IS DISTINCT FROM s."price_currency"
	ORDER BY 1, 2

), open_detail_diffs AS (

	WITH b AS (
	  SELECT "id", "account", "currencies"
	  FROM "beancount"."open_detail"
	),
	s AS (
	  SELECT "id", "account", "currencies"
	  FROM "sqlite"."open_detail"
	)
	SELECT
	  COALESCE(b."id", s."id")        AS "id",
	  b."account"      AS "bc_account",    s."account"      AS "sqlite_account",
	  b."currencies"   AS "bc_currencies", s."currencies"   AS "sqlite_currencies"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."account"    IS DISTINCT FROM s."account"
	   OR b."currencies" IS DISTINCT FROM s."currencies"
	ORDER BY 1

), close_detail_diffs AS (

	WITH b AS (
	  SELECT "id", "account" FROM "beancount"."close_detail"
	),
	s AS (
	  SELECT "id", "account" FROM "sqlite"."close_detail"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."account" AS "bc_account",
	  s."account" AS "sqlite_account"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."account" IS DISTINCT FROM s."account"
	ORDER BY 1

), pad_detail_diffs AS (

	WITH b AS (
	  SELECT "id", "account", "source_account"
	  FROM "beancount"."pad_detail"
	),
	s AS (
	  SELECT "id", "account", "source_account"
	  FROM "sqlite"."pad_detail"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."account"        AS "bc_account",        s."account"        AS "sqlite_account",
	  b."source_account" AS "bc_source_account",s."source_account" AS "sqlite_source_account"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."account"        IS DISTINCT FROM s."account"
	   OR b."source_account" IS DISTINCT FROM s."source_account"
	ORDER BY 1

), balance_detail_diffs AS (

	WITH b AS (
	  SELECT 
	  	"id", 
	  	"account", 
	  	"amount_number", 
	  	"amount_currency", 
	  	case when "diff_number"<0 then 0 else "diff_number" end as "diff_number", -- This is to ignore a known bug in sql.py 
	  	"diff_currency"
	  FROM "beancount"."balance_detail"
	),
	s AS (
	  SELECT "id", "account", "amount_number", "amount_currency", "diff_number", "diff_currency"
	  FROM "sqlite"."balance_detail"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."account"        AS "bc_account",        s."account"        AS "sqlite_account",
	  b."amount_number"  AS "bc_amount_number",  s."amount_number"  AS "sqlite_amount_number",
	  b."amount_currency"AS "bc_amount_currency",s."amount_currency"AS "sqlite_amount_currency",
	  b."diff_number" 	 AS "bc_diff_number",	 s."diff_number"    AS "sqlite_diff_number",
	  b."diff_currency"  AS "bc_diff_currency",  s."diff_currency"  AS "sqlite_diff_currency"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."account"         IS DISTINCT FROM s."account"
	   OR b."amount_number"   IS DISTINCT FROM s."amount_number"
	   OR b."amount_currency" IS DISTINCT FROM s."amount_currency"
	   OR b."diff_number"     IS DISTINCT FROM s."diff_number"
	   OR b."diff_currency"   IS DISTINCT FROM s."diff_currency"
	ORDER BY 1

), note_detail_diffs AS (

	WITH b AS (
	  SELECT "id", "account", "comment" FROM "beancount"."note_detail"
	),
	s AS (
	  SELECT "id", "account", "comment" FROM "sqlite"."note_detail"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."account" AS "bc_account", s."account" AS "sqlite_account",
	  b."comment" AS "bc_comment", s."comment" AS "sqlite_comment"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."account" IS DISTINCT FROM s."account"
	   OR b."comment" IS DISTINCT FROM s."comment"
	ORDER BY 1

), document_detail_diffs AS (

	WITH b AS (
	  SELECT "id", "account", "filenam" FROM "beancount"."document_detail"
	),
	s AS (
	  SELECT "id", "account", "filenam" FROM "sqlite"."document_detail"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."account" AS "bc_account", s."account" AS "sqlite_account",
	  b."filenam" AS "bc_filename", s."filenam" AS "sqlite_filename"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."account" IS DISTINCT FROM s."account"
	   OR b."filenam" IS DISTINCT FROM s."filenam"
	ORDER BY 1

), price_detail_diffs AS (

	WITH b AS (
	  SELECT "id", "currency", "amount_number", "amount_currency"
	  FROM "beancount"."price_detail"
	),
	s AS (
	  SELECT "id", "currency", "amount_number", "amount_currency"
	  FROM "sqlite"."price_detail"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."currency"        AS "bc_currency",        s."currency"        AS "sqlite_currency",
	  b."amount_number"   AS "bc_amount_number",   s."amount_number"   AS "sqlite_amount_number",
	  b."amount_currency" AS "bc_amount_currency", s."amount_currency" AS "sqlite_amount_currency"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."currency"        IS DISTINCT FROM s."currency"
	   OR b."amount_number"   IS DISTINCT FROM s."amount_number"
	   OR b."amount_currency" IS DISTINCT FROM s."amount_currency"
	ORDER BY 1


/*****************************
 * Differences Between Views *
 *****************************/

), transactions_view_diffs AS (

	WITH b AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"flag"
	  ,"payee"
	  ,"narration"
	  ,"tags"
	  ,"links"
	  FROM "beancount"."transactions"
	),
	s AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"flag"
	  ,"payee"
	  ,"narration"
	  ,"tags"
	  ,"links"
	  FROM "sqlite"."transactions"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."date"            AS "bc_date",            s."date"            AS "sqlite_date",
	  b."type"            AS "bc_type",            s."type"            AS "sqlite_type",
	--  b."source_filename" AS "bc_source_filename", s."source_filename" AS "sqlite_source_filename",
	  b."source_lineno"   AS "bc_source_lineno",   s."source_lineno"   AS "sqlite_source_lineno",
	  b."flag"            AS "bc_flag",            s."flag"            AS "sqlite_flag",
	  b."payee"           AS "bc_payee",           s."payee"           AS "sqlite_payee",
	  b."narration"       AS "bc_narration",       s."narration"       AS "sqlite_narration",
	  b."tags"            AS "bc_tags",            s."tags"            AS "sqlite_tags",
	  b."links"           AS "bc_links",           s."links"           AS "sqlite_links"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."date"            IS DISTINCT FROM s."date"
	   OR b."type"            IS DISTINCT FROM s."type"
	--   OR b."source_filename" IS DISTINCT FROM s."source_filename"
	   OR b."source_lineno"   IS DISTINCT FROM s."source_lineno"
	   OR b."flag"            IS DISTINCT FROM s."flag"
	   OR b."payee"           IS DISTINCT FROM s."payee"
	   OR b."narration"       IS DISTINCT FROM s."narration"
	   OR b."tags"            IS DISTINCT FROM s."tags"
	   OR b."links"           IS DISTINCT FROM s."links"
	ORDER BY 1

), open_view_diffs AS (

	WITH b AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"account"
	  ,"currencies"
	  FROM "beancount"."open"
	),
	s AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"account"
	  ,"currencies"
	  FROM "sqlite"."open"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."date"            AS "bc_date",            s."date"            AS "sqlite_date",
	  b."type"            AS "bc_type",            s."type"            AS "sqlite_type",
	--  b."source_filename" AS "bc_source_filename", s."source_filename" AS "sqlite_source_filename",
	  b."source_lineno"   AS "bc_source_lineno",   s."source_lineno"   AS "sqlite_source_lineno",
	  b."account"         AS "bc_account",         s."account"         AS "sqlite_account",
	  b."currencies"      AS "bc_currencies",      s."currencies"      AS "sqlite_currencies"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."date"            IS DISTINCT FROM s."date"
	   OR b."type"            IS DISTINCT FROM s."type"
	--   OR b."source_filename" IS DISTINCT FROM s."source_filename"
	   OR b."source_lineno"   IS DISTINCT FROM s."source_lineno"
	   OR b."account"         IS DISTINCT FROM s."account"
	   OR b."currencies"      IS DISTINCT FROM s."currencies"
	ORDER BY 1

), close_view_diffs AS (

	WITH b AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"account"
	  FROM "beancount"."close"
	),
	s AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"account"
	  FROM "sqlite"."close"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."date"            AS "bc_date",            s."date"            AS "sqlite_date",
	  b."type"            AS "bc_type",            s."type"            AS "sqlite_type",
	--  b."source_filename" AS "bc_source_filename", s."source_filename" AS "sqlite_source_filename",
	  b."source_lineno"   AS "bc_source_lineno",   s."source_lineno"   AS "sqlite_source_lineno",
	  b."account"         AS "bc_account",         s."account"         AS "sqlite_account"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."date"            IS DISTINCT FROM s."date"
	   OR b."type"            IS DISTINCT FROM s."type"
	--   OR b."source_filename" IS DISTINCT FROM s."source_filename"
	   OR b."source_lineno"   IS DISTINCT FROM s."source_lineno"
	   OR b."account"         IS DISTINCT FROM s."account"
	ORDER BY 1

), pad_view_diffs AS (

	WITH b AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"account"
	  ,"source_account"
	  FROM "beancount"."pad"
	),
	s AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"account"
	  ,"source_account"
	  FROM "sqlite"."pad"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."date"            AS "bc_date",            s."date"            AS "sqlite_date",
	  b."type"            AS "bc_type",            s."type"            AS "sqlite_type",
	--  b."source_filename" AS "bc_source_filename", s."source_filename" AS "sqlite_source_filename",
	  b."source_lineno"   AS "bc_source_lineno",   s."source_lineno"   AS "sqlite_source_lineno",
	  b."account"         AS "bc_account",         s."account"         AS "sqlite_account",
	  b."source_account"  AS "bc_source_account",  s."source_account"  AS "sqlite_source_account"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."date"            IS DISTINCT FROM s."date"
	   OR b."type"            IS DISTINCT FROM s."type"
	--   OR b."source_filename" IS DISTINCT FROM s."source_filename"
	   OR b."source_lineno"   IS DISTINCT FROM s."source_lineno"
	   OR b."account"         IS DISTINCT FROM s."account"
	   OR b."source_account"  IS DISTINCT FROM s."source_account"
	ORDER BY 1

), balance_view_diffs AS (

	WITH b AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"account"
	  ,"amount_number"
	  ,"amount_currency"
	  , case when "diff_number"<0 then 0 else "diff_number" end as "diff_number" -- This is to ignore a known bug in sql.py 
	  ,"diff_currency"
	  FROM "beancount"."balance"
	),
	s AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"account"
	  ,"amount_number"
	  ,"amount_currency"
	  ,"diff_number"
	  ,"diff_currency"
	  FROM "sqlite"."balance"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."date"            AS "bc_date",            s."date"            AS "sqlite_date",
	  b."type"            AS "bc_type",            s."type"            AS "sqlite_type",
	--  b."source_filename" AS "bc_source_filename", s."source_filename" AS "sqlite_source_filename",
	  b."source_lineno"   AS "bc_source_lineno",   s."source_lineno"   AS "sqlite_source_lineno",
	  b."account"         AS "bc_account",         s."account"         AS "sqlite_account",
	  b."amount_number"   AS "bc_amount_number",   s."amount_number"   AS "sqlite_amount_number",
	  b."amount_currency" AS "bc_amount_currency", s."amount_currency" AS "sqlite_amount_currency",
	  b."diff_number" 	 AS "bc_diff_number",	 s."diff_number"    AS "sqlite_diff_number",  
	  b."diff_currency"   AS "bc_diff_currency",   s."diff_currency"   AS "sqlite_diff_currency"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."date"            IS DISTINCT FROM s."date"
	   OR b."type"            IS DISTINCT FROM s."type"
	--   OR b."source_filename" IS DISTINCT FROM s."source_filename"
	   OR b."source_lineno"   IS DISTINCT FROM s."source_lineno"
	   OR b."account"         IS DISTINCT FROM s."account"
	   OR b."amount_number"   IS DISTINCT FROM s."amount_number"
	   OR b."amount_currency" IS DISTINCT FROM s."amount_currency"
	   OR b."diff_number"     IS DISTINCT FROM s."diff_number"
	   OR b."diff_currency"   IS DISTINCT FROM s."diff_currency"
	ORDER BY 1

), note_view_diffs AS (

	WITH b AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"account"
	  ,"comment"
	  FROM "beancount"."note"
	),
	s AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"account"
	  ,"comment"  
	  FROM "sqlite"."note"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."date"            AS "bc_date",            s."date"            AS "sqlite_date",
	  b."type"            AS "bc_type",            s."type"            AS "sqlite_type",
	--  b."source_filename" AS "bc_source_filename", s."source_filename" AS "sqlite_source_filename",
	  b."source_lineno"   AS "bc_source_lineno",   s."source_lineno"   AS "sqlite_source_lineno",
	  b."account"         AS "bc_account",         s."account"         AS "sqlite_account",
	  b."comment"         AS "bc_comment",         s."comment"         AS "sqlite_comment"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."date"            IS DISTINCT FROM s."date"
	   OR b."type"            IS DISTINCT FROM s."type"
	--   OR b."source_filename" IS DISTINCT FROM s."source_filename"
	   OR b."source_lineno"   IS DISTINCT FROM s."source_lineno"
	   OR b."account"         IS DISTINCT FROM s."account"
	   OR b."comment"         IS DISTINCT FROM s."comment"
	ORDER BY 1

), price_view_diffs AS (

	WITH b AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"currency"
	  ,"amount_number"
	  ,"amount_currency"
	  FROM "beancount"."price"
	),
	s AS (
	  SELECT
	    "id"
	  ,"date"
	  ,"type"
	--  ,"source_filename"
	  ,"source_lineno"
	  ,"currency"
	  ,"amount_number"
	  ,"amount_currency"
	  FROM "sqlite"."price"
	)
	SELECT
	  COALESCE(b."id", s."id") AS "id",
	  b."date"            AS "bc_date",            s."date"            AS "sqlite_date",
	  b."type"            AS "bc_type",            s."type"            AS "sqlite_type",
	--  b."source_filename" AS "bc_source_filename", s."source_filename" AS "sqlite_source_filename",
	  b."source_lineno"   AS "bc_source_lineno",   s."source_lineno"   AS "sqlite_source_lineno",
	  b."currency"        AS "bc_currency",        s."currency"        AS "sqlite_currency",
	  b."amount_number"   AS "bc_amount_number",   s."amount_number"   AS "sqlite_amount_number",
	  b."amount_currency" AS "bc_amount_currency", s."amount_currency" AS "sqlite_amount_currency"
	FROM b
	FULL OUTER JOIN s ON b."id" = s."id"
	WHERE b."date"            IS DISTINCT FROM s."date"
	   OR b."type"            IS DISTINCT FROM s."type"
	--   OR b."source_filename" IS DISTINCT FROM s."source_filename"
	   OR b."source_lineno"   IS DISTINCT FROM s."source_lineno"
	   OR b."currency"        IS DISTINCT FROM s."currency"
	   OR b."amount_number"   IS DISTINCT FROM s."amount_number"
	   OR b."amount_currency" IS DISTINCT FROM s."amount_currency"
	ORDER BY 1

)			SELECT 'entry'	as "name", 		'table' as "type", count(*) as "num_diffs" FROM entry_diffs
union all 	SELECT 'transactions_detail',	'table'	as "type", count(*) as "num_diffs" FROM transactions_detail_diffs
union all	SELECT 'postings',				'table' as "type", count(*) as "num_diffs" FROM postings_diffs
union all	SELECT 'open_detail',			'table' as "type", count(*) as "num_diffs" FROM open_detail_diffs
union all	SELECT 'close_detail',			'table' as "type", count(*) as "num_diffs" FROM close_detail_diffs
union all	SELECT 'pad_detail',			'table' as "type", count(*) as "num_diffs" FROM pad_detail_diffs
union all	SELECT 'balance_detail',		'table' as "type", count(*) as "num_diffs" FROM balance_detail_diffs
union all	SELECT 'note_detail',			'table' as "type", count(*) as "num_diffs" FROM note_detail_diffs
union all	SELECT 'document_detail',		'table' as "type", count(*) as "num_diffs" FROM document_detail_diffs
union all	SELECT 'price_detail',			'table' as "type", count(*) as "num_diffs" FROM price_detail_diffs
union all	SELECT 'transactions',			'view'  as "type", count(*) as "num_diffs" FROM transactions_view_diffs
union all	SELECT 'open',					'view'  as "type", count(*) as "num_diffs" FROM open_view_diffs
union all	SELECT 'close',					'view'  as "type", count(*) as "num_diffs" FROM close_view_diffs
union all	SELECT 'pad',					'view'  as "type", count(*) as "num_diffs" FROM pad_view_diffs
union all	SELECT 'balance',				'view'  as "type", count(*) as "num_diffs" FROM balance_view_diffs
union all	SELECT 'note',					'view'  as "type", count(*) as "num_diffs" FROM note_view_diffs
union all	SELECT 'price',					'view'  as "type", count(*) as "num_diffs" FROM price_view_diffs
;