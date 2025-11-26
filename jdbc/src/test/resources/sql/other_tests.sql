-- Mount the SQLite database created with bean-sql 
CREATE FOREIGN SCHEMA sqlite
  TYPE 'jdbc'
  OPTIONS (
    jdbcDriver 'org.sqlite.JDBC',
    jdbcUrl    'jdbc:sqlite:C:/Users/jacob-walker/Documents/GitHub/beancount-jdbc/jdbc/src/test/resources/regression/bean_sql/${datasource}.sqlite',
    sqlDialectFactory 'com.beancount.jdbc.calcite.sqlite.BeancountSqliteDialectFactory'
  );

-- Check for Table and Views All Existing
SELECT 
  sqlite_schema.name as sqlite_name
, "TABLES"."tableName" as bc_name  
, sqlite_schema.type
FROM sqlite.sqlite_schema
left join "metadata"."TABLES" on "sqlite_schema"."name" = "TABLES"."tableName" 
and  sqlite_schema.type = lower("TABLES".tableType)
WHERE type IN ('table','view');