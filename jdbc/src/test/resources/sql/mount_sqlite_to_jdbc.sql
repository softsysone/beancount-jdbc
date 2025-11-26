-- Mount the SQLite database created with bean-sql 

CREATE FOREIGN SCHEMA sqlite
  TYPE 'jdbc'
  OPTIONS (
    jdbcDriver 'org.sqlite.JDBC',
    jdbcUrl    'jdbc:sqlite:C:/Users/jacob-walker/Documents/GitHub/beancount-jdbc/jdbc/src/test/resources/regression/bean_sql/${datasource}.sqlite',
    sqlDialectFactory 'com.beancount.jdbc.calcite.sqlite.BeancountSqliteDialectFactory'
  );

-- Test Mount
SELECT *
FROM sqlite.sqlite_schema