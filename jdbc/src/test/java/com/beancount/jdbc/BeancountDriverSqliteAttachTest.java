package com.beancount.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beancount.jdbc.testing.TestResources;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

final class BeancountDriverSqliteAttachTest {

    @Test
    void createForeignSchemaAgainstSqliteFile() throws Exception {
        Class.forName("org.sqlite.JDBC");
        Class.forName("com.beancount.jdbc.BeancountDriver");

        String ledger = TestResources.absolutePath("third_party/beancount/examples/example.beancount").toString();

        Path sqliteFile = Files.createTempFile("sqlite-attach", ".db");
        sqliteFile.toFile().deleteOnExit();
        try (Connection sqlite = DriverManager.getConnection("jdbc:sqlite:" + sqliteFile);
                Statement stmt = sqlite.createStatement()) {
            stmt.execute("CREATE TABLE sample_data(id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO sample_data(id, name) VALUES (1, 'alpha'), (2, 'beta')");
        }

        Properties props = new Properties();
        props.setProperty(
                "parserFactory", "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY");
        try (Connection connection = DriverManager.getConnection("jdbc:beancount:" + ledger, props);
                Statement statement = connection.createStatement()) {
            String schemaName = "sqlite_" + UUID.randomUUID().toString().replace('-', '_');
            String jdbcUrlLiteral = ("jdbc:sqlite:" + sqliteFile.toAbsolutePath()).replace("'", "''");
            statement.execute(
                    "CREATE FOREIGN SCHEMA \""
                            + schemaName
                            + "\"\n"
                            + "  TYPE 'jdbc'\n"
                            + "  OPTIONS (\n"
                            + "    jdbcDriver 'org.sqlite.JDBC',\n"
                            + "    jdbcUrl '"
                            + jdbcUrlLiteral
                            + "',\n"
                            + "    sqlDialectFactory 'com.beancount.jdbc.calcite.sqlite.BeancountSqliteDialectFactory'\n"
                            + "  )");
            try (ResultSet rs =
                    statement.executeQuery(
                            "SELECT COUNT(*) FROM \""
                                    + schemaName
                                    + "\".\"sample_data\"")) {
                assertTrue(rs.next(), "Expected SQLite foreign table to produce a row");
                assertEquals(2, rs.getInt(1), "Foreign schema should expose SQLite rows");
            }
        }
    }
}
