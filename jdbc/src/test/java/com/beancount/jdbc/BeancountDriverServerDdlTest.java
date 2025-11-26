package com.beancount.jdbc;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beancount.jdbc.testing.TestResources;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;

final class BeancountDriverServerDdlTest {

    @Test
    void serverDdlParserCanExecuteCreateForeignSchema() throws Exception {
        Class.forName("com.beancount.jdbc.BeancountDriver");
        String ledger = TestResources.absolutePath("third_party/beancount/examples/example.beancount").toString();
        Properties props = new Properties();
        props.setProperty(
                "parserFactory", "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY");
        try (Connection connection = DriverManager.getConnection("jdbc:beancount:" + ledger, props);
                Statement statement = connection.createStatement()) {
            SQLException ex =
                    assertThrows(
                            SQLException.class,
                            () ->
                                    statement.execute(
                                            "CREATE FOREIGN SCHEMA sqlite\n"
                                                    + "  ( TYPE 'jdbc' )\n"
                                                    + "  OPTIONS (\n"
                                                    + "    jdbcDriver 'org.sqlite.JDBC',\n"
                                                    + "    jdbcUrl 'jdbc:sqlite::memory:'\n"
                                                    + "  )"));
            String message = ex.getMessage();
            assertTrue(
                    message != null && message.contains("org.sqlite.JDBC"),
                    "Expected JDBC adapter to reference sqlite driver class but saw: " + message);
        }
    }
}
