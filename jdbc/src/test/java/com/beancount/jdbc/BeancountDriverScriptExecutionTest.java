package com.beancount.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beancount.jdbc.testing.TestResources;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

final class BeancountDriverScriptExecutionTest {

    @Test
    void executesMultipleSelectsReturningLastResult() throws Exception {
        Class.forName("com.beancount.jdbc.BeancountDriver");
        String ledger =
                TestResources.absolutePath("third_party/beancount/examples/example.beancount")
                        .toString();
        try (Connection connection = DriverManager.getConnection("jdbc:beancount:" + ledger);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT 1; SELECT 2;")) {
            assertTrue(rs.next(), "Expected a row from the final SELECT");
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next(), "Only the final SELECT result set should be visible");
        }
    }

    @Test
    void acceptsTrailingSemicolonOnSingleSelect() throws Exception {
        Class.forName("com.beancount.jdbc.BeancountDriver");
        String ledger =
                TestResources.absolutePath("third_party/beancount/examples/example.beancount")
                        .toString();
        try (Connection connection = DriverManager.getConnection("jdbc:beancount:" + ledger);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT 42;")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void executeUpdateRejectsTrailingQuery() throws Exception {
        Class.forName("com.beancount.jdbc.BeancountDriver");
        String ledger =
                TestResources.absolutePath("third_party/beancount/examples/example.beancount")
                        .toString();
        try (Connection connection = DriverManager.getConnection("jdbc:beancount:" + ledger);
                Statement statement = connection.createStatement()) {
            assertThrows(
                    SQLException.class,
                    () -> statement.executeUpdate("SELECT 1;"),
                    "executeUpdate should reject scripts whose final statement is a query");
        }
    }
}
