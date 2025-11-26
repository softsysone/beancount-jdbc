package com.beancount.jdbc;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beancount.jdbc.testing.TestResources;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class BeancountDriverIntegrationTest {

    @Test
    void driverConnectsAndQueriesEntryTable() throws Exception {
        Class.forName("com.beancount.jdbc.BeancountDriver");
        Path ledger = TestResources.absolutePath("third_party/beancount/examples/example.beancount");
        try (Connection connection = DriverManager.getConnection("jdbc:beancount:" + ledger);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM \"beancount\".\"entry\"")) {
            rs.next();
            int count = rs.getInt(1);
            assertTrue(count > 0, "Expected entry table to return rows via driver");
        }
    }

    @Test
    void metadataTablesVisibleViaDatabaseMetaData() throws Exception {
        Class.forName("com.beancount.jdbc.BeancountDriver");
        Path ledger = TestResources.absolutePath("third_party/beancount/examples/example.beancount");
        try (Connection connection = DriverManager.getConnection("jdbc:beancount:" + ledger)) {
            DatabaseMetaData metaData = connection.getMetaData();
            Set<String> tableNames = new LinkedHashSet<>();
            try (ResultSet rs =
                    metaData.getTables(null, "metadata", "%", new String[] {"TABLE", "VIEW"})) {
                while (rs.next()) {
                    tableNames.add(rs.getString("TABLE_NAME").toUpperCase(Locale.ROOT));
                }
            }
            assertTrue(
                    tableNames.contains("TABLES"),
                    "DatabaseMetaData#getTables should expose metadata.TABLES");
            assertTrue(
                    tableNames.contains("COLUMNS"),
                    "DatabaseMetaData#getTables should expose metadata.COLUMNS");
        }
    }

}
