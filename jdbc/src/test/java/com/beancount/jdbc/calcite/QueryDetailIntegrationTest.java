package com.beancount.jdbc.calcite;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;

final class QueryDetailIntegrationTest {

    @Test
    void queryDetailReturnsRows() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        String ledger =
                CalciteIntegrationTestSupport.moduleLedgerPath(
                        "classpath:regression/ledgers/directives.beancount");
        Properties props = CalciteIntegrationTestSupport.newCalciteConnectionProperties(ledger);
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT \"id\", \"name\", \"query_string\" "
                                        + "FROM \"beancount\".\"query_detail\" "
                                        + "ORDER BY \"id\"")) {
            int rowCount = 0;
            boolean foundSelect = false;
            while (rs.next()) {
                rowCount++;
                if ("entries".equals(rs.getString("name"))
                        && rs.getString("query_string").contains("SELECT 1")) {
                    foundSelect = true;
                }
            }
            assertTrue(rowCount > 0, "Expected query_detail to return rows");
            assertTrue(foundSelect, "Expected to find the entries query");
        }
    }
}
