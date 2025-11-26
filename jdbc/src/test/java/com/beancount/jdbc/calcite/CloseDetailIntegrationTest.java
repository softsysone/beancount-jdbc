package com.beancount.jdbc.calcite;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;

final class CloseDetailIntegrationTest {

    @Test
    void closeDetailReturnsRows() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        String ledger =
                CalciteIntegrationTestSupport.moduleLedgerPath(
                        "classpath:regression/ledgers/directives.beancount");
        Properties props = CalciteIntegrationTestSupport.newCalciteConnectionProperties(ledger);
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT \"id\", \"account\" FROM \"beancount\".\"close_detail\" ORDER BY \"id\" FETCH FIRST 5 ROWS ONLY")) {
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            assertTrue(rowCount > 0, "Expected close_detail to return rows");
        }
    }
}
