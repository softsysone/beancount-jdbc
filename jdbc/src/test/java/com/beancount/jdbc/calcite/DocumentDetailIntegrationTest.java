package com.beancount.jdbc.calcite;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;

final class DocumentDetailIntegrationTest {

    @Test
    void documentDetailReturnsRows() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        String ledger =
                CalciteIntegrationTestSupport.moduleLedgerPath(
                        "classpath:regression/ledgers/directives.beancount");
        Properties props = CalciteIntegrationTestSupport.newCalciteConnectionProperties(ledger);
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT \"id\", \"account\", \"filenam\" "
                                        + "FROM \"beancount\".\"document_detail\" "
                                        + "ORDER BY \"id\"")) {
            int rowCount = 0;
            boolean foundReceipt = false;
            while (rs.next()) {
                rowCount++;
                if ("receipts/feb.pdf".equals(rs.getString("filenam"))) {
                    foundReceipt = true;
                }
            }
            assertTrue(rowCount > 0, "Expected document_detail to return rows");
            assertTrue(foundReceipt, "Expected to find the receipts/feb.pdf document");
        }
    }
}
