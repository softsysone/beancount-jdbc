package com.beancount.jdbc.calcite;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;

final class PriceDetailIntegrationTest {

    @Test
    void priceDetailReturnsRows() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        String ledger =
                CalciteIntegrationTestSupport.moduleLedgerPath(
                        "classpath:regression/ledgers/directives.beancount");
        Properties props = CalciteIntegrationTestSupport.newCalciteConnectionProperties(ledger);
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT \"id\", \"currency\", \"amount_number\", \"amount_currency\" "
                                        + "FROM \"beancount\".\"price_detail\" "
                                        + "ORDER BY \"id\"")) {
            int rowCount = 0;
            boolean foundBtcPrice = false;
            while (rs.next()) {
                rowCount++;
                if ("BTC".equals(rs.getString("currency"))
                        && "USD".equals(rs.getString("amount_currency"))) {
                    foundBtcPrice = true;
                }
            }
            assertTrue(rowCount > 0, "Expected price_detail to return rows");
            assertTrue(foundBtcPrice, "Expected to find the BTC price directive");
        }
    }
}
