package com.beancount.jdbc.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beancount.jdbc.testing.TestResources;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;

final class BalanceDetailIntegrationTest {

    @Test
    void balanceDetailReturnsRows() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        String ledger = TestResources.calciteLedgerOperand("third_party/beancount/examples/example.beancount");
        String model =
                """
                inline:{
                  "version":"1.0",
                  "defaultSchema":"beancount",
                  "schemas":[
                    {
                      "type":"custom",
                      "name":"beancount",
                      "factory":"com.beancount.jdbc.calcite.BeancountSchemaFactory",
                      "operand":{"ledger":"%s"}
                    }
                  ]
                }
                """
                        .formatted(ledger);
        Properties props = new Properties();
        props.setProperty("model", model);
        props.setProperty("lex", "JAVA");
        props.setProperty("quoting", "DOUBLE_QUOTE");
        props.setProperty("caseSensitive", "true");
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT \"id\", \"account\", \"amount_number\" FROM \"beancount\".\"balance_detail\" ORDER BY \"id\" FETCH FIRST 5 ROWS ONLY")) {
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            assertTrue(rowCount > 0, "Expected balance_detail to return rows");
        }
    }

    @Test
    void balanceDetailExposesDiffAmountsAndCurrencies() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        String ledger = TestResources.calciteLedgerOperand("jdbc/src/test/resources/ledger/balance_diff.beancount");
        String model =
                """
                inline:{
                  "version":"1.0",
                  "defaultSchema":"beancount",
                  "schemas":[
                    {
                      "type":"custom",
                      "name":"beancount",
                      "factory":"com.beancount.jdbc.calcite.BeancountSchemaFactory",
                      "operand":{"ledger":"%s"}
                    }
                  ]
                }
                """
                        .formatted(ledger);
        Properties props = new Properties();
        props.setProperty("model", model);
        props.setProperty("lex", "JAVA");
        props.setProperty("quoting", "DOUBLE_QUOTE");
        props.setProperty("caseSensitive", "true");
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT \"id\", \"account\", \"amount_number\", \"amount_currency\", \"diff_number\", \"diff_currency\" "
                                        + "FROM \"beancount\".\"balance_detail\" ORDER BY \"id\"")) {
            assertTrue(rs.next(), "Expected a balance row");
            assertEquals(1, rs.getInt("id"));
            assertEquals("Assets:Cash", rs.getString("account"));
            BigDecimal amountNumber = rs.getBigDecimal("amount_number");
            BigDecimal diffNumber = rs.getBigDecimal("diff_number");
            assertEquals(0, amountNumber.compareTo(new BigDecimal("10")));
            assertEquals(0, diffNumber.compareTo(new BigDecimal("-10")));
            assertEquals("AUD", rs.getString("amount_currency"));
            assertEquals("AUD", rs.getString("diff_currency"));
            assertFalse(rs.next(), "Expected only one balance row");
        }
    }
}
