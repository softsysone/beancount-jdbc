package com.beancount.jdbc.calcite;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beancount.jdbc.testing.TestResources;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;

final class OpenDetailIntegrationTest {

    @Test
    void openDetailTableReturnsRows() throws Exception {
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
                                "SELECT \"id\", \"account\" FROM \"beancount\".\"open_detail\" ORDER BY \"id\" FETCH FIRST 5 ROWS ONLY")) {
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            assertTrue(rowCount > 0, "Expected open_detail to return rows");
        }
    }
}

