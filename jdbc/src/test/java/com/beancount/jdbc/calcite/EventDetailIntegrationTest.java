package com.beancount.jdbc.calcite;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;

final class EventDetailIntegrationTest {

    @Test
    void eventDetailReturnsRows() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        String ledger =
                CalciteIntegrationTestSupport.moduleLedgerPath(
                        "classpath:regression/ledgers/directives.beancount");
        Properties props = CalciteIntegrationTestSupport.newCalciteConnectionProperties(ledger);
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT \"id\", \"type\", \"description\" "
                                        + "FROM \"beancount\".\"event_detail\" "
                                        + "ORDER BY \"id\"")) {
            int rowCount = 0;
            boolean foundAuditEvent = false;
            while (rs.next()) {
                rowCount++;
                if ("audit".equals(rs.getString("type"))) {
                    foundAuditEvent = true;
                }
            }
            assertTrue(rowCount > 0, "Expected event_detail to return rows");
            assertTrue(foundAuditEvent, "Expected to find the audit event");
        }
    }
}
