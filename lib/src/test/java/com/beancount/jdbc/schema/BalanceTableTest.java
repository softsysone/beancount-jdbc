package com.beancount.jdbc.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.beancount.jdbc.ledger.BalanceRecord;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

class BalanceTableTest {

    @Test
    void diffColumnsMatchBeanSqlLegacyOutput() {
        BalanceRecord record =
                new BalanceRecord(
                        1,
                        "Assets:Cash",
                        new BigDecimal("10"),
                        "USD",
                        new BigDecimal("-1"),
                        "USD",
                        null,
                        null);

        Object[] row = BalanceTable.materializeDetailRows(List.of(record)).get(0);

        assertEquals(new BigDecimal("-1"), row[4]); // diff_number column should hold numeric diff
        assertEquals("USD", row[5]); // diff_currency column
    }

    @Test
    void diffColumnsRemainNullWhenNoDiff() {
        BalanceRecord record =
                new BalanceRecord(
                        1,
                        "Assets:Cash",
                        new BigDecimal("10"),
                        "USD",
                        null,
                        null,
                        null,
                        null);

        Object[] row = BalanceTable.materializeDetailRows(List.of(record)).get(0);

        assertNull(row[4]);
        assertNull(row[5]);
    }
}
