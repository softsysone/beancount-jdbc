package com.beancount.jdbc.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.beancount.jdbc.ledger.LedgerEntry;
import com.beancount.jdbc.ledger.TransactionPayload;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;

class TransactionsDetailTableTest {

    @Test
    void emitsEmptyStringsWhenTagsAndLinksMissing() {
        TransactionPayload payload = new TransactionPayload(null, null, null, null, null);
        LedgerEntry entry =
                new LedgerEntry(1, LocalDate.parse("2024-01-01"), "txn", "file", 1, payload);

        List<Object[]> rows = TransactionsDetailTable.materializeRows(List.of(entry));

        Object[] row = rows.get(0);
        assertEquals(null, row[4], "tags column should remain null when missing");
        assertEquals(null, row[5], "links column should remain null when missing");
    }
}
