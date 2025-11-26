package com.beancount.jdbc.schema;

import com.beancount.jdbc.ledger.LedgerEntry;
import com.beancount.jdbc.ledger.TransactionPayload;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/** Mirrors bean-sql's {@code transactions_detail} table. */
public final class TransactionsDetailTable {
    public static final String NAME = "transactions_detail";

    private static final TableDefinition DEFINITION = createDefinition();

    private TransactionsDetailTable() {}

    public static TableDefinition getDefinition() {
        return DEFINITION;
    }

    public static List<Object[]> materializeRows(List<LedgerEntry> entries) {
        List<Object[]> rows = new ArrayList<>();
        for (LedgerEntry entry : entries) {
            if (!"txn".equals(entry.getType())) {
                continue;
            }
            TransactionPayload payload = entry.getTransactionPayload();
            String tags = payload != null ? payload.getTags() : null;
            String links = payload != null ? payload.getLinks() : null;
            rows.add(
                    new Object[] {
                        entry.getId(),
                        payload != null ? payload.getFlag() : null,
                        payload != null ? payload.getPayee() : null,
                        payload != null ? payload.getNarration() : null,
                        tags,
                        links
                    });
        }
        return rows;
    }

    private static TableDefinition createDefinition() {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(
                new ColumnDescriptor(
                        "id", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "flag", Types.CHAR, "CHAR", 1, 0, true, String.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "payee", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "narration", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "tags", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "links", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        return new TableDefinition(NAME, "TABLE", "Transaction metadata", columns);
    }
}
