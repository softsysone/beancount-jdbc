package com.beancount.jdbc.schema;

import com.beancount.jdbc.ledger.LedgerEntry;
import java.sql.Date;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Mirrors the bean-sql {@code entry} table defined in {@code third_party/beancount/experiments/sql/sql.py}.
 */
public final class EntryTable {
    public static final String NAME = "entry";

    private static final TableDefinition DEFINITION = createDefinition();

    private EntryTable() {}

    public static TableDefinition getDefinition() {
        return DEFINITION;
    }

    public static List<Object[]> materializeRows(List<LedgerEntry> entries) {
        List<Object[]> rows = new ArrayList<>(entries.size());
        for (LedgerEntry entry : entries) {
            rows.add(
                    new Object[] {
                        entry.getId(),
                        toEpochDay(entry.getDate()),
                        entry.getType(),
                        entry.getSourceFilename(),
                        entry.getSourceLineno()
                    });
        }
        return rows;
    }

    private static int toEpochDay(java.time.LocalDate date) {
        return Math.toIntExact(date.toEpochDay());
    }

    private static TableDefinition createDefinition() {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(
                new ColumnDescriptor(
                        "id", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "date", Types.DATE, "DATE", 0, 0, true, Date.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "type", Types.VARCHAR, "STRING", 0, 0, true, String.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "source_filename",
                        Types.VARCHAR,
                        "STRING",
                        0,
                        0,
                        true,
                        String.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "source_lineno",
                        Types.INTEGER,
                        "INTEGER",
                        10,
                        0,
                        true,
                        Integer.class.getName()));
        return new TableDefinition(NAME, "TABLE", "Common entry data", columns);
    }
}
