package com.beancount.jdbc.schema;

import com.beancount.jdbc.ledger.LedgerEntry;
import com.beancount.jdbc.ledger.PostingRecord;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Bean-sql-compatible {@code postings} table. */
public final class PostingsTable {
    public static final String NAME = "postings";

    private static final TableDefinition DEFINITION = createDefinition();

    private PostingsTable() {}

    public static TableDefinition getDefinition() {
        return DEFINITION;
    }

    public static List<Object[]> materializeRows(List<PostingRecord> postings, List<LedgerEntry> entries) {
        Map<Integer, Deque<PostingRecord>> byEntry = groupPostingsByEntry(postings);
        List<Object[]> rows = new ArrayList<>(postings.size());
        for (LedgerEntry entry : entries) {
            Deque<PostingRecord> queue = byEntry.remove(entry.getId());
            if (queue == null) {
                continue;
            }
            while (!queue.isEmpty()) {
                rows.add(toRow(queue.removeFirst()));
            }
        }
        for (Deque<PostingRecord> leftovers : byEntry.values()) {
            while (!leftovers.isEmpty()) {
                rows.add(toRow(leftovers.removeFirst()));
            }
        }
        return rows;
    }

    private static Map<Integer, Deque<PostingRecord>> groupPostingsByEntry(List<PostingRecord> postings) {
        Map<Integer, Deque<PostingRecord>> map = new LinkedHashMap<>();
        for (PostingRecord posting : postings) {
            map.computeIfAbsent(posting.getEntryId(), key -> new ArrayDeque<>()).addLast(posting);
        }
        return map;
    }

    private static Object[] toRow(PostingRecord posting) {
        return new Object[] {
            posting.getPostingId(),
            posting.getEntryId(),
            posting.getFlag(),
            posting.getAccount(),
            posting.getNumber(),
            posting.getCurrency(),
            posting.getCostNumber(),
            posting.getCostCurrency(),
            posting.getCostDate() != null ? Math.toIntExact(posting.getCostDate().toEpochDay()) : null,
            posting.getCostLabel(),
            posting.getPriceNumber(),
            posting.getPriceCurrency()
        };
    }

    private static TableDefinition createDefinition() {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("posting_id", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()));
        columns.add(new ColumnDescriptor("id", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()));
        columns.add(new ColumnDescriptor("flag", Types.CHAR, "CHAR", 1, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("account", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("number", Types.DECIMAL, "DECIMAL(16,6)", 16, 6, true, BigDecimal.class.getName()));
        columns.add(new ColumnDescriptor("currency", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("cost_number", Types.DECIMAL, "DECIMAL(16,6)", 16, 6, true, BigDecimal.class.getName()));
        columns.add(new ColumnDescriptor("cost_currency", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("cost_date", Types.DATE, "DATE", 0, 0, true, java.sql.Date.class.getName()));
        columns.add(new ColumnDescriptor("cost_label", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("price_number", Types.DECIMAL, "DECIMAL(16,6)", 16, 6, true, BigDecimal.class.getName()));
        columns.add(new ColumnDescriptor("price_currency", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        return new TableDefinition(NAME, "TABLE", "Postings detail", columns);
    }
}
