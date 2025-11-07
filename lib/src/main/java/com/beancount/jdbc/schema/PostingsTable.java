package com.beancount.jdbc.schema;

import com.beancount.jdbc.ledger.PostingRecord;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/** Mirrors bean-sql's {@code postings} table. */
public final class PostingsTable {
    public static final String NAME = "postings";

    private static final TableDefinition DEFINITION = createDefinition();

    private PostingsTable() {}

    public static TableDefinition getDefinition() {
        return DEFINITION;
    }

    public static List<Object[]> materializeRows(List<PostingRecord> postings) {
        List<Object[]> rows = new ArrayList<>(postings.size());
        for (PostingRecord posting : postings) {
            rows.add(
                    new Object[] {
                        posting.getPostingId(),
                        posting.getEntryId(),
                        posting.getFlag(),
                        posting.getAccount(),
                        posting.getNumber(),
                        posting.getCurrency(),
                        posting.getCostNumber(),
                        posting.getCostCurrency(),
                        posting.getCostDate() != null ? Date.valueOf(posting.getCostDate()) : null,
                        posting.getCostLabel(),
                        posting.getPriceNumber(),
                        posting.getPriceCurrency()
                    });
        }
        return rows;
    }

    private static TableDefinition createDefinition() {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(
                new ColumnDescriptor(
                        "posting_id", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "id", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "flag", Types.CHAR, "CHAR", 1, 0, true, String.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "account", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "number", Types.DECIMAL, "DECIMAL(16,6)", 16, 6, true, BigDecimal.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "currency", Types.CHAR, "CHAR", 10, 0, true, String.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "cost_number", Types.DECIMAL, "DECIMAL(16,6)", 16, 6, true, BigDecimal.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "cost_currency", Types.CHAR, "CHAR", 10, 0, true, String.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "cost_date", Types.DATE, "DATE", 0, 0, true, Date.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "cost_label", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "price_number", Types.DECIMAL, "DECIMAL(16,6)", 16, 6, true, BigDecimal.class.getName()));
        columns.add(
                new ColumnDescriptor(
                        "price_currency", Types.CHAR, "CHAR", 10, 0, true, String.class.getName()));
        return new TableDefinition(NAME, "TABLE", "Postings detail", columns);
    }
}
