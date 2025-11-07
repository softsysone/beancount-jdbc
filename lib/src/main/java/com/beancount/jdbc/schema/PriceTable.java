package com.beancount.jdbc.schema;

import com.beancount.jdbc.ledger.PriceRecord;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Mirrors bean-sql's {@code price_detail} table and {@code price} view. */
public final class PriceTable {
    public static final String DETAIL_NAME = "price_detail";
    public static final String VIEW_NAME = "price";

    private static final TableDefinition DETAIL_DEFINITION = createDetailDefinition();
    private static final TableDefinition VIEW_DEFINITION = createViewDefinition();

    private PriceTable() {}

    public static TableDefinition getDetailDefinition() {
        return DETAIL_DEFINITION;
    }

    public static TableDefinition getViewDefinition() {
        return VIEW_DEFINITION;
    }

    public static List<Object[]> materializeDetailRows(List<PriceRecord> records) {
        List<Object[]> rows = new ArrayList<>(records.size());
        for (PriceRecord record : records) {
            rows.add(
                    new Object[] {
                        record.getEntryId(),
                        record.getCurrency(),
                        record.getAmountNumber(),
                        record.getAmountCurrency()
                    });
        }
        return rows;
    }

    public static List<Object[]> materializeViewRows(List<Object[]> entryRows, List<Object[]> detailRows) {
        Map<Integer, Object[]> entryById = new HashMap<>();
        for (Object[] entry : entryRows) {
            entryById.put((Integer) entry[0], entry);
        }
        List<Object[]> rows = new ArrayList<>(detailRows.size());
        for (Object[] detail : detailRows) {
            Object[] entry = entryById.get(detail[0]);
            if (entry == null) {
                continue;
            }
            rows.add(
                    new Object[] {
                        entry[0],
                        entry[1],
                        entry[2],
                        entry[3],
                        entry[4],
                        detail[1],
                        detail[2],
                        detail[3]
                    });
        }
        return rows;
    }

    private static TableDefinition createDetailDefinition() {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()));
        columns.add(new ColumnDescriptor("currency", Types.CHAR, "CHAR", 10, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("amount_number", Types.DECIMAL, "DECIMAL(16,6)", 16, 6, true, BigDecimal.class.getName()));
        columns.add(new ColumnDescriptor("amount_currency", Types.CHAR, "CHAR", 10, 0, true, String.class.getName()));
        return new TableDefinition(DETAIL_NAME, "TABLE", "Price directives", columns);
    }

    private static TableDefinition createViewDefinition() {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()));
        columns.add(new ColumnDescriptor("date", Types.DATE, "DATE", 0, 0, true, java.sql.Date.class.getName()));
        columns.add(new ColumnDescriptor("type", Types.CHAR, "CHAR", 8, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("source_filename", Types.VARCHAR, "STRING", 0, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("source_lineno", Types.INTEGER, "INTEGER", 10, 0, true, Integer.class.getName()));
        columns.add(new ColumnDescriptor("currency", Types.CHAR, "CHAR", 10, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("amount_number", Types.DECIMAL, "DECIMAL(16,6)", 16, 6, true, BigDecimal.class.getName()));
        columns.add(new ColumnDescriptor("amount_currency", Types.CHAR, "CHAR", 10, 0, true, String.class.getName()));
        return new TableDefinition(VIEW_NAME, "VIEW", "entry JOIN price_detail USING (id)", columns);
    }
}
