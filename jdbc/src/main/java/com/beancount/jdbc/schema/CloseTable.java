package com.beancount.jdbc.schema;

import com.beancount.jdbc.ledger.CloseRecord;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Mirrors bean-sql's {@code close_detail} table and {@code close} view. */
public final class CloseTable {
    public static final String DETAIL_NAME = "close_detail";
    public static final String VIEW_NAME = "close";

    private static final TableDefinition DETAIL_DEFINITION = createDetailDefinition();
    private static final TableDefinition VIEW_DEFINITION = createViewDefinition();

    private CloseTable() {}

    public static TableDefinition getDetailDefinition() {
        return DETAIL_DEFINITION;
    }

    public static TableDefinition getViewDefinition() {
        return VIEW_DEFINITION;
    }

    public static List<Object[]> materializeDetailRows(List<CloseRecord> records) {
        List<Object[]> rows = new ArrayList<>(records.size());
        for (CloseRecord record : records) {
            rows.add(new Object[] {record.getEntryId(), record.getAccount()});
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
                        detail[1]
                    });
        }
        return rows;
    }

    private static TableDefinition createDetailDefinition() {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()));
        columns.add(new ColumnDescriptor("account", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        return new TableDefinition(DETAIL_NAME, "TABLE", "Close account directives", columns);
    }

    private static TableDefinition createViewDefinition() {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()));
        columns.add(new ColumnDescriptor("date", Types.DATE, "DATE", 0, 0, true, java.sql.Date.class.getName()));
        columns.add(new ColumnDescriptor("type", Types.CHAR, "CHAR", 8, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("source_filename", Types.VARCHAR, "STRING", 0, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("source_lineno", Types.INTEGER, "INTEGER", 10, 0, true, Integer.class.getName()));
        columns.add(new ColumnDescriptor("account", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        return new TableDefinition(VIEW_NAME, "VIEW", "entry JOIN close_detail USING (id)", columns);
    }
}
