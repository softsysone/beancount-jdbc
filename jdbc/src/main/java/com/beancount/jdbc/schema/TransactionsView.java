package com.beancount.jdbc.schema;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/** Mirrors bean-sql's {@code transactions} view. */
public final class TransactionsView {
    public static final String NAME = "transactions";

    private static final TableDefinition DEFINITION = createDefinition();

    private TransactionsView() {}

    public static TableDefinition getDefinition() {
        return DEFINITION;
    }

    private static TableDefinition createDefinition() {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()));
        columns.add(new ColumnDescriptor("date", Types.DATE, "DATE", 0, 0, true, java.sql.Date.class.getName()));
        columns.add(new ColumnDescriptor("type", Types.CHAR, "CHAR", 8, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("source_filename", Types.VARCHAR, "STRING", 0, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("source_lineno", Types.INTEGER, "INTEGER", 10, 0, true, Integer.class.getName()));
        columns.add(new ColumnDescriptor("flag", Types.CHAR, "CHAR", 1, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("payee", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("narration", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("tags", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        columns.add(new ColumnDescriptor("links", Types.VARCHAR, "VARCHAR", 0, 0, true, String.class.getName()));
        return new TableDefinition(NAME, "VIEW", "entry JOIN transactions_detail USING (id)", columns);
    }
}
