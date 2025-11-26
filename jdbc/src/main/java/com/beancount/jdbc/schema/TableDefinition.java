package com.beancount.jdbc.schema;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class TableDefinition {
    private final String name;
    private final String type;
    private final String remarks;
    private final List<ColumnDescriptor> columns;

    public TableDefinition(String name, String type, String remarks, List<ColumnDescriptor> columns) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        this.remarks = remarks;
        this.columns = List.copyOf(columns);
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getRemarks() {
        return remarks;
    }

    public List<ColumnDescriptor> getColumns() {
        return Collections.unmodifiableList(columns);
    }
}
