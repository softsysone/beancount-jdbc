package com.beancount.jdbc.schema;

import java.util.Objects;

public final class ColumnDescriptor {
    private final String name;
    private final int jdbcType;
    private final String typeName;
    private final int size;
    private final int scale;
    private final boolean nullable;
    private final String className;

    public ColumnDescriptor(
            String name,
            int jdbcType,
            String typeName,
            int size,
            int scale,
            boolean nullable,
            String className) {
        this.name = Objects.requireNonNull(name, "name");
        this.jdbcType = jdbcType;
        this.typeName = Objects.requireNonNull(typeName, "typeName");
        this.size = size;
        this.scale = scale;
        this.nullable = nullable;
        this.className = Objects.requireNonNull(className, "className");
    }

    public String getName() {
        return name;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getSize() {
        return size;
    }

    public int getScale() {
        return scale;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String getClassName() {
        return className;
    }
}
