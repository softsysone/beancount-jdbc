package com.beancount.jdbc.util;

import com.beancount.jdbc.schema.ColumnDescriptor;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

final class SimpleResultSetMetaData implements ResultSetMetaData {

    private final List<ColumnDescriptor> columns;

    SimpleResultSetMetaData(List<ColumnDescriptor> columns) {
        this.columns = List.copyOf(columns);
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return true;
    }

    @Override
    public boolean isSearchable(int column) {
        return true;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) {
        return columns.get(column - 1).isNullable()
                ? ResultSetMetaData.columnNullable
                : ResultSetMetaData.columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) {
        int type = getColumnType(column);
        return type == Types.INTEGER || type == Types.BIGINT || type == Types.DECIMAL || type == Types.NUMERIC;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return columns.get(column - 1).getSize();
    }

    @Override
    public String getColumnLabel(int column) {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) {
        return columns.get(column - 1).getName();
    }

    @Override
    public String getSchemaName(int column) {
        return "";
    }

    @Override
    public int getPrecision(int column) {
        return columns.get(column - 1).getSize();
    }

    @Override
    public int getScale(int column) {
        return columns.get(column - 1).getScale();
    }

    @Override
    public String getTableName(int column) {
        return "";
    }

    @Override
    public String getCatalogName(int column) {
        return "";
    }

    @Override
    public int getColumnType(int column) {
        return columns.get(column - 1).getJdbcType();
    }

    @Override
    public String getColumnTypeName(int column) {
        return columns.get(column - 1).getTypeName();
    }

    @Override
    public boolean isReadOnly(int column) {
        return true;
    }

    @Override
    public boolean isWritable(int column) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) {
        return columns.get(column - 1).getClassName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }
}
