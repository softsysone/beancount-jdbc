package com.beancount.jdbc.util;

import com.beancount.jdbc.schema.ColumnDescriptor;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public final class SimpleResultSet implements ResultSet {

    private final List<ColumnDescriptor> columns;
    private final List<Object[]> rows;
    private int cursor = -1;
    private boolean closed = false;
    private Object lastValue = null;

    public SimpleResultSet(List<ColumnDescriptor> columns, List<Object[]> rows) {
        this.columns = List.copyOf(columns);
        this.rows = rows == null ? List.of() : List.copyOf(rows);
    }

    public static SimpleResultSet from(List<ColumnDescriptor> columns, List<Object[]> rows) {
        return new SimpleResultSet(columns, rows);
    }

    @Override
    public boolean next() {
        if (closed) {
            return false;
        }
        if (cursor + 1 >= rows.size()) {
            cursor = rows.size();
            return false;
        }
        cursor++;
        lastValue = null;
        return true;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean wasNull() {
        return lastValue == null;
    }

    @Override
    public String getString(int columnIndex) {
        Object value = getValue(columnIndex);
        return value != null ? value.toString() : null;
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        Object value = getValue(columnIndex);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        if (value == null) {
            return false;
        }
        return Boolean.parseBoolean(value.toString());
    }

    @Override
    public byte getByte(int columnIndex) {
        Object value = getValue(columnIndex);
        return value == null ? 0 : ((Number) value).byteValue();
    }

    @Override
    public short getShort(int columnIndex) {
        Object value = getValue(columnIndex);
        return value == null ? 0 : ((Number) value).shortValue();
    }

    @Override
    public int getInt(int columnIndex) {
        Object value = getValue(columnIndex);
        return value == null ? 0 : ((Number) value).intValue();
    }

    @Override
    public long getLong(int columnIndex) {
        Object value = getValue(columnIndex);
        return value == null ? 0L : ((Number) value).longValue();
    }

    @Override
    public float getFloat(int columnIndex) {
        Object value = getValue(columnIndex);
        return value == null ? 0f : ((Number) value).floatValue();
    }

    @Override
    public double getDouble(int columnIndex) {
        Object value = getValue(columnIndex);
        return value == null ? 0d : ((Number) value).doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) {
        Object value = getValue(columnIndex);
        return value == null ? null : (value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString()));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Date getDate(int columnIndex) {
        Object value = getValue(columnIndex);
        if (value instanceof Date) {
            return (Date) value;
        }
        if (value instanceof java.time.LocalDate) {
            return Date.valueOf((java.time.LocalDate) value);
        }
        return value == null ? null : Date.valueOf(value.toString());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public String getString(String columnLabel) {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Date getDate(String columnLabel) {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {}

    @Override
    public String getCursorName() throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return new SimpleResultSetMetaData(columns);
    }

    @Override
    public Object getObject(int columnIndex) {
        return getValue(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(columnLabel)) {
                return i + 1;
            }
        }
        throw new IllegalArgumentException("Unknown column: " + columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        Object value = getValue(columnIndex);
        if (value == null) {
            return null;
        }
        return value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString());
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() {
        return cursor < 0 && !rows.isEmpty();
    }

    @Override
    public boolean isAfterLast() {
        return cursor >= rows.size();
    }

    @Override
    public boolean isFirst() {
        return cursor == 0 && !rows.isEmpty();
    }

    @Override
    public boolean isLast() {
        return cursor == rows.size() - 1;
    }

    @Override
    public void beforeFirst() {
        cursor = -1;
    }

    @Override
    public void afterLast() {
        cursor = rows.size();
    }

    @Override
    public boolean first() {
        if (rows.isEmpty()) {
            return false;
        }
        cursor = 0;
        return true;
    }

    @Override
    public boolean last() {
        if (rows.isEmpty()) {
            return false;
        }
        cursor = rows.size() - 1;
        return true;
    }

    @Override
    public int getRow() {
        return cursor < 0 ? 0 : cursor + 1;
    }

    @Override
    public boolean absolute(int row) {
        int index = row - 1;
        if (index < 0 || index >= rows.size()) {
            cursor = rows.size();
            return false;
        }
        cursor = index;
        return true;
    }

    @Override
    public boolean relative(int rows) {
        int target = cursor + rows;
        if (target < 0 || target >= this.rows.size()) {
            cursor = this.rows.size();
            return false;
        }
        cursor = target;
        return true;
    }

    @Override
    public boolean previous() {
        if (cursor <= 0) {
            cursor = -1;
            return false;
        }
        cursor--;
        return true;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLFeatureNotSupportedException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw unsupported();
        }
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) {
        // Ignored.
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() {
        return false;
    }

    @Override
    public boolean rowInserted() {
        return false;
    }

    @Override
    public boolean rowDeleted() {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void insertRow() throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateRow() throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void deleteRow() throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void refreshRow() throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void cancelRowUpdates() throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void moveToInsertRow() throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void moveToCurrentRow() throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Statement getStatement() throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) {
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public int getHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public String getNString(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public String getNString(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLFeatureNotSupportedException {
        throw unsupported();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) {
        Object value = getValue(columnIndex);
        if (value == null) {
            return null;
        }
        return type.cast(value);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLFeatureNotSupportedException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    private Object getValue(int columnIndex) {
        if (cursor < 0 || cursor >= rows.size()) {
            throw new IllegalStateException("Cursor positioned outside of result set.");
        }
        if (columnIndex < 1 || columnIndex > columns.size()) {
            throw new IllegalArgumentException("Invalid column index: " + columnIndex);
        }
        Object value = rows.get(cursor)[columnIndex - 1];
        lastValue = value;
        return value;
    }

    private SQLFeatureNotSupportedException unsupported() {
        return new SQLFeatureNotSupportedException("Operation not supported in SimpleResultSet.");
    }
}
