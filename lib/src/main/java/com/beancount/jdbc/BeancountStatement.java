package com.beancount.jdbc;

import com.beancount.jdbc.schema.CloseTable;
import com.beancount.jdbc.schema.EntryTable;
import com.beancount.jdbc.schema.OpenTable;
import com.beancount.jdbc.schema.TableDefinition;
import com.beancount.jdbc.schema.TransactionsDetailTable;
import com.beancount.jdbc.schema.TransactionsView;
import com.beancount.jdbc.schema.PostingsTable;
import com.beancount.jdbc.schema.PadTable;
import com.beancount.jdbc.schema.BalanceTable;
import com.beancount.jdbc.schema.NoteTable;
import com.beancount.jdbc.schema.DocumentTable;
import com.beancount.jdbc.schema.EventTable;
import com.beancount.jdbc.schema.QueryTable;
import com.beancount.jdbc.schema.PriceTable;
import com.beancount.jdbc.util.SimpleResultSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

final class BeancountStatement implements Statement {

    private final BeancountConnection connection;
    private boolean closed = false;
    private ResultSet currentResultSet;
    private int maxRows = 0;

    BeancountStatement(BeancountConnection connection) {
        this.connection = Objects.requireNonNull(connection, "connection");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ensureOpen();
        closeCurrentResultSet();
        String tableName = extractTableName(sql);
        if (tableName == null) {
            throw new SQLException("Only simple SELECT * FROM <table> queries are supported in alpha driver.");
        }

        TableDefinition definition;
        List<Object[]> rows;
        switch (tableName) {
            case EntryTable.NAME:
                definition = EntryTable.getDefinition();
                rows = connection.getEntryRows();
                break;
            case TransactionsDetailTable.NAME:
                definition = TransactionsDetailTable.getDefinition();
                rows = connection.getTransactionsDetailRows();
                break;
            case TransactionsView.NAME:
                definition = TransactionsView.getDefinition();
                rows = connection.getTransactionsViewRows();
                break;
            case OpenTable.DETAIL_NAME:
                definition = OpenTable.getDetailDefinition();
                rows = connection.getOpenDetailRows();
                break;
            case OpenTable.VIEW_NAME:
                definition = OpenTable.getViewDefinition();
                rows = connection.getOpenViewRows();
                break;
            case CloseTable.DETAIL_NAME:
                definition = CloseTable.getDetailDefinition();
                rows = connection.getCloseDetailRows();
                break;
            case CloseTable.VIEW_NAME:
                definition = CloseTable.getViewDefinition();
                rows = connection.getCloseViewRows();
                break;
            case PadTable.DETAIL_NAME:
                definition = PadTable.getDetailDefinition();
                rows = connection.getPadDetailRows();
                break;
            case PadTable.VIEW_NAME:
                definition = PadTable.getViewDefinition();
                rows = connection.getPadViewRows();
                break;
            case BalanceTable.DETAIL_NAME:
                definition = BalanceTable.getDetailDefinition();
                rows = connection.getBalanceDetailRows();
                break;
            case BalanceTable.VIEW_NAME:
                definition = BalanceTable.getViewDefinition();
                rows = connection.getBalanceViewRows();
                break;
            case NoteTable.DETAIL_NAME:
                definition = NoteTable.getDetailDefinition();
                rows = connection.getNoteDetailRows();
                break;
            case NoteTable.VIEW_NAME:
                definition = NoteTable.getViewDefinition();
                rows = connection.getNoteViewRows();
                break;
            case DocumentTable.DETAIL_NAME:
                definition = DocumentTable.getDetailDefinition();
                rows = connection.getDocumentDetailRows();
                break;
            case DocumentTable.VIEW_NAME:
                definition = DocumentTable.getViewDefinition();
                rows = connection.getDocumentViewRows();
                break;
            case EventTable.DETAIL_NAME:
                definition = EventTable.getDetailDefinition();
                rows = connection.getEventDetailRows();
                break;
            case EventTable.VIEW_NAME:
                definition = EventTable.getViewDefinition();
                rows = connection.getEventViewRows();
                break;
            case QueryTable.DETAIL_NAME:
                definition = QueryTable.getDetailDefinition();
                rows = connection.getQueryDetailRows();
                break;
            case QueryTable.VIEW_NAME:
                definition = QueryTable.getViewDefinition();
                rows = connection.getQueryViewRows();
                break;
            case PriceTable.DETAIL_NAME:
                definition = PriceTable.getDetailDefinition();
                rows = connection.getPriceDetailRows();
                break;
            case PriceTable.VIEW_NAME:
                definition = PriceTable.getViewDefinition();
                rows = connection.getPriceViewRows();
                break;
            case PostingsTable.NAME:
                definition = PostingsTable.getDefinition();
                rows = connection.getPostingsRows();
                break;
            default:
                throw new SQLException("Unsupported table: " + tableName);
        }

        List<Object[]> limitedRows = rows;
        if (maxRows > 0 && rows.size() > maxRows) {
            limitedRows = rows.subList(0, maxRows);
        }
        currentResultSet = SimpleResultSet.from(definition.getColumns(), limitedRows);
        return currentResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    @Override
    public void close() {
        closed = true;
        closeCurrentResultSet();
    }

    @Override
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) {}

    @Override
    public int getMaxRows() {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) {
        this.maxRows = Math.max(0, max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) {}

    @Override
    public int getQueryTimeout() {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) {}

    @Override
    public void cancel() {}

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {}

    @Override
    public void setCursorName(String name) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Cursor names not supported.");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQuery(sql);
        return true;
    }

    @Override
    public ResultSet getResultSet() {
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() {
        return -1;
    }

    @Override
    public boolean getMoreResults() {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLFeatureNotSupportedException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Only FETCH_FORWARD is supported.");
        }
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) {}

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Batch statements not supported.");
    }

    @Override
    public void clearBatch() {}

    @Override
    public int[] executeBatch() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Batch statements not supported.");
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Generated keys not supported.");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Updates not supported.");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Updates not supported.");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Updates not supported.");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Updates not supported.");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Updates not supported.");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Updates not supported.");
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) {}

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public void closeOnCompletion() {}

    @Override
    public boolean isCloseOnCompletion() {
        return false;
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

    private void ensureOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed.");
        }
    }

    private void closeCurrentResultSet() {
        if (currentResultSet != null) {
            try {
                currentResultSet.close();
            } catch (SQLException ignored) {
                // Ignore errors when closing existing result sets.
            }
            currentResultSet = null;
        }
    }

    private static String extractTableName(String sql) {
        String trimmed = sql.trim();
        if (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        trimmed = trimmed.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        String lower = trimmed.toLowerCase(Locale.ROOT);
        if (!lower.startsWith("select")) {
            return null;
        }

        String[] tokens = lower.split("\\s+");
        if (tokens.length < 4 || !"select".equals(tokens[0]) || !"*".equals(tokens[1]) || !"from".equals(tokens[2])) {
            return null;
        }

        String identifier = trimmed.substring(trimmed.toLowerCase(Locale.ROOT).indexOf("from") + 4).trim();
        if (identifier.startsWith("\"")) {
            if (!identifier.endsWith("\"")) {
                return null;
            }
            identifier = identifier.substring(1, identifier.length() - 1);
        }
        identifier = identifier.trim();
        if (identifier.isEmpty()) {
            return null;
        }
        return identifier.toLowerCase(Locale.ROOT);
    }
}
