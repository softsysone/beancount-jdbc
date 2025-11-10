package com.beancount.jdbc;

import com.beancount.jdbc.loader.LoaderException;
import com.beancount.jdbc.loader.LoaderMessage;
import com.beancount.jdbc.loader.LoaderResult;
import com.beancount.jdbc.loader.semantic.SemanticLedger;
import com.beancount.jdbc.ledger.LedgerData;
import com.beancount.jdbc.ledger.LedgerProvider;
import com.beancount.jdbc.ledger.LedgerEntry;
import com.beancount.jdbc.schema.ColumnDescriptor;
import com.beancount.jdbc.schema.EntryTable;
import com.beancount.jdbc.schema.OpenTable;
import com.beancount.jdbc.schema.CloseTable;
import com.beancount.jdbc.schema.PadTable;
import com.beancount.jdbc.schema.BalanceTable;
import com.beancount.jdbc.schema.NoteTable;
import com.beancount.jdbc.schema.DocumentTable;
import com.beancount.jdbc.schema.EventTable;
import com.beancount.jdbc.schema.QueryTable;
import com.beancount.jdbc.schema.PriceTable;
import com.beancount.jdbc.schema.TableDefinition;
import com.beancount.jdbc.schema.TransactionsDetailTable;
import com.beancount.jdbc.schema.TransactionsView;
import com.beancount.jdbc.schema.PostingsTable;
import com.beancount.jdbc.util.SimpleResultSet;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.ShardingKey;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lightweight {@link Connection} implementation that only tracks basic connection state.
 *
 * <p>This class intentionally throws {@link SQLFeatureNotSupportedException} from every operation
 * that would require SQL execution. The goal for this alpha is to ensure that the driver can
 * validate and open a connection to a Beancount ledger file.</p>
 */
final class BeancountConnection implements Connection {

    private final Path ledgerPath;
    private final Properties clientInfo;
    private final List<LedgerEntry> ledgerEntries;
    private final List<Object[]> entryRows;
    private final List<Object[]> transactionsDetailRows;
    private final List<Object[]> transactionsViewRows;
    private final List<Object[]> openDetailRows;
    private final List<Object[]> openViewRows;
    private final List<Object[]> closeDetailRows;
    private final List<Object[]> closeViewRows;
    private final List<Object[]> padDetailRows;
    private final List<Object[]> padViewRows;
    private final List<Object[]> postingsRows;
    private final List<Object[]> balanceDetailRows;
    private final List<Object[]> balanceViewRows;
    private final List<Object[]> noteDetailRows;
    private final List<Object[]> noteViewRows;
    private final List<Object[]> documentDetailRows;
    private final List<Object[]> documentViewRows;
    private final List<Object[]> eventDetailRows;
    private final List<Object[]> eventViewRows;
    private final List<Object[]> queryDetailRows;
    private final List<Object[]> queryViewRows;
    private final List<Object[]> priceDetailRows;
    private final List<Object[]> priceViewRows;
    private final List<LoaderMessage> loaderMessages;
    private final SemanticLedger semanticLedger;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile boolean readOnly = true;
    private volatile DatabaseMetaData metadata;

    BeancountConnection(Path ledgerPath, Properties properties) throws SQLException {
        this.ledgerPath = Objects.requireNonNull(ledgerPath, "ledgerPath");
        this.clientInfo = new Properties();
        if (properties != null) {
            this.clientInfo.putAll(properties);
        }
        LoaderResult loaderResult;
        try {
            loaderResult = LedgerProvider.load(ledgerPath);
        } catch (LoaderException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
        this.loaderMessages = List.copyOf(loaderResult.getMessages());
        this.semanticLedger = loaderResult.getSemanticLedger();
        LedgerData ledgerData = loaderResult.getLedgerData();
        this.ledgerEntries = List.copyOf(ledgerData.getEntries());
        List<Object[]> entryRowsMutable = EntryTable.materializeRows(ledgerEntries);
        this.entryRows = List.copyOf(entryRowsMutable);
        this.transactionsDetailRows = List.copyOf(TransactionsDetailTable.materializeRows(ledgerEntries));
        this.transactionsViewRows = List.copyOf(TransactionsView.materializeRows(ledgerEntries));
        List<Object[]> openDetailMutable = OpenTable.materializeDetailRows(ledgerData.getOpens());
        this.openDetailRows = List.copyOf(openDetailMutable);
        this.openViewRows = List.copyOf(OpenTable.materializeViewRows(entryRowsMutable, openDetailMutable));
        List<Object[]> closeDetailMutable = CloseTable.materializeDetailRows(ledgerData.getCloses());
        this.closeDetailRows = List.copyOf(closeDetailMutable);
        this.closeViewRows = List.copyOf(CloseTable.materializeViewRows(entryRowsMutable, closeDetailMutable));
        List<Object[]> padDetailMutable = PadTable.materializeDetailRows(ledgerData.getPads());
        this.padDetailRows = List.copyOf(padDetailMutable);
        this.padViewRows = List.copyOf(PadTable.materializeViewRows(entryRowsMutable, padDetailMutable));
        List<Object[]> balanceDetailMutable = BalanceTable.materializeDetailRows(ledgerData.getBalances());
        this.balanceDetailRows = List.copyOf(balanceDetailMutable);
        this.balanceViewRows = List.copyOf(BalanceTable.materializeViewRows(entryRowsMutable, balanceDetailMutable));
        List<Object[]> noteDetailMutable = NoteTable.materializeDetailRows(ledgerData.getNotes());
        this.noteDetailRows = List.copyOf(noteDetailMutable);
        this.noteViewRows = List.copyOf(NoteTable.materializeViewRows(entryRowsMutable, noteDetailMutable));
        List<Object[]> documentDetailMutable = DocumentTable.materializeDetailRows(ledgerData.getDocuments());
        this.documentDetailRows = List.copyOf(documentDetailMutable);
        this.documentViewRows = List.copyOf(DocumentTable.materializeViewRows(entryRowsMutable, documentDetailMutable));
        List<Object[]> eventDetailMutable = EventTable.materializeDetailRows(ledgerData.getEvents());
        this.eventDetailRows = List.copyOf(eventDetailMutable);
        this.eventViewRows = List.copyOf(EventTable.materializeViewRows(entryRowsMutable, eventDetailMutable));
        List<Object[]> queryDetailMutable = QueryTable.materializeDetailRows(ledgerData.getQueries());
        this.queryDetailRows = List.copyOf(queryDetailMutable);
        this.queryViewRows = List.copyOf(QueryTable.materializeViewRows(entryRowsMutable, queryDetailMutable));
        List<Object[]> priceDetailMutable = PriceTable.materializeDetailRows(ledgerData.getPrices());
        this.priceDetailRows = List.copyOf(priceDetailMutable);
        this.priceViewRows = List.copyOf(PriceTable.materializeViewRows(entryRowsMutable, priceDetailMutable));
        this.postingsRows = List.copyOf(PostingsTable.materializeRows(ledgerData.getRawPostings()));
    }

    Path getLedgerPath() {
        return ledgerPath;
    }

    List<LoaderMessage> getLoaderMessages() {
        return loaderMessages;
    }

    List<Object[]> getEntryRows() {
        return entryRows;
    }

    List<Object[]> getTransactionsDetailRows() {
        return transactionsDetailRows;
    }

    List<Object[]> getTransactionsViewRows() {
        return transactionsViewRows;
    }

    SemanticLedger getSemanticLedger() {
        return semanticLedger;
    }

    List<Object[]> getOpenDetailRows() {
        return openDetailRows;
    }

    List<Object[]> getOpenViewRows() {
        return openViewRows;
    }

    List<Object[]> getCloseDetailRows() {
        return closeDetailRows;
    }

    List<Object[]> getCloseViewRows() {
        return closeViewRows;
    }

    List<Object[]> getPadDetailRows() {
        return padDetailRows;
    }

    List<Object[]> getPadViewRows() {
        return padViewRows;
    }

    List<Object[]> getPostingsRows() {
        return postingsRows;
    }

    List<Object[]> getNoteDetailRows() {
        return noteDetailRows;
    }

    List<Object[]> getNoteViewRows() {
        return noteViewRows;
    }

    List<Object[]> getDocumentDetailRows() {
        return documentDetailRows;
    }

    List<Object[]> getDocumentViewRows() {
        return documentViewRows;
    }

    List<Object[]> getEventDetailRows() {
        return eventDetailRows;
    }

    List<Object[]> getEventViewRows() {
        return eventViewRows;
    }

    List<Object[]> getQueryDetailRows() {
        return queryDetailRows;
    }

    List<Object[]> getQueryViewRows() {
        return queryViewRows;
    }

    List<Object[]> getPriceDetailRows() {
        return priceDetailRows;
    }

    List<Object[]> getPriceViewRows() {
        return priceViewRows;
    }

    List<Object[]> getBalanceDetailRows() {
        return balanceDetailRows;
    }

    List<Object[]> getBalanceViewRows() {
        return balanceViewRows;
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed.");
        }
        return new BeancountStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw unsupported("prepareStatement");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw unsupported("prepareCall");
    }

    @Override
    public String nativeSQL(String sql) {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (!autoCommit) {
            throw unsupported("Transactions are not supported yet.");
        }
    }

    @Override
    public boolean getAutoCommit() {
        return true;
    }

    @Override
    public void commit() throws SQLException {
        throw unsupported("commit");
    }

    @Override
    public void rollback() throws SQLException {
        throw unsupported("rollback");
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed.");
        }
        DatabaseMetaData local = metadata;
        if (local == null) {
            local =
                    (DatabaseMetaData)
                            Proxy.newProxyInstance(
                                    DatabaseMetaData.class.getClassLoader(),
                                    new Class<?>[] {DatabaseMetaData.class},
                                    new MetadataHandler(this));
            metadata = local;
        }
        return local;
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw unsupported("setCatalog");
    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw unsupported("setTransactionIsolation");
    }

    @Override
    public int getTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
        // Nothing to clear.
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw unsupported("createStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw unsupported("prepareStatement");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw unsupported("prepareCall");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw unsupported("getTypeMap");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw unsupported("setTypeMap");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw unsupported("Only CLOSE_CURSORS_AT_COMMIT is supported.");
        }
    }

    @Override
    public int getHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw unsupported("setSavepoint");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw unsupported("setSavepoint");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw unsupported("rollback");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw unsupported("releaseSavepoint");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw unsupported("createStatement");
    }

    @Override
    public PreparedStatement prepareStatement(
            String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw unsupported("prepareStatement");
    }

    @Override
    public CallableStatement prepareCall(
            String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw unsupported("prepareCall");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw unsupported("prepareStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw unsupported("prepareStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw unsupported("prepareStatement");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw unsupported("createClob");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw unsupported("createBlob");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw unsupported("createNClob");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw unsupported("createSQLXML");
    }

    @Override
    public boolean isValid(int timeout) {
        return !isClosed();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        clientInfo.setProperty(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        clientInfo.clear();
        if (properties != null) {
            clientInfo.putAll(properties);
        }
    }

    @Override
    public String getClientInfo(String name) {
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() {
        Properties copy = new Properties();
        copy.putAll(clientInfo);
        return copy;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw unsupported("createArrayOf");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw unsupported("createStruct");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw unsupported("setSchema");
    }

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw unsupported("setNetworkTimeout");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw unsupported("getNetworkTimeout");
    }

    @Override
    public void beginRequest() throws SQLException {
        throw unsupported("beginRequest");
    }

    @Override
    public void endRequest() throws SQLException {
        throw unsupported("endRequest");
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey, int timeout)
            throws SQLException {
        throw unsupported("setShardingKeyIfValid");
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout)
            throws SQLException {
        throw unsupported("setShardingKeyIfValid");
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey)
            throws SQLException {
        throw unsupported("setShardingKey");
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey) throws SQLException {
        throw unsupported("setShardingKey");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw unsupported("unwrap");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    @Override
    public String toString() {
        return "BeancountConnection[" + ledgerPath + "]";
    }

    private SQLFeatureNotSupportedException unsupported(String feature) {
        return new SQLFeatureNotSupportedException(feature + " not supported in alpha driver.");
    }

    private static final class MetadataHandler implements InvocationHandler {

        private static final List<TableDefinition> TABLE_DEFINITIONS =
                List.of(
                        EntryTable.getDefinition(),
                        TransactionsDetailTable.getDefinition(),
                        TransactionsView.getDefinition(),
                        OpenTable.getDetailDefinition(),
                        OpenTable.getViewDefinition(),
                        CloseTable.getDetailDefinition(),
                        CloseTable.getViewDefinition(),
                        PadTable.getDetailDefinition(),
                        PadTable.getViewDefinition(),
                        BalanceTable.getDetailDefinition(),
                        BalanceTable.getViewDefinition(),
                        NoteTable.getDetailDefinition(),
                        NoteTable.getViewDefinition(),
                        DocumentTable.getDetailDefinition(),
                        DocumentTable.getViewDefinition(),
                        EventTable.getDetailDefinition(),
                        EventTable.getViewDefinition(),
                        QueryTable.getDetailDefinition(),
                        QueryTable.getViewDefinition(),
                        PriceTable.getDetailDefinition(),
                        PriceTable.getViewDefinition(),
                        PostingsTable.getDefinition());

        private static final List<ColumnDescriptor> TABLES_COLUMNS =
                List.of(
                        new ColumnDescriptor("TABLE_CAT", Types.VARCHAR, "VARCHAR", 128, 0, true, String.class.getName()),
                        new ColumnDescriptor("TABLE_SCHEM", Types.VARCHAR, "VARCHAR", 128, 0, true, String.class.getName()),
                        new ColumnDescriptor("TABLE_NAME", Types.VARCHAR, "VARCHAR", 128, 0, false, String.class.getName()),
                        new ColumnDescriptor("TABLE_TYPE", Types.VARCHAR, "VARCHAR", 32, 0, false, String.class.getName()),
                        new ColumnDescriptor("REMARKS", Types.VARCHAR, "VARCHAR", 256, 0, true, String.class.getName()),
                        new ColumnDescriptor("TYPE_CAT", Types.VARCHAR, "VARCHAR", 128, 0, true, String.class.getName()),
                        new ColumnDescriptor("TYPE_SCHEM", Types.VARCHAR, "VARCHAR", 128, 0, true, String.class.getName()),
                        new ColumnDescriptor("TYPE_NAME", Types.VARCHAR, "VARCHAR", 128, 0, true, String.class.getName()),
                        new ColumnDescriptor("SELF_REFERENCING_COL_NAME", Types.VARCHAR, "VARCHAR", 128, 0, true, String.class.getName()),
                        new ColumnDescriptor("REF_GENERATION", Types.VARCHAR, "VARCHAR", 32, 0, true, String.class.getName()));

        private static final List<ColumnDescriptor> TABLE_TYPES_COLUMNS =
                List.of(
                        new ColumnDescriptor("TABLE_TYPE", Types.VARCHAR, "VARCHAR", 32, 0, false, String.class.getName()));

        private static final List<ColumnDescriptor> COLUMNS_COLUMNS =
                List.of(
                        new ColumnDescriptor("TABLE_CAT", Types.VARCHAR, "VARCHAR", 128, 0, true, String.class.getName()),
                        new ColumnDescriptor("TABLE_SCHEM", Types.VARCHAR, "VARCHAR", 128, 0, true, String.class.getName()),
                        new ColumnDescriptor("TABLE_NAME", Types.VARCHAR, "VARCHAR", 128, 0, false, String.class.getName()),
                        new ColumnDescriptor("COLUMN_NAME", Types.VARCHAR, "VARCHAR", 128, 0, false, String.class.getName()),
                        new ColumnDescriptor("DATA_TYPE", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()),
                        new ColumnDescriptor("TYPE_NAME", Types.VARCHAR, "VARCHAR", 128, 0, false, String.class.getName()),
                        new ColumnDescriptor("COLUMN_SIZE", Types.INTEGER, "INTEGER", 10, 0, true, Integer.class.getName()),
                        new ColumnDescriptor("DECIMAL_DIGITS", Types.INTEGER, "INTEGER", 10, 0, true, Integer.class.getName()),
                        new ColumnDescriptor("NUM_PREC_RADIX", Types.INTEGER, "INTEGER", 10, 0, true, Integer.class.getName()),
                        new ColumnDescriptor("NULLABLE", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()),
                        new ColumnDescriptor("REMARKS", Types.VARCHAR, "VARCHAR", 256, 0, true, String.class.getName()),
                        new ColumnDescriptor("COLUMN_DEF", Types.VARCHAR, "VARCHAR", 256, 0, true, String.class.getName()),
                        new ColumnDescriptor("SQL_DATA_TYPE", Types.INTEGER, "INTEGER", 10, 0, true, Integer.class.getName()),
                        new ColumnDescriptor("SQL_DATETIME_SUB", Types.INTEGER, "INTEGER", 10, 0, true, Integer.class.getName()),
                        new ColumnDescriptor("CHAR_OCTET_LENGTH", Types.INTEGER, "INTEGER", 10, 0, true, Integer.class.getName()),
                        new ColumnDescriptor("ORDINAL_POSITION", Types.INTEGER, "INTEGER", 10, 0, false, Integer.class.getName()),
                        new ColumnDescriptor("IS_NULLABLE", Types.VARCHAR, "VARCHAR", 3, 0, false, String.class.getName()),
                        new ColumnDescriptor("SCOPE_CATALOG", Types.VARCHAR, "VARCHAR", 128, 0, true, String.class.getName()),
                        new ColumnDescriptor("SCOPE_SCHEMA", Types.VARCHAR, "VARCHAR", 128, 0, true, String.class.getName()),
                        new ColumnDescriptor("SCOPE_TABLE", Types.VARCHAR, "VARCHAR", 128, 0, true, String.class.getName()),
                        new ColumnDescriptor("SOURCE_DATA_TYPE", Types.INTEGER, "INTEGER", 10, 0, true, Integer.class.getName()),
                        new ColumnDescriptor("IS_AUTOINCREMENT", Types.VARCHAR, "VARCHAR", 3, 0, false, String.class.getName()),
                        new ColumnDescriptor("IS_GENERATEDCOLUMN", Types.VARCHAR, "VARCHAR", 3, 0, false, String.class.getName()));

        private final BeancountConnection connection;

        MetadataHandler(BeancountConnection connection) {
            this.connection = connection;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();

            if (method.getDeclaringClass() == Object.class) {
                switch (name) {
                    case "toString":
                        return "BeancountDatabaseMetaData[" + connection.ledgerPath + "]";
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "equals":
                        return proxy == args[0];
                    default:
                        return method.invoke(this, args);
                }
            }

            switch (name) {
                case "getConnection":
                    return connection;
                case "getURL":
                    return BeancountDriver.URL_PREFIX + connection.ledgerPath;
                case "getUserName":
                    return null;
                case "getDatabaseProductName":
                    return "Beancount";
                case "getDatabaseProductVersion":
                    return Version.RUNTIME;
                case "getDriverName":
                    return "Beancount JDBC Driver (alpha)";
                case "getDriverVersion":
                    return Version.RUNTIME;
                case "getDriverMajorVersion":
                    return Version.MAJOR;
                case "getDriverMinorVersion":
                    return Version.MINOR;
                case "getDatabaseMajorVersion":
                    return Version.MAJOR;
                case "getDatabaseMinorVersion":
                    return Version.MINOR;
                case "getJDBCMajorVersion":
                    return 4;
                case "getJDBCMinorVersion":
                    return 0;
                case "getIdentifierQuoteString":
                    return "\"";
                case "isReadOnly":
                    return true;
                case "getDefaultTransactionIsolation":
                    return Connection.TRANSACTION_NONE;
                case "supportsTransactions":
                case "supportsBatchUpdates":
                case "supportsCatalogsInDataManipulation":
                case "supportsSchemasInDataManipulation":
                case "supportsStoredProcedures":
                case "supportsMultipleResultSets":
                case "supportsSubqueriesInComparisons":
                case "supportsUnion":
                case "supportsUnionAll":
                case "supportsSavepoints":
                case "supportsNamedParameters":
                    return false;
                case "supportsResultSetType":
                case "supportsResultSetConcurrency":
                case "supportsResultSetHoldability":
                case "supportsTransactionIsolationLevel":
                    return false;
                case "supportsMixedCaseIdentifiers":
                case "supportsMixedCaseQuotedIdentifiers":
                    return true;
                case "getTableTypes":
                    return tableTypesResult();
                case "getTables":
                    return tablesResult(
                            (String) args[0],
                            (String) args[1],
                            (String) args[2],
                            (String[]) args[3]);
                case "getColumns":
                    return columnsResult(
                            (String) args[0],
                            (String) args[1],
                            (String) args[2],
                            (String) args[3]);
                case "unwrap":
                    Class<?> iface = (Class<?>) args[0];
                    if (iface.isInstance(proxy) || iface == DatabaseMetaData.class) {
                        return iface.cast(proxy);
                    }
                    throw new SQLFeatureNotSupportedException("Cannot unwrap to " + iface.getName());
                case "isWrapperFor":
                    return ((Class<?>) args[0]).isInstance(proxy) || args[0] == DatabaseMetaData.class;
                default:
                    break;
            }

            Class<?> returnType = method.getReturnType();
            if (ResultSet.class.isAssignableFrom(returnType)) {
                throw new SQLFeatureNotSupportedException(name + " not supported in alpha driver.");
            }
            if (returnType == boolean.class) {
                return false;
            }
            if (returnType == int.class) {
                return 0;
            }
            if (returnType == long.class) {
                return 0L;
            }
            if (returnType == short.class) {
                return (short) 0;
            }
            if (returnType == byte.class) {
                return (byte) 0;
            }
            if (returnType == float.class) {
                return 0f;
            }
            if (returnType == double.class) {
                return 0d;
            }
            if (returnType == String.class) {
                return "";
            }
            return null;
        }

        private ResultSet tableTypesResult() {
            List<Object[]> rows = new ArrayList<>();
            Set<String> types = new LinkedHashSet<>();
            for (TableDefinition definition : TABLE_DEFINITIONS) {
                types.add(definition.getType());
            }
            for (String type : types) {
                rows.add(new Object[] {type});
            }
            return SimpleResultSet.from(TABLE_TYPES_COLUMNS, rows);
        }

        private ResultSet tablesResult(
                String catalog, String schemaPattern, String tableNamePattern, String[] types) {
            List<Object[]> rows = new ArrayList<>();
            for (TableDefinition definition : TABLE_DEFINITIONS) {
                if (!matchesPattern(definition.getName(), tableNamePattern)) {
                    continue;
                }
                if (!matchesType(definition.getType(), types)) {
                    continue;
                }
                rows.add(
                        new Object[] {
                            null,
                            null,
                            definition.getName(),
                            definition.getType(),
                            definition.getRemarks(),
                            null,
                            null,
                            null,
                            null,
                            null
                        });
            }
            return SimpleResultSet.from(TABLES_COLUMNS, rows);
        }

        private ResultSet columnsResult(
                String catalog,
                String schemaPattern,
                String tableNamePattern,
                String columnNamePattern) {
            List<Object[]> rows = new ArrayList<>();
            for (TableDefinition definition : TABLE_DEFINITIONS) {
                if (!matchesPattern(definition.getName(), tableNamePattern)) {
                    continue;
                }
                List<ColumnDescriptor> columns = definition.getColumns();
                for (int index = 0; index < columns.size(); index++) {
                    ColumnDescriptor column = columns.get(index);
                    if (!matchesPattern(column.getName(), columnNamePattern)) {
                        continue;
                    }
                    rows.add(
                            new Object[] {
                                null,
                                null,
                                definition.getName(),
                                column.getName(),
                                column.getJdbcType(),
                                column.getTypeName(),
                                column.getSize(),
                                column.getScale(),
                                10,
                                column.isNullable()
                                        ? DatabaseMetaData.columnNullable
                                        : DatabaseMetaData.columnNoNulls,
                                null,
                                null,
                                0,
                                0,
                                column.getJdbcType() == Types.VARCHAR ? column.getSize() : null,
                                index + 1,
                                column.isNullable() ? "YES" : "NO",
                                null,
                                null,
                                null,
                                null,
                                "NO",
                                "NO"
                            });
                }
            }
            return SimpleResultSet.from(COLUMNS_COLUMNS, rows);
        }

        private static boolean matchesType(String tableType, String[] types) {
            if (types == null || types.length == 0) {
                return true;
            }
            for (String type : types) {
                if (type != null && type.equalsIgnoreCase(tableType)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean matchesPattern(String value, String pattern) {
            if (value == null) {
                return false;
            }
            if (pattern == null) {
                return true;
            }
            String regex = sqlPatternToRegex(pattern);
            return value.toUpperCase(Locale.ROOT).matches(regex);
        }

        private static String sqlPatternToRegex(String pattern) {
            StringBuilder builder = new StringBuilder("^");
            String upperPattern = pattern.toUpperCase(Locale.ROOT);
            for (int i = 0; i < upperPattern.length(); i++) {
                char ch = upperPattern.charAt(i);
                switch (ch) {
                    case '%':
                        builder.append(".*");
                        break;
                    case '_':
                        builder.append('.');
                        break;
                    default:
                        builder.append("\\Q").append(ch).append("\\E");
                        break;
                }
            }
            builder.append('$');
            return builder.toString();
        }
    }
}
