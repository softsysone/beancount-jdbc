package com.beancount.jdbc;

import com.beancount.jdbc.calcite.CalciteConnectionFactory;
import com.beancount.jdbc.calcite.script.BeancountSqlScriptEngine;
import com.beancount.jdbc.ledger.LedgerProvider;
import com.beancount.jdbc.loader.LoaderException;
import com.beancount.jdbc.loader.LoaderMessage;
import com.beancount.jdbc.loader.LoaderResult;
import com.beancount.jdbc.loader.semantic.SemanticAnalyzer;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.config.CalciteConnectionConfig;

/**
 * JDBC driver that opens Calcite-backed connections for Beancount ledgers.
 */
public final class BeancountDriver implements Driver {

    static final String URL_PREFIX = "jdbc:beancount:";
    private static final String METADATA_SCHEMA_NAME = "metadata";
    private static final String SYSTEM_TABLE_JDBC_NAME = Schema.TableType.SYSTEM_TABLE.jdbcName;
    private static final Logger LOGGER = Logger.getLogger(BeancountDriver.class.getName());

    static {
        enableCalciteDebugLogging();
        try {
            DriverManager.registerDriver(new BeancountDriver());
        } catch (SQLException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        ParsedUrl parsed = parseUrl(url);
        Properties properties = info == null ? new Properties() : new Properties(info);
        properties.putAll(parsed.properties);
        properties.setProperty("ledger", parsed.ledgerPath.toString());
        String mode = extractMode(properties);
        if ("deprecated".equals(mode)) {
            throw new SQLException("Legacy driver mode is not available in this build.");
        }
        LoaderResult loaderResult;
        try {
            loaderResult = LedgerProvider.load(parsed.ledgerPath);
        } catch (LoaderException ex) {
            throw new SQLException("Failed to load ledger: " + parsed.ledgerPath, ex);
        }
        Connection connection = CalciteConnectionFactory.connect(parsed.ledgerPath, properties);
        SQLWarning warnings = buildWarningChain(loaderResult, parsed.ledgerPath);
        logWarnings(loaderResult, parsed.ledgerPath);
        return wrapCalciteConnection(connection, warnings, hasPluginWarning(loaderResult));
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        DriverPropertyInfo ledgerProperty = new DriverPropertyInfo("ledger", null);
        ledgerProperty.required = true;
        ledgerProperty.description = "Absolute or relative path to the Beancount ledger file.";
        return new DriverPropertyInfo[] {ledgerProperty};
    }

    @Override
    public int getMajorVersion() {
        return Version.MAJOR;
    }

    @Override
    public int getMinorVersion() {
        return Version.MINOR;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Logging hierarchy not implemented.");
    }

    private static ParsedUrl parseUrl(String url) throws SQLException {
        String remainder = url.substring(URL_PREFIX.length());
        if (remainder.isEmpty()) {
            throw new SQLException("Ledger path missing from JDBC URL.");
        }

        String ledgerSegment = remainder;
        Properties props = new Properties();
        int paramIndex = remainder.indexOf('?');
        if (paramIndex >= 0) {
            ledgerSegment = remainder.substring(0, paramIndex);
            String query = remainder.substring(paramIndex + 1);
            for (String pair : query.split("&")) {
                if (pair.isEmpty()) {
                    continue;
                }
                int eq = pair.indexOf('=');
                String key;
                String value;
                if (eq >= 0) {
                    key = pair.substring(0, eq);
                    value = pair.substring(eq + 1);
                } else {
                    key = pair;
                    value = "";
                }
                props.setProperty(key, value);
            }
        }

        Path ledgerPath;
        if (ledgerSegment.startsWith("file:")) {
            try {
                ledgerPath = Paths.get(java.net.URI.create(ledgerSegment));
            } catch (IllegalArgumentException ex) {
                throw new SQLException("Invalid file URI in JDBC URL: " + ledgerSegment, ex);
            }
        } else {
            ledgerPath = Paths.get(ledgerSegment);
        }

        ledgerPath = ledgerPath.normalize();

        if (!Files.exists(ledgerPath)) {
            throw new SQLException("Ledger file not found: " + ledgerPath);
        }
        if (!Files.isReadable(ledgerPath)) {
            throw new SQLException("Ledger file is not readable: " + ledgerPath);
        }

        return new ParsedUrl(ledgerPath.toAbsolutePath(), props);
    }

    private static String extractMode(Properties properties) {
        Object mode = properties.get("mode");
        return mode != null ? mode.toString().toLowerCase(Locale.ROOT) : null;
    }

    private Connection wrapCalciteConnection(Connection delegate, SQLWarning warnings, boolean hasPluginWarning) {
        return (Connection)
                Proxy.newProxyInstance(
                        Connection.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        new DelegatingHandler(delegate) {
                            private final CalciteConnection calciteConnection = unwrapCalcite(delegate);
                            private SQLWarning localWarnings = warnings;
                            private final boolean pluginWarningPresent = hasPluginWarning;

                            @Override
                            Object handle(Object proxy, Method method, Object[] args) throws Throwable {
                                if ("getMetaData".equals(method.getName()) && args == null) {
                                    DatabaseMetaData meta =
                                            (DatabaseMetaData) method.invoke(delegate);
                                    return wrapCalciteMetaData(meta, pluginWarningPresent);
                                } else if ("createStatement".equals(method.getName())) {
                                    Statement stmt = (Statement) method.invoke(delegate, args);
                                    return wrapCalciteStatement(
                                            stmt, calciteConnection.config(), calciteConnection);
                                } else if ("getWarnings".equals(method.getName())) {
                                    return localWarnings;
                                } else if ("clearWarnings".equals(method.getName())) {
                                    localWarnings = null;
                                    return null;
                                }
                                return super.handle(proxy, method, args);
                            }
                        });
    }

    private SQLWarning buildWarningChain(LoaderResult loaderResult, Path ledgerPath) {
        if (loaderResult == null) {
            return null;
        }
        SQLWarning head = null;
        SQLWarning tail = null;
        for (LoaderMessage message : loaderResult.getMessages()) {
            if (message.getLevel() != LoaderMessage.Level.WARNING) {
                continue;
            }
            SQLWarning warning =
                    new SQLWarning(
                            "[Beancount JDBC] "
                                    + message.getMessage()
                                    + " ("
                                    + ledgerPath.getFileName()
                                    + ")");
            if (head == null) {
                head = warning;
                tail = warning;
            } else {
                tail.setNextWarning(warning);
                tail = warning;
            }
        }
        return head;
    }

    private void logWarnings(LoaderResult loaderResult, Path ledgerPath) {
        if (loaderResult == null) {
            return;
        }
        for (LoaderMessage message : loaderResult.getMessages()) {
            if (message.getLevel() != LoaderMessage.Level.WARNING) {
                continue;
            }
            LOGGER.log(
                    Level.WARNING,
                    "[Beancount JDBC] {0} ({1})",
                    new Object[] {message.getMessage(), ledgerPath.getFileName()});
        }
    }

    private boolean hasPluginWarning(LoaderResult loaderResult) {
        if (loaderResult == null) {
            return false;
        }
        for (LoaderMessage message : loaderResult.getMessages()) {
            if (message.getLevel() == LoaderMessage.Level.WARNING) {
                String msg = message.getMessage();
                if (msg != null && msg.startsWith("Plugin '")) {
                    return true;
                }
            }
        }
        return false;
    }

    private DatabaseMetaData wrapCalciteMetaData(DatabaseMetaData delegate, boolean pluginWarning) {
        return (DatabaseMetaData)
                Proxy.newProxyInstance(
                        DatabaseMetaData.class.getClassLoader(),
                        new Class<?>[] {DatabaseMetaData.class},
                        new DelegatingHandler(delegate) {
                            @Override
                            Object handle(Object proxy, Method method, Object[] args) throws Throwable {
                                return switch (method.getName()) {
                                    case "getDatabaseProductName" -> "Beancount";
                                    case "getDatabaseProductVersion",
                                            "getDriverVersion" -> Version.RUNTIME;
                                    case "getDriverName" -> {
                                        if (pluginWarning) {
                                            yield Version.RUNTIME
                                                    + " - WARNING: This driver does not support the Beancount plugins declared in this ledger, so results may be incorrect.";
                                        }
                                        yield "Beancount JDBC Driver (Calcite)";
                                    }
                                    case "getTables" -> method.invoke(delegate, adjustMetadataTableArgs(args));
                                    default -> super.handle(proxy, method, args);
                                };
                            }
                        });
    }

    private Statement wrapCalciteStatement(
            Statement delegate, CalciteConnectionConfig config, Connection owningConnection) {
        BeancountSqlScriptEngine scriptEngine = new BeancountSqlScriptEngine(config);
        return (Statement)
                Proxy.newProxyInstance(
                        Statement.class.getClassLoader(),
                        new Class<?>[] {Statement.class},
                        new DelegatingHandler(delegate) {
                            @Override
                            Object handle(Object proxy, Method method, Object[] args) throws Throwable {
                                if (isExecuteWithSql(method, args)) {
                                    return handleScriptExecution(delegate, method, args, scriptEngine);
                                } else if ("unwrap".equals(method.getName())
                                        && args != null
                                        && args.length == 1
                                        && args[0] == Connection.class) {
                                    return owningConnection;
                                }
                                return super.handle(proxy, method, args);
                            }
                        });
    }

    private boolean isExecuteWithSql(Method method, Object[] args) {
        if (args == null || args.length == 0 || !(args[0] instanceof String)) {
            return false;
        }
        return switch (method.getName()) {
            case "execute", "executeQuery", "executeUpdate", "executeLargeUpdate" -> true;
            default -> false;
        };
    }

    private Object handleScriptExecution(
            Statement delegate,
            Method method,
            Object[] args,
            BeancountSqlScriptEngine scriptEngine)
            throws Throwable {
        String rawSql = (String) args[0];
        java.util.List<BeancountSqlScriptEngine.ScriptStatement> statements =
                scriptEngine.parse(rawSql);
        if (statements.isEmpty()) {
            throw new SQLException("SQL script contained no statements.");
        }
        BeancountSqlScriptEngine.ScriptStatement last = statements.get(statements.size() - 1);
        // Execute all leading statements and discard their results.
        for (int i = 0; i < statements.size() - 1; i++) {
            BeancountSqlScriptEngine.ScriptStatement stmt = statements.get(i);
            boolean hasResult = delegate.execute(stmt.sql());
            if (hasResult) {
                try (ResultSet rs = delegate.getResultSet()) {
                    // Close immediately; content is intentionally ignored.
                }
            }
        }
        Object[] adjustedArgs = args.clone();
        adjustedArgs[0] = last.sql();
        return switch (method.getName()) {
            case "executeQuery" -> {
                if (!last.query()) {
                    throw new SQLException(
                            "executeQuery() requires the final statement to be a query.");
                }
                yield method.invoke(delegate, adjustedArgs);
            }
            case "executeUpdate", "executeLargeUpdate" -> {
                if (last.query()) {
                    throw new SQLException(
                            method.getName()
                                    + " cannot be used when the final statement is a query.");
                }
                yield method.invoke(delegate, adjustedArgs);
            }
            case "execute" -> method.invoke(delegate, adjustedArgs);
            default -> method.invoke(delegate, args);
        };
    }

    private record ParsedUrl(Path ledgerPath, Properties properties) {}

    private abstract static class DelegatingHandler implements InvocationHandler {
        private final Object delegate;

        DelegatingHandler(Object delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass() == Object.class) {
                return method.invoke(delegate, args);
            }
            return handle(proxy, method, args);
        }

        Object handle(Object proxy, Method method, Object[] args) throws Throwable {
            return method.invoke(delegate, args);
        }
    }

    private static Object[] adjustMetadataTableArgs(Object[] args) {
        if (args == null || args.length < 4) {
            return args;
        }
        if (!metadataSchemaRequested(args[1])) {
            return args;
        }
        String[] requestedTypes = (String[]) args[3];
        if (requestedTypes == null) {
            return args;
        }
        String[] augmentedTypes = includeSystemTableType(requestedTypes);
        if (augmentedTypes == requestedTypes) {
            return args;
        }
        Object[] adjusted = args.clone();
        adjusted[3] = augmentedTypes;
        return adjusted;
    }

    private static boolean metadataSchemaRequested(Object schemaPattern) {
        if (schemaPattern == null) {
            return true;
        }
        if (!(schemaPattern instanceof String pattern)) {
            return false;
        }
        String normalized = pattern.trim();
        if (normalized.isEmpty() || "%".equals(normalized)) {
            return true;
        }
        if (normalized.startsWith("\"") && normalized.endsWith("\"") && normalized.length() > 1) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        return METADATA_SCHEMA_NAME.equalsIgnoreCase(normalized);
    }

    private static String[] includeSystemTableType(String[] requestedTypes) {
        for (String type : requestedTypes) {
            if (type != null && SYSTEM_TABLE_JDBC_NAME.equalsIgnoreCase(type.trim())) {
                return requestedTypes;
            }
        }
        String[] augmented = Arrays.copyOf(requestedTypes, requestedTypes.length + 1);
        augmented[requestedTypes.length] = SYSTEM_TABLE_JDBC_NAME;
        return augmented;
    }

    private static CalciteConnection unwrapCalcite(Connection delegate) {
        try {
            return delegate.unwrap(CalciteConnection.class);
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to unwrap CalciteConnection", ex);
        }
    }

    private static void enableCalciteDebugLogging() {
        if (System.getProperty("calcite.debug") == null) {
            System.setProperty("calcite.debug", "true");
        }
        Logger calciteLogger = Logger.getLogger("org.apache.calcite");
        calciteLogger.setLevel(Level.FINE);
        boolean hasConsole = false;
        for (Handler handler : calciteLogger.getHandlers()) {
            if (handler instanceof ConsoleHandler console) {
                console.setLevel(Level.FINE);
                hasConsole = true;
            }
        }
        if (!hasConsole) {
            ConsoleHandler handler = new ConsoleHandler();
            handler.setLevel(Level.FINE);
            calciteLogger.addHandler(handler);
        }
    }
}
