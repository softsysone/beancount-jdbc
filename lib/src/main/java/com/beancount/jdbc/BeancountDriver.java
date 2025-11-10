package com.beancount.jdbc;

import com.beancount.jdbc.calcite.BeancountSchemaFactory;
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
import java.util.Locale;
import java.util.Properties;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.logging.Logger;

/**
 * Minimal JDBC driver that validates connectivity to a Beancount ledger file.
 *
 * <p>This alpha driver only checks that the provided URL points to an existing ledger file and does
 * not yet support executing SQL statements.</p>
 */
public final class BeancountDriver implements Driver {

    static final String URL_PREFIX = "jdbc:beancount:";

    static {
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
        Path ledgerPath = parsed.ledgerPath;
        String mode = extractMode(properties);
        if ("calcite".equalsIgnoreCase(mode)) {
            return createCalciteConnection(ledgerPath, properties);
        }
        return new BeancountConnection(ledgerPath, properties);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
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
        throw new SQLFeatureNotSupportedException("Logging hierarchy not implemented yet.");
    }

    private ParsedUrl parseUrl(String url) throws SQLException {
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


    private String extractMode(Properties properties) {
        Object mode = properties.get("mode");
        return mode != null ? mode.toString().toLowerCase(Locale.ROOT) : null;
    }

    private Connection createCalciteConnection(Path ledgerPath, Properties properties) throws SQLException {
        Properties calciteProps = new Properties();
        calciteProps.putAll(properties);
        calciteProps.setProperty("lex", "JAVA");
        calciteProps.setProperty(
                "model",
                String.format(
                        "inline:{\"version\":\"1.0\",\"defaultSchema\":\"beancount\",\"schemas\":[{\"name\":\"beancount\",\"type\":\"custom\",\"factory\":\"%s\",\"operand\":{\"ledger\":\"%s\"}}]}",
                        BeancountSchemaFactory.class.getName(), escapeJson(ledgerPath.toAbsolutePath().toString())));
        calciteProps.setProperty("conformance", "BABEL");
        Connection calcite = DriverManager.getConnection("jdbc:calcite:", calciteProps);
        return wrapCalciteConnection(calcite);
    }

    private record ParsedUrl(Path ledgerPath, Properties properties) {}

    private static String escapeJson(String value) {
        StringBuilder builder = new StringBuilder(value.length());
        for (char ch : value.toCharArray()) {
            if (ch == '\\' || ch == '"') {
                builder.append('\\');
            }
            builder.append(ch);
        }
        return builder.toString();
    }

    private Connection wrapCalciteConnection(Connection delegate) {
        return (Connection)
                Proxy.newProxyInstance(
                        Connection.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        new DelegatingHandler(delegate) {
                            @Override
                            Object handle(Object proxy, Method method, Object[] args) throws Throwable {
                                if ("getMetaData".equals(method.getName()) && args == null) {
                                    DatabaseMetaData meta =
                                            (DatabaseMetaData) method.invoke(delegate);
                                    return wrapCalciteMetaData(meta);
                                }
                                return super.handle(proxy, method, args);
                            }
                        });
    }

    private DatabaseMetaData wrapCalciteMetaData(DatabaseMetaData delegate) {
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
                                    case "getDriverName" -> "Beancount JDBC Driver (Calcite)";
                                    default -> super.handle(proxy, method, args);
                                };
                            }
                        });
    }

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
}
