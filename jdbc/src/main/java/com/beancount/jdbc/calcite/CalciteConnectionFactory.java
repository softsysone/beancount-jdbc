package com.beancount.jdbc.calcite;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Helper for opening Calcite connections that are pre-wired with the Beancount schema.
 */
public final class CalciteConnectionFactory {

    private CalciteConnectionFactory() {}

    public static Connection connect(Path ledgerPath, Properties properties) throws SQLException {
        Objects.requireNonNull(ledgerPath, "ledgerPath");
        Properties calciteProps = new Properties();
        if (properties != null) {
            calciteProps.putAll(properties);
        }
        setDefault(calciteProps, "lex", "JAVA");
        setDefault(calciteProps, "quoting", "DOUBLE_QUOTE");
        setDefault(calciteProps, "quotedCasing", "UNCHANGED");
        setDefault(calciteProps, "unquotedCasing", "UNCHANGED");
        setDefault(calciteProps, "caseSensitive", "true");
        setDefault(
                calciteProps,
                "parserFactory",
                "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY");
        setDefault(calciteProps, "conformance", "BABEL");
        setDefault(calciteProps, "mutable", "true");

        Connection connection = DriverManager.getConnection("jdbc:calcite:", calciteProps);
        CalciteConnection calcite = connection.unwrap(CalciteConnection.class);
        SchemaPlus root = calcite.getRootSchema();
        root.add(
                "beancount",
                new BeancountSchema(
                        root,
                        "beancount",
                        Map.of("ledger", ledgerPath.toAbsolutePath().toString())));
        calcite.setSchema("beancount");
        return connection;
    }

    private static void setDefault(Properties properties, String key, String value) {
        if (!properties.containsKey(key)) {
            properties.setProperty(key, value);
        }
    }
}
