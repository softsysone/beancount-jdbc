package com.beancount.jdbc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.Properties;
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
        Properties properties = info == null ? new Properties() : new Properties(info);
        Path ledgerPath = parseLedgerPath(url, properties);
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

    private Path parseLedgerPath(String url, Properties properties) throws SQLException {
        String remainder = url.substring(URL_PREFIX.length());
        if (remainder.isEmpty()) {
            throw new SQLException("Ledger path missing from JDBC URL.");
        }

        Path ledgerPath;
        if (remainder.startsWith("file:")) {
            try {
                ledgerPath = Paths.get(java.net.URI.create(remainder));
            } catch (IllegalArgumentException ex) {
                throw new SQLException("Invalid file URI in JDBC URL: " + remainder, ex);
            }
        } else {
            ledgerPath = Paths.get(remainder);
        }

        ledgerPath = ledgerPath.normalize();

        if (!Files.exists(ledgerPath)) {
            throw new SQLException("Ledger file not found: " + ledgerPath);
        }
        if (!Files.isReadable(ledgerPath)) {
            throw new SQLException("Ledger file is not readable: " + ledgerPath);
        }

        properties.setProperty("ledger", Objects.toString(ledgerPath.toAbsolutePath()));
        return ledgerPath;
    }
}
