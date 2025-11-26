package com.beancount.jdbc.calcite.sqlite;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.SqlDialectFactory;

/**
 * SQLite dialect that disables charset clauses so Calcite's JDBC adapter emits syntax accepted by
 * the stock SQLite engine.
 */
public final class BeancountSqliteDialectFactory implements SqlDialectFactory {

    @Override
    public SqlDialect create(DatabaseMetaData databaseMetaData) {
        try {
            SqlDialect.Context context =
                    SqlDialect.EMPTY_CONTEXT
                            .withDatabaseProduct(DatabaseProduct.UNKNOWN)
                            .withDatabaseProductName(databaseMetaData.getDatabaseProductName())
                            .withDatabaseVersion(databaseMetaData.getDatabaseProductVersion())
                            .withDatabaseMajorVersion(databaseMetaData.getDatabaseMajorVersion())
                            .withDatabaseMinorVersion(databaseMetaData.getDatabaseMinorVersion())
                            .withIdentifierQuoteString(sanitize(databaseMetaData.getIdentifierQuoteString()))
                            .withLiteralQuoteString("'")
                            .withLiteralEscapedQuoteString("''")
                            .withUnquotedCasing(Casing.UNCHANGED)
                            .withQuotedCasing(Casing.UNCHANGED)
                            .withCaseSensitive(databaseMetaData.storesMixedCaseIdentifiers())
                            .withNullCollation(NullCollation.HIGH);
            return new BeancountSqliteDialect(context);
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to inspect SQLite metadata", ex);
        }
    }

    private static final class BeancountSqliteDialect extends SqlDialect {
        BeancountSqliteDialect(SqlDialect.Context context) {
            super(context);
        }

        @Override
        public boolean supportsCharSet() {
            return false;
        }
    }

    private static String sanitize(String identifierQuote) {
        if (identifierQuote == null || identifierQuote.trim().isEmpty()) {
            return "\""; // SQLite accepts double quotes for identifiers.
        }
        return identifierQuote;
    }
}
