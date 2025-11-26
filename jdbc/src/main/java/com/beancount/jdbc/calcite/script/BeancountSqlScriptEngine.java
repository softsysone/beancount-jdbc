package com.beancount.jdbc.calcite.script;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

/**
 * Minimal script parser for JDBC: uses Calcite's stmt-list parser so semicolons separate statements,
 * with an optional trailing semicolon. Each statement is unparsed back to SQL for execution.
 */
public final class BeancountSqlScriptEngine {

    private final CalciteConnectionConfig config;
    private final SqlDialect dialect;

    public BeancountSqlScriptEngine(CalciteConnectionConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.dialect = SqlDialect.DatabaseProduct.CALCITE.getDialect();
    }

    public List<ScriptStatement> parse(String sql) throws SQLException {
        Objects.requireNonNull(sql, "sql");
        try {
            SqlParser parser = SqlParser.create(sql, buildParserConfig());
            SqlNodeList stmtList = parser.parseStmtList();
            List<ScriptStatement> statements = new ArrayList<>(stmtList.size());
            for (SqlNode node : stmtList) {
                if (node == null) {
                    continue;
                }
                statements.add(
                        new ScriptStatement(node.toSqlString(dialect).getSql(), isQuery(node)));
            }
            if (statements.isEmpty()) {
                throw new SQLException("SQL script contained no statements.");
            }
            return statements;
        } catch (SqlParseException ex) {
            throw new SQLException("Failed to parse SQL script.", ex);
        }
    }

    private SqlParser.Config buildParserConfig() {
        SqlParser.ConfigBuilder builder = SqlParser.configBuilder();
        builder.setCaseSensitive(config.caseSensitive());
        builder.setQuotedCasing(config.quotedCasing());
        builder.setUnquotedCasing(config.unquotedCasing());
        builder.setQuoting(config.quoting());
        builder.setConformance(config.conformance());
        SqlParserImplFactory parserFactory =
                config.parserFactory(SqlParserImplFactory.class, SqlParserImpl.FACTORY);
        builder.setParserFactory(parserFactory);
        return builder.build();
    }

    private static boolean isQuery(SqlNode node) {
        SqlKind kind = node.getKind();
        // Covers SELECT/UNION/ORDER BY/VALUES and other query-producing kinds.
        return kind.belongsTo(SqlKind.QUERY);
    }

    public record ScriptStatement(String sql, boolean query) {}
}
