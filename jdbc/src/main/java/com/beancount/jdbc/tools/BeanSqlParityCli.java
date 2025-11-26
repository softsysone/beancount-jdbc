package com.beancount.jdbc.tools;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lightweight CLI parity checker that mirrors {@code BeanSqlParityTest} so we can diff Calcite
 * output against the bean-sql baseline without running Gradle.
 */
public final class BeanSqlParityCli {

    private record TableSpec(String name, String orderColumn) {}

    private static final List<TableSpec> TABLES =
            List.of(
                    new TableSpec("entry", "id"),
                    new TableSpec("transactions_detail", "id"),
                    new TableSpec("transactions", "id"),
                    new TableSpec("postings", "posting_id"),
                    new TableSpec("open_detail", "id"),
                    new TableSpec("close_detail", "id"),
                    new TableSpec("pad_detail", "id"),
                    new TableSpec("balance_detail", "id"));

    private static final Set<String> ENTRY_TYPE_EXCLUSIONS = Set.of("event", "query");

    private BeanSqlParityCli() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: BeanSqlParityCli <ledger.beancount> <baseline.sqlite>");
            System.exit(1);
        }
        Path ledger = Path.of(args[0]).toAbsolutePath().normalize();
        Path baseline = Path.of(args[1]).toAbsolutePath().normalize();
        if (!Files.exists(ledger)) {
            throw new IllegalStateException("Ledger file not found: " + ledger);
        }
        if (!Files.exists(baseline)) {
            throw new IllegalStateException("Baseline sqlite file not found: " + baseline);
        }
        Class.forName("org.sqlite.JDBC");
        Class.forName("com.beancount.jdbc.BeancountDriver");
        String caseName = ledger.getFileName().toString();
        compareLedger(caseName, ledger, baseline);
        System.out.println("Bean-sql parity succeeded for " + caseName);
    }

    private static void compareLedger(String caseName, Path ledger, Path baseline)
            throws SQLException {
        String beanSqlUrl = "jdbc:sqlite:" + baseline;
        String calciteUrl = "jdbc:beancount:" + ledger.toUri();
        try (Connection beanSql = DriverManager.getConnection(beanSqlUrl);
                Connection calcite = DriverManager.getConnection(calciteUrl)) {
            Map<String, String> beanEntryKeys = new LinkedHashMap<>();
            Map<String, String> calciteEntryKeys = new LinkedHashMap<>();
            Set<String> canonicalEntryKeys = new LinkedHashSet<>();
            for (TableSpec table : TABLES) {
                if (!tableExists(beanSql, table.name())) {
                    System.out.println("Skipping table absent in bean-sql baseline: " + table.name());
                    continue;
                }
                if (!tableExists(calcite, table.name())) {
                    throw new IllegalStateException("Calcite missing expected table: " + table.name());
                }
                List<Map<String, String>> beanRowsRaw = fetchRows(beanSql, table);
                List<Map<String, String>> calciteRowsRaw = fetchRows(calcite, table);
                if ("entry".equals(table.name())) {
                    beanEntryKeys = buildEntryKeyMap(beanRowsRaw);
                    canonicalEntryKeys = new LinkedHashSet<>(beanEntryKeys.values());
                    calciteEntryKeys = buildEntryKeyMap(calciteRowsRaw);
                }
                List<Map<String, String>> beanRows =
                        normalizeRows(
                                table.name(), beanRowsRaw, beanEntryKeys, canonicalEntryKeys, true);
                List<Map<String, String>> calciteRows =
                        normalizeRows(
                                table.name(), calciteRowsRaw, calciteEntryKeys, canonicalEntryKeys, false);
                assertTableEquals(caseName, table.name(), beanRows, calciteRows);
            }
        }
    }

    private static boolean tableExists(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tables =
                metaData.getTables(null, null, tableName, new String[] {"TABLE", "VIEW"})) {
            return tables.next();
        }
    }

    private static List<Map<String, String>> fetchRows(Connection connection, TableSpec table)
            throws SQLException {
        List<Map<String, String>> rows = new ArrayList<>();
        String sql = "SELECT * FROM " + table.name() + " ORDER BY " + table.orderColumn();
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, String> row = new LinkedHashMap<>();
                for (int column = 1; column <= columnCount; column++) {
                    String name = metaData.getColumnLabel(column);
                    String value = normalize(resultSet.getObject(column));
                    if ("source_filename".equals(name)) {
                        value = normalizeFilename(value);
                    }
                    row.put(name, value);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private static void assertTableEquals(
            String caseName,
            String table,
            List<Map<String, String>> beanRows,
            List<Map<String, String>> calciteRows) {
        if (beanRows.size() != calciteRows.size()) {
            throw new IllegalStateException(
                    buildRowCountMismatchMessage(caseName, table, beanRows, calciteRows));
        }
        for (int i = 0; i < beanRows.size(); i++) {
            int rowNumber = i + 1;
            Map<String, String> expected = beanRows.get(i);
            Map<String, String> actual = calciteRows.get(i);
            if (!expected.keySet().equals(actual.keySet())) {
                throw new IllegalStateException(
                        "%s/%s column order mismatch at row %d"
                                .formatted(caseName, table, rowNumber));
            }
            for (String column : expected.keySet()) {
                String expectedValue = expected.get(column);
                String actualValue = actual.get(column);
                if (!java.util.Objects.equals(expectedValue, actualValue)) {
                    String entryKey =
                            expected.getOrDefault(
                                    "entry_key", actual.getOrDefault("entry_key", "<unknown>"));
                    throw new IllegalStateException(
                            "%s/%s mismatch at row %d column %s (entry=%s): bean=%s calcite=%s"
                                    .formatted(
                                            caseName,
                                            table,
                                            rowNumber,
                                            column,
                                            entryKey,
                                            expectedValue,
                                            actualValue));
                }
            }
        }
    }

    private static String buildRowCountMismatchMessage(
            String caseName,
            String table,
            List<Map<String, String>> beanRows,
            List<Map<String, String>> calciteRows) {
        List<String> beanOnly = rowDiff(beanRows, calciteRows, 5);
        List<String> calciteOnly = rowDiff(calciteRows, beanRows, 5);
        return "%s/%s row count mismatch: bean=%d calcite=%d | bean-only=%s | calcite-only=%s"
                .formatted(
                        caseName,
                        table,
                        beanRows.size(),
                        calciteRows.size(),
                        beanOnly,
                        calciteOnly);
    }

    private static List<String> rowDiff(
            List<Map<String, String>> left, List<Map<String, String>> right, int limit) {
        Map<String, Integer> rightCounts = new LinkedHashMap<>();
        for (Map<String, String> row : right) {
            String signature = serializeRow(row);
            rightCounts.merge(signature, 1, Integer::sum);
        }
        List<String> missing = new ArrayList<>();
        for (Map<String, String> row : left) {
            String signature = serializeRow(row);
            int count = rightCounts.getOrDefault(signature, 0);
            if (count == 0) {
                missing.add(signature);
                if (missing.size() >= limit) {
                    break;
                }
            } else {
                rightCounts.put(signature, count - 1);
            }
        }
        return missing;
    }

    private static String serializeRow(Map<String, String> row) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : row.entrySet()) {
            if (builder.length() > 0) {
                builder.append(" | ");
            }
            builder.append(entry.getKey()).append("=").append(entry.getValue());
        }
        return builder.toString();
    }

    private static List<Map<String, String>> normalizeRows(
            String table,
            List<Map<String, String>> rows,
            Map<String, String> idToKey,
            Set<String> canonicalKeys,
            boolean isBean) {
        if ("entry".equals(table)) {
            List<Map<String, String>> normalized = new ArrayList<>(rows.size());
            for (Map<String, String> row : rows) {
                Map<String, String> copy = new LinkedHashMap<>(row);
                String key = entryKey(copy);
                if (!canonicalKeys.contains(key)
                        || ENTRY_TYPE_EXCLUSIONS.contains(row.getOrDefault("type", ""))) {
                    continue;
                }
                if (copy.containsKey("source_filename")) {
                    copy.put("source_filename", normalizeFilename(copy.get("source_filename")));
                }
                normalized.add(copy);
            }
            normalized.sort(
                    Comparator.comparing(
                                    (Map<String, String> row) ->
                                            row.getOrDefault("source_filename", ""))
                            .thenComparing(row -> parseInt(row.get("source_lineno")))
                            .thenComparing(row -> row.getOrDefault("date", ""))
                            .thenComparing(row -> row.getOrDefault("type", "")));
            return normalized;
        }
        List<Map<String, String>> normalized = new ArrayList<>(rows.size());
        for (Map<String, String> row : rows) {
            Map<String, String> copy = new LinkedHashMap<>(row);
            if (copy.containsKey("id")) {
                String key = idToKey.get(copy.get("id"));
                if (key == null || !canonicalKeys.contains(key)) {
                    if (isBean) {
                        continue;
                    } else {
                        continue;
                    }
                }
                copy.put("entry_key", key);
                copy.remove("id");
            }
            if (copy.containsKey("source_filename")) {
                copy.put("source_filename", normalizeFilename(copy.get("source_filename")));
            }
            normalized.add(copy);
        }
        normalized.sort(
                Comparator.comparing((Map<String, String> row) -> entryKeyFilename(row.get("entry_key")))
                        .thenComparing(row -> entryKeyLine(row.get("entry_key")))
                        .thenComparing(row -> entryKeyType(row.get("entry_key")))
                        .thenComparing(Map::toString));
        return normalized;
    }

    private static Map<String, String> buildEntryKeyMap(List<Map<String, String>> rows) {
        Map<String, String> map = new LinkedHashMap<>();
        for (Map<String, String> row : rows) {
            map.put(row.get("id"), entryKey(row));
        }
        return map;
    }

    private static String entryKey(Map<String, String> row) {
        return normalizeFilename(row.get("source_filename"))
                + ":"
                + row.getOrDefault("source_lineno", "")
                + ":"
                + row.getOrDefault("type", "");
    }

    private static String entryKeyFilename(String key) {
        if (key == null) {
            return "";
        }
        int first = key.indexOf(':');
        return first >= 0 ? key.substring(0, first) : key;
    }

    private static int entryKeyLine(String key) {
        if (key == null) {
            return 0;
        }
        int first = key.indexOf(':');
        int second = key.indexOf(':', first + 1);
        if (first < 0 || second < 0) {
            return 0;
        }
        return parseInt(key.substring(first + 1, second));
    }

    private static String entryKeyType(String key) {
        if (key == null) {
            return "";
        }
        int second = key.lastIndexOf(':');
        return second >= 0 ? key.substring(second + 1) : "";
    }

    private static String normalize(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal bd) {
            return bd.stripTrailingZeros().toPlainString();
        }
        if (value instanceof BigInteger bi) {
            return bi.toString();
        }
        if (value instanceof Double || value instanceof Float) {
            double d = ((Number) value).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                return Double.toString(d);
            }
            return BigDecimal.valueOf(d).stripTrailingZeros().toPlainString();
        }
        if (value instanceof Number number) {
            return BigDecimal.valueOf(number.longValue()).toPlainString();
        }
        return value.toString();
    }

    private static String normalizeFilename(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }
        try {
            return Path.of(path).getFileName().toString();
        } catch (Exception ex) {
            int lastSlash = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
            return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
        }
    }

    private static int parseInt(String value) {
        try {
            return value == null ? 0 : Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return 0;
        }
    }
}
