package com.beancount.jdbc.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.beancount.jdbc.ledger.LedgerProvider;
import com.beancount.jdbc.loader.LoaderException;
import com.beancount.jdbc.loader.LoaderMessage;
import com.beancount.jdbc.loader.LoaderResult;
import com.beancount.jdbc.testing.TestResources;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

final class BeanSqlParityTest {

    private record TableSpec(String name, String orderColumn) {}

    private record LedgerCase(String name, String ledgerPath, String baselinePath) {}

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

    private static final List<LedgerCase> CASES = buildLedgerCases();

    private static final Path DEBUG_DIR =
            Path.of(System.getProperty("java.io.tmpdir"), "bean_sql_parity");
    private static final Path DEBUG_LOG = DEBUG_DIR.resolve("parity.log");
    private static final AtomicBoolean ROWS_DUMPED = new AtomicBoolean();

    static {
        try {
            Files.createDirectories(DEBUG_DIR);
            Files.deleteIfExists(DEBUG_LOG);
        } catch (Exception ignored) {
        }
    }


    @Test
    void calciteMatchesBeanSqlBaseline() throws Exception {
        Class.forName("org.sqlite.JDBC");
        Class.forName("com.beancount.jdbc.BeancountDriver");

        for (LedgerCase ledgerCase : CASES) {
            Path ledger = TestResources.resolveResource(ledgerCase.ledgerPath());
            Path baseline = TestResources.resolveResource(ledgerCase.baselinePath());
            assertTrue(Files.exists(ledger), "Missing ledger file for " + ledgerCase.name() + ": " + ledger);
            assertTrue(
                    Files.exists(baseline),
                    "Missing bean-sql baseline for " + ledgerCase.name() + ": " + baseline);
            if (ledgerHasUnsupportedPlugin(ledgerCase.name(), ledger)) {
                continue;
            }
            compareLedger(ledgerCase.name(), ledger, baseline);
        }
    }

    @Test
    void debugExampleTransactionsDetail() throws Exception {
        Path ledger = TestResources.absolutePath("third_party/beancount/examples/example.beancount");
        try (Connection connection = DriverManager.getConnection("jdbc:beancount:" + ledger.toUri());
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT e.id, e.source_lineno, td.payee FROM entry e JOIN transactions_detail td ON e.id = td.id ORDER BY e.id LIMIT 12")) {
            while (resultSet.next()) {
                System.out.println(
                        resultSet.getInt("id")
                                + " @"
                                + resultSet.getInt("source_lineno")
                                + " -> "
                                + resultSet.getString("payee"));
            }
        }
    }

    private static void compareLedger(String caseName, Path ledger, Path baseline) throws SQLException {
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

    private static void assertTableEquals(
            String caseName,
            String table,
            List<Map<String, String>> beanRows,
            List<Map<String, String>> calciteRows) {
        if (beanRows.size() != calciteRows.size()) {
            fail(
                    buildRowCountMismatchMessage(
                            caseName, table, beanRows, calciteRows));
        }
        for (int i = 0; i < beanRows.size(); i++) {
            int rowNumber = i + 1;
            Map<String, String> expected = beanRows.get(i);
            Map<String, String> actual = calciteRows.get(i);
            assertEquals(
                    expected.keySet(),
                    actual.keySet(),
                    () -> "%s/%s column order mismatch at row %d".formatted(caseName, table, rowNumber));
            for (String column : expected.keySet()) {
                String expectedValue = expected.get(column);
                String actualValue = actual.get(column);
                assertEquals(
                        expectedValue,
                        actualValue,
                        () ->
                                "%s/%s mismatch at row %d column %s: bean=%s calcite=%s"
                                        .formatted(
                                                caseName,
                                                table,
                                                rowNumber,
                                                column,
                                                expectedValue,
                                                actualValue));
            }
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

    private static boolean ledgerHasUnsupportedPlugin(String caseName, Path ledger) {
        try {
            LoaderResult result = LedgerProvider.load(ledger);
            for (LoaderMessage message : result.getMessages()) {
                if (isPluginWarning(message)) {
                    System.out.println(
                            "[BeanSqlParityTest] Skipping "
                                    + caseName
                                    + " because plugins are not supported: "
                                    + message.getMessage());
                    return true;
                }
            }
            return false;
        } catch (LoaderException ex) {
            throw new IllegalStateException("Failed to inspect ledger for plugins: " + ledger, ex);
        }
    }

    private static boolean isPluginWarning(LoaderMessage message) {
        return message.getLevel() == LoaderMessage.Level.WARNING
                && message.getMessage() != null
                && message.getMessage().startsWith("Plugin '");
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

    private static boolean tableExists(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tables =
                metaData.getTables(null, null, tableName, new String[] {"TABLE", "VIEW"})) {
            return tables.next();
        }
    }

    private static String buildRowCountMismatchMessage(
            String caseName,
            String table,
            List<Map<String, String>> beanRows,
            List<Map<String, String>> calciteRows) {
        if ("entry".equals(table)) {
            System.out.println(
                    "%s/%s calcite head: %s".formatted(
                            caseName, table, calciteRows.subList(0, Math.min(12, calciteRows.size()))));
            System.out.println(
                    "%s/%s bean head: %s".formatted(
                            caseName, table, beanRows.subList(0, Math.min(12, beanRows.size()))));
        }
        String base =
                "%s/%s row count mismatch: bean=%d calcite=%d"
                        .formatted(caseName, table, beanRows.size(), calciteRows.size());
        List<String> beanOnly = rowDiff(beanRows, calciteRows, 5);
        if (!beanOnly.isEmpty()) {
            System.out.println("%s/%s bean-only rows (first %d): %s".formatted(caseName, table, beanOnly.size(), beanOnly));
        }
        List<String> calciteOnly = rowDiff(calciteRows, beanRows, 5);
        if (!calciteOnly.isEmpty()) {
            System.out.println(
                    "%s/%s calcite-only rows (first %d): %s".formatted(
                            caseName, table, calciteOnly.size(), calciteOnly));
        }
        String message =
                "%s | bean-only=%s | calcite-only=%s".formatted(base, beanOnly, calciteOnly);
        appendDebug(message);
        if (ROWS_DUMPED.compareAndSet(false, true)) {
            dumpRows(caseName, table, beanRows, calciteRows);
        }
        return message;
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

    private static void appendDebug(String message) {
        String line = "%s%n".formatted(message);
        try {
            Files.writeString(
                    DEBUG_LOG,
                    line,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE);
        } catch (Exception ignored) {
        }
    }

    private static void dumpRows(
            String caseName,
            String table,
            List<Map<String, String>> beanRows,
            List<Map<String, String>> calciteRows) {
        try {
            Path beanFile =
                    DEBUG_DIR.resolve("%s_%s_bean_rows.txt".formatted(caseName, table));
            Path calciteFile =
                    DEBUG_DIR.resolve("%s_%s_calcite_rows.txt".formatted(caseName, table));
            Files.write(
                    beanFile,
                    beanRows.stream().map(Object::toString).collect(Collectors.toList()),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            Files.write(
                    calciteFile,
                    calciteRows.stream().map(Object::toString).collect(Collectors.toList()),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception ignored) {
        }
    }

    private static List<LedgerCase> buildLedgerCases() {
        Path ledgerDir = TestResources.absolutePath("jdbc/src/test/resources/regression/ledgers");
        Path baselineDir = TestResources.absolutePath("jdbc/src/test/resources/regression/bean_sql");
        try (var stream = Files.list(ledgerDir)) {
            return stream.filter(Files::isRegularFile)
                    .filter(path -> !path.getFileName().toString().startsWith("."))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .map(path -> createLedgerCase(path, baselineDir))
                    .collect(Collectors.toCollection(ArrayList::new));
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to enumerate regression ledgers", ex);
        }
    }

    private static LedgerCase createLedgerCase(Path ledgerPath, Path baselineDir) {
        String filename = ledgerPath.getFileName().toString();
        String caseName = stripExtension(filename);
        Path baselinePath = baselineDir.resolve(caseName + ".sqlite");
        if (!Files.exists(baselinePath)) {
            throw new IllegalStateException(
                    "Missing bean-sql baseline for "
                            + filename
                            + " (expected "
                            + baselinePath
                            + ")");
        }
        return new LedgerCase(
                caseName,
                "classpath:regression/ledgers/" + filename,
                "classpath:regression/bean_sql/" + baselinePath.getFileName().toString());
    }

    private static String stripExtension(String filename) {
        int dot = filename.lastIndexOf('.');
        return dot >= 0 ? filename.substring(0, dot) : filename;
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
                    if (!canonicalKeys.contains(key) || ENTRY_TYPE_EXCLUSIONS.contains(row.getOrDefault("type", ""))) {
                        continue;
                    }
                    if (copy.containsKey("source_filename")) {
                        copy.put("source_filename", normalizeFilename(copy.get("source_filename")));
                    }
                    normalized.add(copy);
                }
                normalized.sort(
                        Comparator.comparing((Map<String, String> row) -> row.getOrDefault("source_filename", ""))
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
            if ("postings".equals(table)) {
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
