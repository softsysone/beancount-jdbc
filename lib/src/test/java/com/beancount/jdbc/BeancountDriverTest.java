package com.beancount.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class BeancountDriverTest {

    @Test
    void connectsWhenLedgerExists() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-ledger");
        Path ledger = tempDir.resolve("main.beancount");
        Path secondary = tempDir.resolve("secondary.beancount");

        Files.writeString(
                secondary,
                String.join(
                                System.lineSeparator(),
                                "2019-01-03 open Expenses:Food",
                                "2019-01-03 close Assets:Cash",
                                "2019-01-03 * \"Snacks\" \"Vending\"",
                                "  Expenses:Snacks 3 USD",
                                "  Assets:Cash",
                                "")
                        + System.lineSeparator());

        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "2019-01-01 open Assets:Cash USD,EUR",
                                "include \"secondary.beancount\"",
                                "2019-01-02 pad Assets:Cash Equity:Opening",
                                "2019-01-02 * \"Coffee\" \"Beans\" #breakfast",
                                "  ; #coffee ^onetime",
                                "  Expenses:Coffee 5 USD",
                                "  Assets:Cash",
                                "2019-01-02 note Assets:Cash FirstNote",
                                "2019-01-02 document Assets:Cash receipts/invoice.pdf",
                                "2019-01-02 event Project Kickoff",
                                "2019-01-02 custom \"ignored\" \"placeholder\"",
                                "2019-01-02 query accounts SELECT_SUMMARY",
                                "2019-01-02 price USD 1 CAD",
                                "2019-01-03 close Expenses:Snacks",
                                "2019-01-03 balance Assets:Cash 0 USD",
                                "2019-01-04 * \"Lunch\" \"Cafe\"",
                                "  Expenses:Food 12 USD",
                                "  Assets:Cash",
                                "2019-01-05 balance Assets:Cash -10 USD",
                                "")
                        + System.lineSeparator());

        Connection connection = DriverManager.getConnection("jdbc:beancount:" + ledger);
        assertNotNull(connection, "Driver should return a connection");
        assertTrue(connection.isValid(0));
        assertEquals("com.beancount.jdbc.BeancountConnection", connection.unwrap(Connection.class).getClass().getName());

        DatabaseMetaData metaData = connection.getMetaData();
        assertEquals("Beancount", metaData.getDatabaseProductName());
        assertEquals(Version.RUNTIME, metaData.getDatabaseProductVersion());
        assertTrue(metaData.isReadOnly());

        try (ResultSet tables = metaData.getTables(null, null, "%", new String[] {"TABLE", "VIEW"})) {
            Set<String> actual = new HashSet<>();
            while (tables.next()) {
                actual.add(tables.getString("TABLE_NAME") + ":" + tables.getString("TABLE_TYPE"));
            }
            Set<String> expected =
                    Set.of(
                            "entry:TABLE",
                            "transactions_detail:TABLE",
                            "transactions:VIEW",
                            "open_detail:TABLE",
                            "open:VIEW",
                            "close_detail:TABLE",
                            "close:VIEW",
                            "pad_detail:TABLE",
                            "pad:VIEW",
                            "balance_detail:TABLE",
                            "balance:VIEW",
                            "note_detail:TABLE",
                            "note:VIEW",
                            "document_detail:TABLE",
                            "document:VIEW",
                            "event_detail:TABLE",
                            "event:VIEW",
                            "query_detail:TABLE",
                            "query:VIEW",
                            "price_detail:TABLE",
                            "price:VIEW",
                            "postings:TABLE");
            assertEquals(expected, actual);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "ENTRY", null)) {
            List<String> names = new ArrayList<>();
            List<Integer> dataTypes = new ArrayList<>();
            List<String> typeNames = new ArrayList<>();
            List<String> nullableFlags = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
                dataTypes.add(columns.getInt("DATA_TYPE"));
                typeNames.add(columns.getString("TYPE_NAME"));
                nullableFlags.add(columns.getString("IS_NULLABLE"));
            }
            assertEquals(List.of("id", "date", "type", "source_filename", "source_lineno"), names);
            assertEquals(
                    List.of(Types.INTEGER, Types.DATE, Types.VARCHAR, Types.VARCHAR, Types.INTEGER), dataTypes);
            assertEquals(List.of("INTEGER", "DATE", "STRING", "STRING", "INTEGER"), typeNames);
            assertEquals(List.of("NO", "YES", "YES", "YES", "YES"), nullableFlags);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "TRANSACTIONS_DETAIL", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "flag", "payee", "narration", "tags", "links"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "TRANSACTIONS", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(
                    List.of("id", "date", "type", "source_filename", "source_lineno", "flag", "payee", "narration", "tags", "links"),
                    names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "OPEN_DETAIL", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "account", "currencies"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "OPEN", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(
                    List.of("id", "date", "type", "source_filename", "source_lineno", "account", "currencies"),
                    names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "CLOSE_DETAIL", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "account"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "CLOSE", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "date", "type", "source_filename", "source_lineno", "account"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "PAD_DETAIL", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "account", "source_account"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "PAD", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(
                    List.of("id", "date", "type", "source_filename", "source_lineno", "account", "source_account"),
                    names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "NOTE_DETAIL", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "account", "comment"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "NOTE", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(
                    List.of("id", "date", "type", "source_filename", "source_lineno", "account", "comment"),
                    names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "DOCUMENT_DETAIL", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "account", "filenam"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "DOCUMENT", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(
                    List.of("id", "date", "type", "source_filename", "source_lineno", "account", "filenam"),
                    names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "EVENT_DETAIL", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "type", "description"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "EVENT", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(
                    List.of("id", "date", "type", "source_filename", "source_lineno", "event_type", "description"),
                    names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "QUERY_DETAIL", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "name", "query_string"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "QUERY", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(
                    List.of("id", "date", "type", "source_filename", "source_lineno", "name", "query_string"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "PRICE_DETAIL", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "currency", "amount_number", "amount_currency"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "PRICE", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(
                    List.of("id", "date", "type", "source_filename", "source_lineno", "currency", "amount_number", "amount_currency"),
                    names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "BALANCE_DETAIL", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "account", "amount_number", "amount_currency", "diff_number", "diff_currency"), names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "BALANCE", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(
                    List.of("id", "date", "type", "source_filename", "source_lineno", "account", "amount_number", "amount_currency", "diff_number", "diff_currency"),
                    names);
        }

        try (ResultSet columns = metaData.getColumns(null, null, "POSTINGS", null)) {
            List<String> names = new ArrayList<>();
            while (columns.next()) {
                names.add(columns.getString("COLUMN_NAME"));
            }
            assertEquals(
                    List.of(
                            "posting_id",
                            "id",
                            "flag",
                            "account",
                            "number",
                            "currency",
                            "cost_number",
                            "cost_currency",
                            "cost_date",
                            "cost_label",
                            "price_number",
                            "price_currency"),
                    names);
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("entry"))) {
            List<Integer> ids = new ArrayList<>();
            List<String> types = new ArrayList<>();
            List<String> filenames = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                types.add(rs.getString("type"));
                filenames.add(rs.getString("source_filename"));
            }
            assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15), ids);
            assertEquals(
                    List.of(
                            "open",
                            "open",
                            "close",
                            "txn",
                            "pad",
                            "txn",
                            "note",
                            "document",
                            "event",
                            "query",
                            "price",
                            "close",
                            "balance",
                            "txn",
                            "balance"),
                    types);
            assertTrue(filenames.get(0).endsWith("main.beancount"));
            assertTrue(filenames.get(1).endsWith("secondary.beancount"));
            assertTrue(filenames.get(2).endsWith("secondary.beancount"));
            assertTrue(filenames.get(3).endsWith("secondary.beancount"));
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("transactions_detail"))) {
            List<Integer> ids = new ArrayList<>();
            List<String> payees = new ArrayList<>();
            List<Set<String>> tagSets = new ArrayList<>();
            List<String> links = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                payees.add(rs.getString("payee"));
                tagSets.add(splitValues(rs.getString("tags")));
                links.add(rs.getString("links"));
            }
            assertEquals(List.of(3, 5, 14), ids);
            assertEquals(List.of("Snacks", "Coffee", "Lunch"), payees);
            assertEquals(List.of(Set.of(), Set.of("breakfast", "coffee"), Set.of()), tagSets);
            assertEquals(Arrays.asList(null, "onetime", null), links);
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("transactions"))) {
            List<Integer> ids = new ArrayList<>();
            List<String> payees = new ArrayList<>();
            List<Set<String>> tagSets = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                payees.add(rs.getString("payee"));
                tagSets.add(splitValues(rs.getString("tags")));
            }
            assertEquals(List.of(3, 5, 14), ids);
            assertEquals(List.of("Snacks", "Coffee", "Lunch"), payees);
            assertEquals(List.of(Set.of(), Set.of("breakfast", "coffee"), Set.of()), tagSets);
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("open_detail"))) {
            List<Integer> ids = new ArrayList<>();
            List<String> accounts = new ArrayList<>();
            List<String> currencies = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                accounts.add(rs.getString("account"));
                currencies.add(rs.getString("currencies"));
            }
            assertEquals(List.of(0, 1), ids);
            assertEquals(List.of("Assets:Cash", "Expenses:Food"), accounts);
            assertEquals(List.of("USD,EUR", ""), currencies);
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("open"))) {
            List<Integer> ids = new ArrayList<>();
            List<String> accounts = new ArrayList<>();
            List<String> currencies = new ArrayList<>();
            List<String> filenames = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                accounts.add(rs.getString("account"));
                currencies.add(rs.getString("currencies"));
                filenames.add(rs.getString("source_filename"));
            }
            assertEquals(List.of(0, 1), ids);
            assertEquals(List.of("Assets:Cash", "Expenses:Food"), accounts);
            assertEquals(List.of("USD,EUR", ""), currencies);
            assertTrue(filenames.get(0).endsWith("main.beancount"));
            assertTrue(filenames.get(1).endsWith("secondary.beancount"));
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("close_detail"))) {
            List<Integer> ids = new ArrayList<>();
            List<String> accounts = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                accounts.add(rs.getString("account"));
            }
            assertEquals(List.of(2, 12), ids);
            assertEquals(List.of("Assets:Cash", "Expenses:Snacks"), accounts);
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("close"))) {
            List<Integer> ids = new ArrayList<>();
            List<String> accounts = new ArrayList<>();
            List<String> filenames = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                accounts.add(rs.getString("account"));
                filenames.add(rs.getString("source_filename"));
            }
            assertEquals(List.of(2, 12), ids);
            assertEquals(List.of("Assets:Cash", "Expenses:Snacks"), accounts);
            assertTrue(filenames.get(0).endsWith("secondary.beancount"));
            assertTrue(filenames.get(1).endsWith("main.beancount"));
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("pad_detail"))) {
            List<Integer> ids = new ArrayList<>();
            List<String> accounts = new ArrayList<>();
            List<String> sources = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                accounts.add(rs.getString("account"));
                sources.add(rs.getString("source_account"));
            }
            assertEquals(List.of(4), ids);
            assertEquals(List.of("Assets:Cash"), accounts);
            assertEquals(List.of("Equity:Opening"), sources);
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("pad"))) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("id"));
            assertEquals("pad", rs.getString("type"));
            assertEquals("Assets:Cash", rs.getString("account"));
            assertEquals("Equity:Opening", rs.getString("source_account"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("note_detail"))) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("id"));
            assertEquals("Assets:Cash", rs.getString("account"));
            assertEquals("FirstNote", rs.getString("comment"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("note"))) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("id"));
            assertEquals("note", rs.getString("type"));
            assertEquals("Assets:Cash", rs.getString("account"));
            assertEquals("FirstNote", rs.getString("comment"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("document_detail"))) {
            assertTrue(rs.next());
            assertEquals(7, rs.getInt("id"));
            assertEquals("Assets:Cash", rs.getString("account"));
            assertEquals("receipts/invoice.pdf", rs.getString("filenam"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("document"))) {
            assertTrue(rs.next());
            assertEquals(7, rs.getInt("id"));
            assertEquals("document", rs.getString("type"));
            assertEquals("Assets:Cash", rs.getString("account"));
            assertEquals("receipts/invoice.pdf", rs.getString("filenam"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("event_detail"))) {
            assertTrue(rs.next());
            assertEquals(8, rs.getInt("id"));
            assertEquals("Project", rs.getString("type"));
            assertEquals("Kickoff", rs.getString("description"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("event"))) {
            assertTrue(rs.next());
            assertEquals(8, rs.getInt("id"));
            assertEquals("event", rs.getString("type"));
            assertEquals("Project", rs.getString(6));
            assertEquals("Kickoff", rs.getString("description"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("query_detail"))) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("id"));
            assertEquals("accounts", rs.getString("name"));
            assertEquals("SELECT_SUMMARY", rs.getString("query_string"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("query"))) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("id"));
            assertEquals("query", rs.getString("type"));
            assertEquals("accounts", rs.getString("name"));
            assertEquals("SELECT_SUMMARY", rs.getString("query_string"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("price_detail"))) {
            assertTrue(rs.next());
            assertEquals(11, rs.getInt("id"));
            assertEquals("USD", rs.getString("currency"));
            assertEquals(new BigDecimal("1"), rs.getBigDecimal("amount_number"));
            assertEquals("CAD", rs.getString("amount_currency"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("price"))) {
            assertTrue(rs.next());
            assertEquals(11, rs.getInt("id"));
            assertEquals("price", rs.getString("type"));
            assertEquals("USD", rs.getString("currency"));
            assertEquals(new BigDecimal("1"), rs.getBigDecimal("amount_number"));
            assertEquals("CAD", rs.getString("amount_currency"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("balance_detail"))) {
            assertTrue(rs.next());
            assertEquals(13, rs.getInt("id"));
            assertEquals("Assets:Cash", rs.getString("account"));
            assertEquals(new BigDecimal("0"), rs.getBigDecimal("amount_number"));
            assertEquals("USD", rs.getString("amount_currency"));
            assertNull(rs.getBigDecimal("diff_number"));
            assertNull(rs.getString("diff_currency"));
            assertTrue(rs.next());
            assertEquals(15, rs.getInt("id"));
            assertEquals("Assets:Cash", rs.getString("account"));
            assertEquals(new BigDecimal("-10"), rs.getBigDecimal("amount_number"));
            assertEquals("USD", rs.getString("amount_currency"));
            assertEquals(new BigDecimal("-2"), rs.getBigDecimal("diff_number"));
            assertEquals("USD", rs.getString("diff_currency"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("balance"))) {
            assertTrue(rs.next());
            assertEquals(13, rs.getInt("id"));
            assertEquals("balance", rs.getString("type"));
            assertEquals("Assets:Cash", rs.getString("account"));
            assertEquals(new BigDecimal("0"), rs.getBigDecimal("amount_number"));
            assertEquals("USD", rs.getString("amount_currency"));
            assertNull(rs.getBigDecimal("diff_number"));
            assertNull(rs.getString("diff_currency"));
            assertTrue(rs.next());
            assertEquals(15, rs.getInt("id"));
            assertEquals("balance", rs.getString("type"));
            assertEquals("Assets:Cash", rs.getString("account"));
            assertEquals(new BigDecimal("-10"), rs.getBigDecimal("amount_number"));
            assertEquals("USD", rs.getString("amount_currency"));
            assertEquals(new BigDecimal("-2"), rs.getBigDecimal("diff_number"));
            assertEquals("USD", rs.getString("diff_currency"));
            assertFalse(rs.next());
        }

        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sqlAll("postings"))) {
            List<Integer> postingIds = new ArrayList<>();
            List<Integer> entryIds = new ArrayList<>();
            List<String> accounts = new ArrayList<>();
            List<BigDecimal> numbers = new ArrayList<>();
            while (rs.next()) {
                postingIds.add(rs.getInt("posting_id"));
                entryIds.add(rs.getInt("id"));
                accounts.add(rs.getString("account"));
                numbers.add((BigDecimal) rs.getObject("number"));
            }
            assertEquals(List.of(0, 1, 2, 3, 4, 5), postingIds);
            assertEquals(List.of(3, 3, 5, 5, 14, 14), entryIds);
            assertEquals(
                    List.of(
                            "Expenses:Snacks",
                            "Assets:Cash",
                            "Expenses:Coffee",
                            "Assets:Cash",
                            "Expenses:Food",
                            "Assets:Cash"),
                    accounts);
            assertEquals(
                    Arrays.asList(
                            new BigDecimal("3"),
                            null,
                            new BigDecimal("5"),
                            null,
                            new BigDecimal("12"),
                            null),
                    numbers);
        }

        connection.close();
        assertTrue(connection.isClosed());
    }

    private static String sqlAll(String table) {
        return "SELECT * FROM \"" + table + "\"";
    }

    private static String sql(List<String> columns, String table, String orderBy) {
        String select =
                columns.stream()
                        .map(column -> "\"" + column + "\"")
                        .collect(Collectors.joining(", "));
        StringBuilder builder = new StringBuilder("SELECT ")
                .append(select)
                .append(" FROM \"")
                .append(table)
                .append("\"");
        if (orderBy != null) {
            builder.append(" ORDER BY \"").append(orderBy).append("\"");
        }
        return builder.toString();
    }

    @Test
    void rejectsMissingLedger() {
        Path missing = Path.of("does-not-exist.beancount");
        SQLException exception =
                assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:beancount:" + missing));
        assertTrue(exception.getMessage().contains("Ledger file not found"));
    }

    @Test
    void ledgerPathCanBeProvidedViaProperties() throws Exception {
        Path ledger = Files.createTempFile("beancount-ledger", ".beancount");
        Properties props = new Properties();
        props.setProperty("any", "value");

        try (Connection connection = DriverManager.getConnection("jdbc:beancount:" + ledger, props)) {
            assertEquals(ledger.toAbsolutePath().toString(), connection.getClientInfo("ledger"));
        }
    }

    private static Set<String> splitValues(String value) {
        if (value == null || value.isEmpty()) {
            return Set.of();
        }
        return new HashSet<>(Arrays.asList(value.split(",")));
    }
}
