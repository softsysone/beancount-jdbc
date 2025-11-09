package com.beancount.jdbc.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.beancount.jdbc.ledger.LedgerData;
import com.beancount.jdbc.loader.semantic.SemanticAnalyzer;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class SchemaRoundTripTest {

    @Test
    void transactionsDetailReflectsStackedTagsAndLinks() throws Exception {
        LedgerData data =
                loadLedger(
                        String.join(
                                        System.lineSeparator(),
                                        "pushtag travel",
                                        "2019-01-01 * \"Taxi\" #travel #project ^itinerary",
                                        "  Expenses:Travel 10 USD",
                                        "  Assets:Cash -10 USD",
                                        "")
                                + System.lineSeparator());

        List<Object[]> rows = TransactionsDetailTable.materializeRows(data.getEntries());
        assertEquals(1, rows.size());
        Object[] row = rows.get(0);
        assertEquals("travel,project", row[4], "stack tag + inline tag should dedupe like bean-sql");
        assertEquals("itinerary", row[5], "links column should capture inline links");
    }

    @Test
    void directiveTablesEmitBeanSqlCompatibleDetailRows() throws Exception {
        LedgerData data =
                loadLedger(
                        String.join(
                                        System.lineSeparator(),
                                        "2019-01-01 open Assets:Cash USD",
                                        "2019-01-02 note Assets:Cash \"Ready\"",
                                        "2019-01-03 document Assets:Cash \"receipts/jan.pdf\"",
                                        "2019-01-04 event Status \"Kickoff\"",
                                        "2019-01-05 query balances \"SELECT 1\"",
                                        "2019-01-06 pad Assets:Cash Equity:Opening",
                                        "2019-01-07 price USD 1.25 CAD",
                                        "2019-01-08 balance Assets:Cash 10 USD",
                                        "2019-01-09 close Assets:Cash",
                                        "")
                                + System.lineSeparator());

        Object[] open = OpenTable.materializeDetailRows(data.getOpens()).get(0);
        assertEquals("Assets:Cash", open[1]);
        assertEquals("USD", open[2]);

        Object[] note = NoteTable.materializeDetailRows(data.getNotes()).get(0);
        assertEquals("Assets:Cash", note[1]);
        assertEquals("Ready", note[2]);

        Object[] document = DocumentTable.materializeDetailRows(data.getDocuments()).get(0);
        assertEquals("Assets:Cash", document[1]);
        assertEquals("receipts/jan.pdf", document[2]);

        Object[] event = EventTable.materializeDetailRows(data.getEvents()).get(0);
        assertEquals("Status", event[1]);
        assertEquals("Kickoff", event[2]);

        Object[] query = QueryTable.materializeDetailRows(data.getQueries()).get(0);
        assertEquals("balances", query[1]);
        assertEquals("SELECT 1", query[2]);

        Object[] pad = PadTable.materializeDetailRows(data.getPads()).get(0);
        assertEquals("Assets:Cash", pad[1]);
        assertEquals("Equity:Opening", pad[2]);

        Object[] price = PriceTable.materializeDetailRows(data.getPrices()).get(0);
        assertEquals("USD", price[1]);
        assertEquals(0, ((BigDecimal) price[2]).compareTo(new BigDecimal("1.25")));
        assertEquals("CAD", price[3]);

        Object[] balance = BalanceTable.materializeDetailRows(data.getBalances()).get(0);
        assertEquals("Assets:Cash", balance[1]);
        assertEquals(0, ((BigDecimal) balance[2]).compareTo(new BigDecimal("10")));
        assertEquals("USD", balance[3]);
        assertNull(balance[4], "diff_number should be null when no adjustment is required");
        assertNull(balance[5], "diff_currency should be null when diff_number is null");

        Object[] close = CloseTable.materializeDetailRows(data.getCloses()).get(0);
        assertEquals(2, close.length, "close_detail should only emit id + account");
        assertEquals("Assets:Cash", close[1]);
    }

    @Test
    void viewRowsJoinEntryAndDetailColumns() throws Exception {
        LedgerData data =
                loadLedger(
                        String.join(
                                        System.lineSeparator(),
                                        "2019-01-01 open Assets:Cash USD",
                                        "2019-01-02 pad Assets:Cash Equity:Opening",
                                        "")
                                + System.lineSeparator());

        List<Object[]> entryRows = EntryTable.materializeRows(data.getEntries());

        List<Object[]> openView =
                OpenTable.materializeViewRows(entryRows, OpenTable.materializeDetailRows(data.getOpens()));
        assertEquals(1, openView.size());
        Object[] openRow = openView.get(0);
        Object[] openEntry = entryRows.get(0);
        assertEquals(openEntry[0], openRow[0]);
        assertEquals(openEntry[1], openRow[1]);
        assertEquals(openEntry[2], openRow[2]);
        assertEquals("Assets:Cash", openRow[5]);
        assertEquals("USD", openRow[6]);

        List<Object[]> padView =
                PadTable.materializeViewRows(entryRows, PadTable.materializeDetailRows(data.getPads()));
        assertEquals(1, padView.size());
        Object[] padRow = padView.get(0);
        Object[] padEntry = entryRows.get(1);
        assertEquals(padEntry[0], padRow[0]);
        assertEquals(padEntry[3], padRow[3]);
        assertEquals(padEntry[4], padRow[4]);
        assertEquals("Assets:Cash", padRow[5]);
        assertEquals("Equity:Opening", padRow[6]);
    }

    private static LedgerData loadLedger(String contents) throws Exception {
        Path tempDir = Files.createTempDirectory("schema-ledger");
        Path ledger = tempDir.resolve("main.beancount");
        Files.writeString(ledger, contents);
        return new SemanticAnalyzer().analyze(ledger).getLedgerData();
    }
}
