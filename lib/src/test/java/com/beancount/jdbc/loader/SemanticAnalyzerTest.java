package com.beancount.jdbc.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beancount.jdbc.ledger.BalanceRecord;
import com.beancount.jdbc.ledger.CloseRecord;
import com.beancount.jdbc.ledger.DocumentRecord;
import com.beancount.jdbc.ledger.EventRecord;
import com.beancount.jdbc.ledger.LedgerData;
import com.beancount.jdbc.ledger.LedgerEntry;
import com.beancount.jdbc.ledger.NoteRecord;
import com.beancount.jdbc.ledger.OpenRecord;
import com.beancount.jdbc.ledger.PadRecord;
import com.beancount.jdbc.ledger.PriceRecord;
import com.beancount.jdbc.ledger.QueryRecord;
import com.beancount.jdbc.ledger.PostingRecord;
import com.beancount.jdbc.loader.LoaderMessage;
import com.beancount.jdbc.loader.semantic.SemanticAnalysis;
import com.beancount.jdbc.loader.semantic.SemanticLedger;
import com.beancount.jdbc.loader.semantic.SemanticMetadataEntry;
import com.beancount.jdbc.loader.semantic.SemanticAnalyzer;
import com.beancount.jdbc.loader.semantic.SemanticPosting;
import com.beancount.jdbc.loader.semantic.SemanticTransaction;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SemanticAnalyzerTest {

    @Test
    void analyzesTransactionsMetadataAndChart() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-semantic");
        Path ledger = tempDir.resolve("main.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "option \"title\" \"Test Ledger\"",
                                "2019-01-02 * \"Coffee\" \"Beans\" #breakfast",
                                "  project: \"Alpha\"",
                                "  ; #coffee ^onetime",
                                "  Expenses:Coffee 5 USD",
                                "  Assets:Cash",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        List<LoaderMessage> messages = analysis.getMessages();
        // Neither account is opened, so expect warnings for both accounts.
        assertEquals(3, messages.size());
        Set<String> warningText =
                messages.stream().map(LoaderMessage::getMessage).collect(Collectors.toSet());
        assertTrue(warningText.contains("Account used before open: Expenses:Coffee"));
        assertTrue(warningText.contains("Account used before open: Assets:Cash"));
        assertTrue(warningText.stream().anyMatch(s -> s.toLowerCase().contains("option")));

        SemanticLedger semanticLedger = analysis.getLedger();
        assertNotNull(semanticLedger);
        assertTrue(semanticLedger.getOpenedAccounts().isEmpty());
        assertEquals(1, semanticLedger.getTransactions().size());
        SemanticTransaction txn = semanticLedger.getTransactions().get(0);
        assertEquals("Coffee", txn.getPayee());
        assertEquals("Beans", txn.getNarration());
        assertEquals(List.of("breakfast", "coffee"), txn.getTags());
        assertEquals(List.of("onetime"), txn.getLinks());

        List<SemanticMetadataEntry> metadata = txn.getMetadata();
        assertEquals(1, metadata.size());
        assertEquals("project", metadata.get(0).getKey());
        assertEquals("Alpha", metadata.get(0).getValue());

        List<SemanticPosting> postings = txn.getPostings();
        assertEquals(2, postings.size());
        SemanticPosting expenses = postings.get(0);
        assertEquals("Expenses:Coffee", expenses.getAccount());
        assertEquals(0, expenses.getNumber().compareTo(new BigDecimal("5")));
        SemanticPosting assets = postings.get(1);
        assertEquals("Assets:Cash", assets.getAccount());
        assertEquals(0, assets.getNumber().compareTo(new BigDecimal("-5")));

        LedgerData ledgerData = analysis.getLedgerData();
        assertEquals(1, ledgerData.getEntries().size());
        LedgerEntry entry = ledgerData.getEntries().get(0);
        assertEquals("txn", entry.getType());
        assertEquals(0, entry.getId());
        assertNotNull(entry.getTransactionPayload());
        assertEquals("Coffee", entry.getTransactionPayload().getPayee());
        assertEquals("Beans", entry.getTransactionPayload().getNarration());

        List<PostingRecord> postingRecords = ledgerData.getPostings();
        assertEquals(2, postingRecords.size());
        assertEquals("Expenses:Coffee", postingRecords.get(0).getAccount());
        assertEquals("Assets:Cash", postingRecords.get(1).getAccount());
    }

    @Test
    void collectsDirectiveRecordsIntoLedgerData() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-semantic-ledger");
        Path ledger = tempDir.resolve("main.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "2019-01-01 open Assets:Cash USD",
                                "2019-01-02 balance Assets:Cash 10 USD",
                                "2019-01-03 price USD 1.25 CAD",
                                "2019-01-04 document Assets:Cash \"receipts/jan.pdf\"",
                                "2019-01-05 event Status \"Kickoff\"",
                                "2019-01-06 note Assets:Cash \"Review\"",
                                "2019-01-07 query balances \"SELECT 1\"",
                                "2019-01-08 pad Assets:Cash Equity:Opening-Balances",
                                "2019-01-09 close Assets:Cash",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        LedgerData data = analysis.getLedgerData();

        List<String> entryTypes =
                data.getEntries().stream().map(LedgerEntry::getType).collect(Collectors.toList());
        assertEquals(
                List.of("open", "balance", "price", "document", "event", "note", "query", "pad", "close"),
                entryTypes);

        OpenRecord open = data.getOpens().get(0);
        assertEquals("Assets:Cash", open.getAccount());
        assertEquals(List.of("USD"), open.getCurrencies());

        BalanceRecord balance = data.getBalances().get(0);
        assertEquals("Assets:Cash", balance.getAccount());
        assertEquals(0, balance.getAmountNumber().compareTo(new BigDecimal("10")));

        PriceRecord price = data.getPrices().get(0);
        assertEquals("USD", price.getCurrency());
        assertEquals(0, price.getAmountNumber().compareTo(new BigDecimal("1.25")));

        DocumentRecord document = data.getDocuments().get(0);
        assertEquals("Assets:Cash", document.getAccount());
        assertEquals("receipts/jan.pdf", document.getFilename());

        EventRecord event = data.getEvents().get(0);
        assertEquals("Status", event.getType());
        assertEquals("Kickoff", event.getDescription());

        NoteRecord note = data.getNotes().get(0);
        assertEquals("Assets:Cash", note.getAccount());
        assertEquals("Review", note.getComment());

        QueryRecord query = data.getQueries().get(0);
        assertEquals("balances", query.getName());
        assertEquals("SELECT 1", query.getQueryString());

        PadRecord pad = data.getPads().get(0);
        assertEquals("Assets:Cash", pad.getAccount());
        assertEquals("Equity:Opening-Balances", pad.getSourceAccount());

        CloseRecord close = data.getCloses().get(0);
        assertEquals("Assets:Cash", close.getAccount());

        // No postings recorded because this ledger contains only non-transaction directives.
        assertTrue(data.getPostings().isEmpty());
    }

    @Test
    void balancesHonorToleranceAndOverrides() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-balance-tolerance");
        Path ledger = tempDir.resolve("main.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "2019-01-01 open Assets:Cash USD",
                                "2019-01-02 * \"Deposit\"",
                                "  Assets:Cash 10.00 USD",
                                "  Equity:Opening-Balances -10.00 USD",
                                "2019-01-03 balance Assets:Cash 10.01 USD",
                                "2019-01-04 option \"tolerance_multiplier\" \"0.25\"",
                                "2019-01-05 balance Assets:Cash 10.03 USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        List<BalanceRecord> balances = analysis.getLedgerData().getBalances();
        assertEquals(2, balances.size());

        BalanceRecord withinTolerance = balances.get(0);
        assertNull(withinTolerance.getDiffNumber(), "diff should be cleared when within tolerance");

        BalanceRecord outsideTolerance = balances.get(1);
        assertEquals(
                new BigDecimal("-0.03"), outsideTolerance.getDiffNumber(), "diff should remain when above tolerance");
        assertEquals("USD", outsideTolerance.getDiffCurrency());
    }

    @Test
    void structuredTransactionsPreserveMetadataCostAndComments() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-structured");
        Path ledger = tempDir.resolve("structured.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "2024-03-01 open Assets:Cash USD",
                                "2024-03-02 * \"Run\" \"Park\" #fitness",
                                "  Assets:Cash 10 USD {100 EUR, 2024-03-05, \"Lot-42\"} @ 1.50 CAD ; inline posting",
                                "    memo: \"Lot memo\"",
                                "  Expenses:Health -10 USD",
                                "    note: \"posting note\"",
                                "  description: \"Morning jog\"",
                                "  ; transaction level note",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);

        SemanticTransaction txn = analysis.getLedger().getTransactions().get(0);
        assertEquals("Run", txn.getPayee());
        assertEquals("Park", txn.getNarration());
        assertEquals(List.of("fitness"), txn.getTags());
        assertEquals(1, txn.getMetadata().size());
        assertEquals("description", txn.getMetadata().get(0).getKey());
        assertEquals("Morning jog", txn.getMetadata().get(0).getValue());

        List<SemanticPosting> postings = txn.getPostings();
        assertEquals(2, postings.size());

        SemanticPosting assets = postings.get(0);
        assertEquals("Assets:Cash", assets.getAccount());
        assertEquals(0, assets.getNumber().compareTo(new BigDecimal("10")));
        assertEquals("USD", assets.getCurrency());
        assertEquals(0, assets.getCostNumber().compareTo(new BigDecimal("100")));
        assertEquals("EUR", assets.getCostCurrency());
        assertEquals(0, assets.getPriceNumber().compareTo(new BigDecimal("1.50")));
        assertEquals("CAD", assets.getPriceCurrency());

        SemanticPosting expenses = postings.get(1);
        assertEquals("Expenses:Health", expenses.getAccount());
        assertEquals(0, expenses.getNumber().compareTo(new BigDecimal("-10")));

        LedgerData data = analysis.getLedgerData();
        assertEquals(2, data.getEntries().size(), "open + txn entries expected");
        List<PostingRecord> postingRecords = data.getPostings();
        assertEquals(2, postingRecords.size());
        PostingRecord firstRecord = postingRecords.get(0);
        assertEquals("Assets:Cash", firstRecord.getAccount());
        assertEquals(0, firstRecord.getNumber().compareTo(new BigDecimal("10")));
        assertEquals("USD", firstRecord.getCurrency());
        assertEquals(0, firstRecord.getCostNumber().compareTo(new BigDecimal("100")));
        assertEquals("EUR", firstRecord.getCostCurrency());
        assertEquals("2024-03-05", firstRecord.getCostDate().toString());
        assertEquals("Lot-42", firstRecord.getCostLabel());
        assertEquals(0, firstRecord.getPriceNumber().compareTo(new BigDecimal("1.50")));
        assertEquals("CAD", firstRecord.getPriceCurrency());
    }

    @Test
    void bookingMethodFifoAllowsPadAfterCostedLotConsumed() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-booking-fifo");
        Path ledger = tempDir.resolve("booking-fifo.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "2019-01-01 open Assets:Invest XYZ",
                                "2019-01-01 open Equity:Opening USD",
                                "2019-01-02 * \"Buy1\"",
                                "  Assets:Invest 5 XYZ {100 USD}",
                                "  Equity:Opening -500 USD",
                                "2019-01-03 * \"Buy2\"",
                                "  Assets:Invest 5 XYZ",
                                "  Equity:Opening -500 USD",
                                "2019-01-04 * \"Sell\"",
                                "  Assets:Invest -5 XYZ",
                                "  Equity:Opening 500 USD",
                                "2019-01-05 pad Assets:Invest Equity:Opening",
                                "2019-01-06 balance Assets:Invest 0 XYZ",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        assertFalse(
                containsPadCostWarning(analysis),
                "FIFO booking should consume costed lot first and allow padding");
    }

    @Test
    void bookingMethodLifoKeepsCostedLotAndBlocksPad() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-booking-lifo");
        Path ledger = tempDir.resolve("booking-lifo.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "option \"booking_method\" \"LIFO\"",
                                "2019-01-01 open Assets:Invest XYZ",
                                "2019-01-01 open Equity:Opening USD",
                                "2019-01-02 * \"Buy1\"",
                                "  Assets:Invest 5 XYZ {100 USD}",
                                "  Equity:Opening -500 USD",
                                "2019-01-03 * \"Buy2\"",
                                "  Assets:Invest 5 XYZ",
                                "  Equity:Opening -500 USD",
                                "2019-01-04 * \"Sell\"",
                                "  Assets:Invest -5 XYZ",
                                "  Equity:Opening 500 USD",
                                "2019-01-05 pad Assets:Invest Equity:Opening",
                                "2019-01-06 balance Assets:Invest 0 XYZ",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        assertTrue(
                containsPadCostWarning(analysis),
                "LIFO booking should leave costed lot and block padding");
    }

    @Test
    void bookingMethodAverageKeepsCostedLotAfterMixedBuys() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-booking-average");
        Path ledger = tempDir.resolve("booking-average.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "option \"booking_method\" \"AVERAGE\"",
                                "2019-01-01 open Assets:Invest XYZ",
                                "2019-01-01 open Equity:Opening USD",
                                "2019-01-02 * \"BuyCost\"",
                                "  Assets:Invest 5 XYZ {100 USD}",
                                "  Equity:Opening -500 USD",
                                "2019-01-03 * \"BuyNoCost\"",
                                "  Assets:Invest 5 XYZ",
                                "  Equity:Opening -500 USD",
                                "2019-01-04 * \"Sell\"",
                                "  Assets:Invest -5 XYZ",
                                "  Equity:Opening 500 USD",
                                "2019-01-05 pad Assets:Invest Equity:Opening",
                                "2019-01-06 balance Assets:Invest 5 XYZ",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        assertTrue(
                containsPadCostWarning(analysis),
                "AVERAGE booking should retain blended cost and block padding");
    }

    @Test
    void duplicateTagsCollapsedAcrossStackAndInline() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-tags");
        Path ledger = tempDir.resolve("tags.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "pushtag project",
                                "2019-01-01 * \"Stack\" #project",
                                "  Expenses:Projects 10 USD",
                                "  Assets:Cash -10 USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        SemanticTransaction txn = analysis.getLedger().getTransactions().get(0);
        assertEquals(List.of("project"), txn.getTags());
    }

    @Test
    void bookingMethodStrictRequiresCostAnnotations() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-booking-strict-missing-cost");
        Path ledger = tempDir.resolve("booking-strict-missing-cost.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "option \"booking_method\" \"STRICT\"",
                                "2019-01-01 open Assets:Invest XYZ",
                                "2019-01-02 * \"Buy\"",
                                "  Assets:Invest 5 XYZ",
                                "  Equity:Opening -500 USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        assertTrue(
                containsMessage(analysis, "requires explicit cost"),
                "STRICT booking should error when cost spec missing");
    }

    @Test
    void bookingMethodStrictMatchesExplicitLot() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-booking-strict-match");
        Path ledger = tempDir.resolve("booking-strict-match.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "option \"booking_method\" \"STRICT\"",
                                "2019-01-01 open Assets:Invest XYZ",
                                "2019-01-02 * \"Buy\"",
                                "  Assets:Invest 5 XYZ {100 USD}",
                                "  Equity:Opening -500 USD",
                                "2019-01-03 * \"Sell\"",
                                "  Assets:Invest -5 XYZ {100 USD}",
                                "  Equity:Opening 500 USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        assertTrue(
                analysis.getMessages().isEmpty(),
                "STRICT booking should succeed when lots are annotated");
    }

    @Test
    void bookingMethodStrictErrorsOnMissingMatchingLot() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-booking-strict-mismatch");
        Path ledger = tempDir.resolve("booking-strict-mismatch.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "option \"booking_method\" \"STRICT\"",
                                "2019-01-01 open Assets:Invest XYZ",
                                "2019-01-02 * \"Buy\"",
                                "  Assets:Invest 5 XYZ {100 USD}",
                                "  Equity:Opening -500 USD",
                                "2019-01-03 * \"Sell\"",
                                "  Assets:Invest -5 XYZ {200 USD}",
                                "  Equity:Opening 500 USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        assertTrue(
                containsMessage(analysis, "could not find lot"),
                "STRICT booking should error when lot annotations do not match");
    }

    @Test
    void pushtagAndPushmetaAffectSubsequentTransactions() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-tags-meta");
        Path ledger = tempDir.resolve("tags-meta.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "pushtag travel",
                                "pushmeta purpose \"Trip\"",
                                "2019-01-01 * \"Taxi\"",
                                "  Expenses:Travel 10 USD",
                                "  Assets:Cash -10 USD",
                                "poptag",
                                "popmeta purpose",
                                "2019-01-02 * \"Lunch\"",
                                "  Expenses:Food 5 USD",
                                "  Assets:Cash -5 USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        List<SemanticTransaction> txns = analysis.getLedger().getTransactions();
        SemanticTransaction first = txns.get(0);
        assertTrue(first.getTags().contains("travel"));
        assertTrue(
                first.getMetadata().stream()
                        .anyMatch(entry -> entry.getKey().equals("purpose") && entry.getValue().equals("Trip")));

        SemanticTransaction second = txns.get(1);
        assertFalse(second.getTags().contains("travel"));
        assertFalse(
                second.getMetadata().stream()
                        .anyMatch(entry -> entry.getKey().equals("purpose")));
    }

    @Test
    void aggregatesOperatingCurrencyOptions() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-operating");
        Path ledger = tempDir.resolve("operating.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "option \"operating_currency\" \"USD\"",
                                "option \"operating_currency\" \"CAD\"",
                                "option \"operating_currency\" \"USD\"",
                                "2019-01-01 open Assets:Cash USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        assertEquals(List.of("USD", "CAD"), analysis.getLedger().getOperatingCurrencies());
    }

    @Test
    void renderCommasOptionPropagatesToDisplayContext() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-render-commas");
        Path ledger = tempDir.resolve("render-commas.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "option \"render_commas\" \"TRUE\"",
                                "2019-01-01 open Assets:Cash USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        assertTrue(analysis.getLedger().getDisplayContext().isRenderCommas());
    }

    @Test
    void displayPrecisionOptionDefinesCurrencyPrecision() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-display-precision");
        Path ledger = tempDir.resolve("display-precision.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "option \"display_precision\" \"USD:0.01\"",
                                "2019-01-01 open Assets:Cash USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        int precision =
                analysis.getLedger().getDisplayContext().getPrecision("USD", 0);
        assertEquals(2, precision);
    }

    @Test
    void toleranceOptionClearsDiffAcrossCurrencies() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-tolerance-option");
        Path ledger = tempDir.resolve("tolerance.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "option \"tolerance\" \"2\"",
                                "2019-01-01 open Assets:Cash USD",
                                "2019-01-02 * \"Deposit\"",
                                "  Assets:Cash 10 USD",
                                "  Equity:Opening -10 USD",
                                "2019-01-03 balance Assets:Cash 8 USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        BalanceRecord balance = analysis.getLedgerData().getBalances().get(0);
        assertNull(balance.getDiffNumber(), "Diff should be cleared by tolerance option");
    }

    @Test
    void toleranceMapOverridesSpecificCurrency() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-tolerance-map");
        Path ledger = tempDir.resolve("tolerance-map.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "option \"tolerance_map\" \"USD:0.5\"",
                                "2019-01-01 open Assets:Cash USD",
                                "2019-01-02 * \"Deposit\"",
                                "  Assets:Cash 10 USD",
                                "  Equity:Opening -10 USD",
                                "2019-01-03 balance Assets:Cash 10.4 USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        BalanceRecord balance = analysis.getLedgerData().getBalances().get(0);
        assertNull(balance.getDiffNumber(), "Currency-specific tolerance should clear diff");
    }

    @Test
    void padAllowedAfterCostedLotsClosed() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-pad-cost-closed");
        Path ledger = tempDir.resolve("padcost.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "2024-01-01 open Assets:Invest XYZ",
                                "2024-01-01 open Equity:Opening USD",
                                "2024-01-02 * \"Buy\"",
                                "  Assets:Invest 5 XYZ {500 USD}",
                                "  Equity:Opening -500 USD",
                                "2024-01-03 * \"Sell\"",
                                "  Assets:Invest -5 XYZ",
                                "  Equity:Opening 500 USD",
                                "2024-01-04 pad Assets:Invest Equity:Opening",
                                "2024-01-05 balance Assets:Invest 10 XYZ",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        boolean hasCostWarning =
                analysis.getMessages().stream()
                        .anyMatch(message -> message.getMessage().contains("Attempt to pad an entry with cost"));
        assertFalse(hasCostWarning, "Pad should be allowed once costed lots are closed");

        long padTxnCount =
                analysis.getLedgerData().getEntries().stream()
                        .filter(entry -> "txn".equals(entry.getType()))
                        .count();
        assertEquals(2, padTxnCount, "Only the explicit buy/sell transactions should remain");

        BalanceRecord balance = analysis.getLedgerData().getBalances().get(0);
        assertNull(balance.getDiffNumber(), "Pad adjustment should still satisfy the balance");
    }

    @Test
    void padDirectiveDoesNotSynthesizeTransaction() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-pad");
        Path ledger = tempDir.resolve("main.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "2019-01-01 open Assets:Cash USD",
                                "2019-01-02 pad Assets:Cash Equity:Opening-Balances",
                                "2019-01-03 balance Assets:Cash 5 USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        LedgerData data = analysis.getLedgerData();

        List<LedgerEntry> entries = data.getEntries();
        assertEquals(3, entries.size(), "open, pad, balance");
        long txnCount = entries.stream().filter(entry -> "txn".equals(entry.getType())).count();
        assertEquals(0, txnCount, "Pad directive should not synthesize transactions");

        assertTrue(data.getPostings().isEmpty(), "No postings should be generated for pad adjustments");

        BalanceRecord balance = data.getBalances().get(0);
        assertNull(balance.getDiffNumber(), "balance should be satisfied after padding");
    }

    @Test
    void padFailsForCostedPositions() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-pad-cost");
        Path ledger = tempDir.resolve("main.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "2019-01-01 open Assets:Invest XYZ",
                                "2019-01-01 open Equity:Opening-Balances USD",
                                "2019-01-02 * \"Buy\"",
                                "  Assets:Invest 1 XYZ {100 USD}",
                                "  Equity:Opening-Balances -100 USD",
                                "2019-01-03 pad Assets:Invest Equity:Opening-Balances",
                                "2019-01-04 balance Assets:Invest 2 XYZ",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        List<LoaderMessage> messages = analysis.getMessages();
        assertTrue(
                messages.stream()
                        .anyMatch(
                                message ->
                                        message.getMessage()
                                                .contains("Attempt to pad an entry with cost for balance")),
                "Expected padding with cost warning");

        LedgerData data = analysis.getLedgerData();
        BalanceRecord balance = data.getBalances().get(0);
        assertEquals(new BigDecimal("-1"), balance.getDiffNumber());
        assertEquals("XYZ", balance.getDiffCurrency());

        long txnCount = data.getEntries().stream().filter(entry -> "txn".equals(entry.getType())).count();
        assertEquals(1, txnCount, "Padding should not insert additional transactions");
    }

    @Test
    void includesPreserveDirectiveOrderAndStackState() throws Exception {
        Path tempDir = Files.createTempDirectory("beancount-include-stack");
        Path sub = tempDir.resolve("sub.beancount");
        Files.writeString(
                sub,
                String.join(
                                System.lineSeparator(),
                                "2019-01-02 * \"SubTxn\"",
                                "  Assets:Cash -10 USD",
                                "  Expenses:Travel 10 USD",
                                "")
                        + System.lineSeparator());

        Path ledger = tempDir.resolve("main.beancount");
        Files.writeString(
                ledger,
                String.join(
                                System.lineSeparator(),
                                "2019-01-01 open Assets:Cash USD",
                                "pushtag stack",
                                "include \"" + sub.getFileName().toString() + "\"",
                                "poptag",
                                "2019-01-04 * \"RootTxn\"",
                                "  Assets:Cash -5 USD",
                                "  Expenses:Misc 5 USD",
                                "")
                        + System.lineSeparator());

        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledger);
        LedgerData data = analysis.getLedgerData();

        List<LedgerEntry> entries = data.getEntries();
        assertEquals(3, entries.size(), "open + include txn + local txn");
        assertEquals(List.of("open", "txn", "txn"), entries.stream().map(LedgerEntry::getType).collect(Collectors.toList()));
        assertEquals(List.of(0, 1, 2), entries.stream().map(LedgerEntry::getId).collect(Collectors.toList()));
        assertEquals(ledger.toString(), entries.get(0).getSourceFilename());
        assertEquals(sub.toString(), entries.get(1).getSourceFilename());
        assertEquals(ledger.toString(), entries.get(2).getSourceFilename());

        List<SemanticTransaction> transactions = analysis.getLedger().getTransactions();
        assertEquals(2, transactions.size());
        SemanticTransaction fromInclude = transactions.get(0);
        assertTrue(fromInclude.getTags().contains("stack"), "pushed tags should apply inside included files");
        SemanticTransaction inMain = transactions.get(1);
        assertTrue(inMain.getTags().isEmpty(), "popped tags should stop applying after include");
    }

    private static boolean containsPadCostWarning(SemanticAnalysis analysis) {
        return containsMessage(analysis, "Attempt to pad an entry with cost");
    }

    private static boolean containsMessage(SemanticAnalysis analysis, String substring) {
        return analysis.getMessages().stream()
                .anyMatch(message -> message.getMessage().contains(substring));
    }
}
