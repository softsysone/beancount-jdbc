package com.beancount.jdbc.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beancount.jdbc.loader.ast.PostingNode;
import com.beancount.jdbc.loader.ast.DirectiveNode;
import com.beancount.jdbc.loader.ast.DocumentDirectiveNode;
import com.beancount.jdbc.loader.ast.IncludeNode;
import com.beancount.jdbc.loader.ast.LedgerNode;
import com.beancount.jdbc.loader.ast.NoteDirectiveNode;
import com.beancount.jdbc.loader.ast.OpenDirectiveNode;
import com.beancount.jdbc.loader.ast.PadDirectiveNode;
import com.beancount.jdbc.loader.ast.PriceDirectiveNode;
import com.beancount.jdbc.loader.ast.StatementNode;
import com.beancount.jdbc.loader.ast.TransactionNode;
import java.util.List;
import java.math.BigDecimal;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

class BeancountAstBuilderTest {

    private static final String SAMPLE_LEDGER =
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
                    + System.lineSeparator();

    @Test
    void parsesLedgerIntoAst() throws Exception {
        BeancountAstBuilder builder = new BeancountAstBuilder();
        LedgerNode ledger = builder.parse("main.beancount", SAMPLE_LEDGER);
        List<StatementNode> statements = ledger.getStatements();

        assertEquals(14, statements.size(), "Should parse 14 significant statements (includes + directives)");

        OpenDirectiveNode first = assertInstanceOf(OpenDirectiveNode.class, statements.get(0));
        assertEquals("2019-01-01", first.getDate());
        assertEquals("open", first.getDirectiveType());
        assertEquals("Assets:Cash", first.getAccount());
        assertEquals(List.of("USD", "EUR"), first.getCurrencies());

        IncludeNode include = assertInclude(statements.get(1));
        assertTrue(!include.isIncludeOnce());
        assertEquals("secondary.beancount", include.getPath());

        PadDirectiveNode pad = assertInstanceOf(PadDirectiveNode.class, statements.get(2));
        assertEquals("pad", pad.getDirectiveType());
        assertEquals("Assets:Cash", pad.getAccount());
        assertEquals("Equity:Opening", pad.getSourceAccount());

        DirectiveNode txn = assertDirective(statements.get(3));
        assertEquals("*", txn.getDirectiveType());
        assertEquals("2019-01-02", txn.getDate());
        assertEquals(
                List.of(
                        "\"Coffee\" \"Beans\" #breakfast",
                        "; #coffee ^onetime",
                        "Expenses:Coffee 5 USD",
                        "Assets:Cash"),
                txn.getContentLines());

        DirectiveNode custom = assertDirective(statements.get(7));
        assertEquals("custom", custom.getDirectiveType());
        assertEquals(List.of("\"ignored\" \"placeholder\""), custom.getContentLines());

        NoteDirectiveNode note = assertInstanceOf(NoteDirectiveNode.class, statements.get(8));
        assertEquals("Assets:Cash", note.getAccount());
        assertEquals("FirstNote", note.getComment());

        DocumentDirectiveNode document = assertInstanceOf(DocumentDirectiveNode.class, statements.get(9));
        assertEquals("Assets:Cash", document.getAccount());
        assertEquals("receipts/invoice.pdf", document.getFilename());

        PriceDirectiveNode price = assertInstanceOf(PriceDirectiveNode.class, statements.get(12));
        assertEquals("USD", price.getCurrency());
        assertEquals("CAD", price.getAmountCurrency());
    }

    @Test
    void directiveHeaderSplitsLeadingContentIntoTypeAndRemainder() throws Exception {
        String input = "2018-01-01 open Assets:Bank:Personal:Checking USD\n";
        BeancountAstBuilder builder = new BeancountAstBuilder();
        LedgerNode ledger = builder.parse("accounts.personal", input);

        assertEquals(1, ledger.getStatements().size());
        OpenDirectiveNode directive = assertInstanceOf(OpenDirectiveNode.class, ledger.getStatements().get(0));
        assertEquals("2018-01-01", directive.getDate());
        assertEquals("open", directive.getDirectiveType());
        assertEquals("Assets:Bank:Personal:Checking", directive.getAccount());
        assertEquals(List.of("USD"), directive.getCurrencies());
    }

    @Test
    void transactionHeaderCapturesPayeeFromStringLiteral() throws Exception {
        String input = "2022-01-01 * \"Balance Forward\"\n"
                + "  Assets:Bank:Cash  10 USD\n"
                + "  Equity:Opening-Balances  -10 USD\n";
        BeancountAstBuilder builder = new BeancountAstBuilder();
        LedgerNode ledger = builder.parse("accounts.personal", input);

        assertEquals(1, ledger.getStatements().size());
        DirectiveNode directive = assertDirective(ledger.getStatements().get(0));
        assertEquals("*", directive.getDirectiveType());
        assertEquals(List.of("\"Balance Forward\"", "Assets:Bank:Cash  10 USD", "Equity:Opening-Balances  -10 USD"),
                directive.getContentLines());
    }

    @Test
    void parsesStructuredPostingsMetadataAndComments() throws Exception {
        String input = "2024-01-01 * \"Run\"\n"
                + "  Assets:Cash 10 USD {100 EUR, 2024-01-02, \"Lot-1\"} @ 2 CAD ; inline note\n"
                + "    memo: \"Lot memo\"\n"
                + "  ; transaction level comment\n"
                + "  description: \"Txn description\"\n"
                + "  Expenses:Food -10 USD\n"
                + "    note: \"Posting meta\"\n";

        BeancountAstBuilder builder = new BeancountAstBuilder();
        LedgerNode ledger = builder.parse("txn.ledger", input);

        TransactionNode txn = assertInstanceOf(TransactionNode.class, ledger.getStatements().get(0));
        assertEquals("Run", txn.getPayee());
        assertEquals(List.of("transaction level comment"), txn.getComments());
        assertEquals(1, txn.getMetadata().size());
        assertEquals("description", txn.getMetadata().get(0).getKey());
        assertEquals("Txn description", txn.getMetadata().get(0).getValue());

        assertEquals(2, txn.getPostings().size());
        PostingNode first = txn.getPostings().get(0);
        assertEquals("Assets:Cash", first.getAccount());
        assertEquals(0, first.getAmountNumber().compareTo(new BigDecimal("10")));
        assertEquals("USD", first.getAmountCurrency());
        assertEquals(0, first.getCostNumber().compareTo(new BigDecimal("100")));
        assertEquals("EUR", first.getCostCurrency());
        assertEquals(LocalDate.parse("2024-01-02"), first.getCostDate());
        assertEquals("Lot-1", first.getCostLabel());
        assertEquals(0, first.getPriceNumber().compareTo(new BigDecimal("2")));
        assertEquals("CAD", first.getPriceCurrency());
        assertEquals(List.of("inline note"), first.getComments());
        assertEquals(1, first.getMetadata().size());
        assertEquals("memo", first.getMetadata().get(0).getKey());
        assertEquals("Lot memo", first.getMetadata().get(0).getValue());

        PostingNode second = txn.getPostings().get(1);
        assertEquals("Expenses:Food", second.getAccount());
        assertEquals(1, second.getMetadata().size());
        assertEquals("note", second.getMetadata().get(0).getKey());
        assertEquals("Posting meta", second.getMetadata().get(0).getValue());
    }

    private static DirectiveNode assertDirective(StatementNode node) {
        return assertInstanceOf(DirectiveNode.class, node);
    }

    private static IncludeNode assertInclude(StatementNode node) {
        return assertInstanceOf(IncludeNode.class, node);
    }
}
