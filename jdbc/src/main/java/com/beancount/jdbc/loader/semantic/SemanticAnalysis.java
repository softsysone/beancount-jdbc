package com.beancount.jdbc.loader.semantic;

import com.beancount.jdbc.ledger.LedgerData;
import com.beancount.jdbc.loader.LoaderMessage;
import java.util.List;

public final class SemanticAnalysis {
    private final LedgerData ledgerData;
    private final SemanticLedger ledger;
    private final List<LoaderMessage> messages;

    public SemanticAnalysis(LedgerData ledgerData, SemanticLedger ledger, List<LoaderMessage> messages) {
        this.ledgerData = ledgerData;
        this.ledger = ledger;
        this.messages = List.copyOf(messages);
    }

    public LedgerData getLedgerData() {
        return ledgerData;
    }

    public SemanticLedger getLedger() {
        return ledger;
    }

    public List<LoaderMessage> getMessages() {
        return messages;
    }
}
