package com.beancount.jdbc.loader;

import com.beancount.jdbc.ledger.LedgerData;
import com.beancount.jdbc.loader.semantic.SemanticLedger;
import java.util.List;

/**
 * Container for the results of loading a Beancount ledger. Future iterations will surface additional metadata and
 * diagnostics from the evaluation pipeline.
 */
public final class LoaderResult {
    private final LedgerData ledgerData;
    private final List<LoaderMessage> messages;
    private final SemanticLedger semanticLedger;

    public LoaderResult(LedgerData ledgerData, List<LoaderMessage> messages, SemanticLedger semanticLedger) {
        this.ledgerData = ledgerData;
        this.messages = messages;
        this.semanticLedger = semanticLedger;
    }

    public LedgerData getLedgerData() {
        return ledgerData;
    }

    public List<LoaderMessage> getMessages() {
        return messages;
    }

    public SemanticLedger getSemanticLedger() {
        return semanticLedger;
    }
}
