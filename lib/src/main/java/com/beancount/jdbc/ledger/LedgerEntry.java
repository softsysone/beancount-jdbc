package com.beancount.jdbc.ledger;

import java.time.LocalDate;
import java.util.Objects;

/**
 * Represents a single directive entry extracted from a Beancount ledger.
 */
public final class LedgerEntry {
    private final int id;
    private final LocalDate date;
    private final String type;
    private final String sourceFilename;
    private final int sourceLineno;
    private final TransactionPayload transactionPayload;

    public LedgerEntry(
            int id,
            LocalDate date,
            String type,
            String sourceFilename,
            int sourceLineno,
            TransactionPayload transactionPayload) {
        this.id = id;
        this.date = Objects.requireNonNull(date, "date");
        this.type = Objects.requireNonNull(type, "type");
        this.sourceFilename = Objects.requireNonNull(sourceFilename, "sourceFilename");
        this.sourceLineno = sourceLineno;
        this.transactionPayload = transactionPayload;
    }

    public int getId() {
        return id;
    }

    public LocalDate getDate() {
        return date;
    }

    public String getType() {
        return type;
    }

    public String getSourceFilename() {
        return sourceFilename;
    }

    public int getSourceLineno() {
        return sourceLineno;
    }

    public TransactionPayload getTransactionPayload() {
        return transactionPayload;
    }

    public LedgerEntry withTransactionPayload(TransactionPayload payload) {
        return new LedgerEntry(id, date, type, sourceFilename, sourceLineno, payload);
    }
}
