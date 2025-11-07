package com.beancount.jdbc.ledger;

public final class DocumentRecord {
    private final int entryId;
    private final String account;
    private final String filename;

    public DocumentRecord(int entryId, String account, String filename) {
        this.entryId = entryId;
        this.account = account;
        this.filename = filename;
    }

    public int getEntryId() {
        return entryId;
    }

    public String getAccount() {
        return account;
    }

    public String getFilename() {
        return filename;
    }
}
