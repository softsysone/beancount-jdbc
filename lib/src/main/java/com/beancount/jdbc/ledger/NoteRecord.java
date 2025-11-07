package com.beancount.jdbc.ledger;

public final class NoteRecord {
    private final int entryId;
    private final String account;
    private final String comment;

    public NoteRecord(int entryId, String account, String comment) {
        this.entryId = entryId;
        this.account = account;
        this.comment = comment;
    }

    public int getEntryId() {
        return entryId;
    }

    public String getAccount() {
        return account;
    }

    public String getComment() {
        return comment;
    }
}
