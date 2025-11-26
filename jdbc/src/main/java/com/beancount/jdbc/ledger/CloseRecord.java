package com.beancount.jdbc.ledger;

public final class CloseRecord {
    private final int entryId;
    private final String account;

    public CloseRecord(int entryId, String account) {
        this.entryId = entryId;
        this.account = account;
    }

    public int getEntryId() {
        return entryId;
    }

    public String getAccount() {
        return account;
    }
}
