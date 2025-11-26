package com.beancount.jdbc.ledger;

public final class PadRecord {
    private final int entryId;
    private final String account;
    private final String sourceAccount;

    public PadRecord(int entryId, String account, String sourceAccount) {
        this.entryId = entryId;
        this.account = account;
        this.sourceAccount = sourceAccount;
    }

    public int getEntryId() {
        return entryId;
    }

    public String getAccount() {
        return account;
    }

    public String getSourceAccount() {
        return sourceAccount;
    }
}
