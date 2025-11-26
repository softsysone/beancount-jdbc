package com.beancount.jdbc.ledger;

import java.util.List;

public final class OpenRecord {
    private final int entryId;
    private final String account;
    private final List<String> currencies;

    public OpenRecord(int entryId, String account, List<String> currencies) {
        this.entryId = entryId;
        this.account = account;
        this.currencies = List.copyOf(currencies);
    }

    public int getEntryId() {
        return entryId;
    }

    public String getAccount() {
        return account;
    }

    public List<String> getCurrencies() {
        return currencies;
    }
}
