package com.beancount.jdbc.ledger;

import java.math.BigDecimal;

public final class PriceRecord {
    private final int entryId;
    private final String currency;
    private final BigDecimal amountNumber;
    private final String amountCurrency;

    public PriceRecord(int entryId, String currency, BigDecimal amountNumber, String amountCurrency) {
        this.entryId = entryId;
        this.currency = currency;
        this.amountNumber = amountNumber;
        this.amountCurrency = amountCurrency;
    }

    public int getEntryId() {
        return entryId;
    }

    public String getCurrency() {
        return currency;
    }

    public BigDecimal getAmountNumber() {
        return amountNumber;
    }

    public String getAmountCurrency() {
        return amountCurrency;
    }
}
