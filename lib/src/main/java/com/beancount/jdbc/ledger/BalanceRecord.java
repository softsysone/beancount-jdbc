package com.beancount.jdbc.ledger;

import java.math.BigDecimal;

public final class BalanceRecord {
    private final int entryId;
    private final String account;
    private final BigDecimal amountNumber;
    private final String amountCurrency;
    private final BigDecimal diffNumber;
    private final String diffCurrency;
    private final BigDecimal toleranceNumber;
    private final String toleranceCurrency;

    public BalanceRecord(
            int entryId,
            String account,
            BigDecimal amountNumber,
            String amountCurrency,
            BigDecimal diffNumber,
            String diffCurrency,
            BigDecimal toleranceNumber,
            String toleranceCurrency) {
        this.entryId = entryId;
        this.account = account;
        this.amountNumber = amountNumber;
        this.amountCurrency = amountCurrency;
        this.diffNumber = diffNumber;
        this.diffCurrency = diffCurrency;
        this.toleranceNumber = toleranceNumber;
        this.toleranceCurrency = toleranceCurrency;
    }

    public int getEntryId() {
        return entryId;
    }

    public String getAccount() {
        return account;
    }

    public BigDecimal getAmountNumber() {
        return amountNumber;
    }

    public String getAmountCurrency() {
        return amountCurrency;
    }

    public BigDecimal getDiffNumber() {
        return diffNumber;
    }

    public String getDiffCurrency() {
        return diffCurrency;
    }

    public BigDecimal getToleranceNumber() {
        return toleranceNumber;
    }

    public String getToleranceCurrency() {
        return toleranceCurrency;
    }
}
