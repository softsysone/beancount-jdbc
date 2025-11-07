package com.beancount.jdbc.ledger;

import java.math.BigDecimal;
import java.time.LocalDate;

public final class PostingRecord {
    private final int postingId;
    private final int entryId;
    private final String flag;
    private final String account;
    private final BigDecimal number;
    private final String currency;
    private final BigDecimal costNumber;
    private final String costCurrency;
    private final LocalDate costDate;
    private final String costLabel;
    private final BigDecimal priceNumber;
    private final String priceCurrency;

    public PostingRecord(
            int postingId,
            int entryId,
            String flag,
            String account,
            BigDecimal number,
            String currency,
            BigDecimal costNumber,
            String costCurrency,
            LocalDate costDate,
            String costLabel,
            BigDecimal priceNumber,
            String priceCurrency) {
        this.postingId = postingId;
        this.entryId = entryId;
        this.flag = flag;
        this.account = account;
        this.number = number;
        this.currency = currency;
        this.costNumber = costNumber;
        this.costCurrency = costCurrency;
        this.costDate = costDate;
        this.costLabel = costLabel;
        this.priceNumber = priceNumber;
        this.priceCurrency = priceCurrency;
    }

    public int getPostingId() {
        return postingId;
    }

    public int getEntryId() {
        return entryId;
    }

    public String getFlag() {
        return flag;
    }

    public String getAccount() {
        return account;
    }

    public BigDecimal getNumber() {
        return number;
    }

    public String getCurrency() {
        return currency;
    }

    public BigDecimal getCostNumber() {
        return costNumber;
    }

    public String getCostCurrency() {
        return costCurrency;
    }

    public LocalDate getCostDate() {
        return costDate;
    }

    public String getCostLabel() {
        return costLabel;
    }

    public BigDecimal getPriceNumber() {
        return priceNumber;
    }

    public String getPriceCurrency() {
        return priceCurrency;
    }
}
