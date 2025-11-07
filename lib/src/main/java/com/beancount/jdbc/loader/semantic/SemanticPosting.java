package com.beancount.jdbc.loader.semantic;

import java.math.BigDecimal;
import java.util.Objects;

public final class SemanticPosting {
    private final String account;
    private final BigDecimal number;
    private final String currency;
    private final BigDecimal costNumber;
    private final String costCurrency;
    private final BigDecimal priceNumber;
    private final String priceCurrency;

    public SemanticPosting(
            String account,
            BigDecimal number,
            String currency,
            BigDecimal costNumber,
            String costCurrency,
            BigDecimal priceNumber,
            String priceCurrency) {
        this.account = Objects.requireNonNull(account, "account");
        this.number = number;
        this.currency = currency;
        this.costNumber = costNumber;
        this.costCurrency = costCurrency;
        this.priceNumber = priceNumber;
        this.priceCurrency = priceCurrency;
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

    public BigDecimal getPriceNumber() {
        return priceNumber;
    }

    public String getPriceCurrency() {
        return priceCurrency;
    }
}
