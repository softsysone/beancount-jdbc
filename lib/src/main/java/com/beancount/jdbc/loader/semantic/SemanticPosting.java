package com.beancount.jdbc.loader.semantic;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

public final class SemanticPosting {
    private final String account;
    private final BigDecimal number;
    private final String currency;
    private final BigDecimal costNumber;
    private final String costCurrency;
    private final BigDecimal priceNumber;
    private final String priceCurrency;
    private final List<SemanticMetadataEntry> metadata;
    private final List<String> comments;

    public SemanticPosting(
            String account,
            BigDecimal number,
            String currency,
            BigDecimal costNumber,
            String costCurrency,
            BigDecimal priceNumber,
            String priceCurrency,
            List<SemanticMetadataEntry> metadata,
            List<String> comments) {
        this.account = Objects.requireNonNull(account, "account");
        this.number = number;
        this.currency = currency;
        this.costNumber = costNumber;
        this.costCurrency = costCurrency;
        this.priceNumber = priceNumber;
        this.priceCurrency = priceCurrency;
        this.metadata = metadata == null ? List.of() : List.copyOf(metadata);
        this.comments = comments == null ? List.of() : List.copyOf(comments);
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

    public List<SemanticMetadataEntry> getMetadata() {
        return metadata;
    }

    public List<String> getComments() {
        return comments;
    }
}
