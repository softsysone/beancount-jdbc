package com.beancount.jdbc.loader.ast;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class PostingNode {
    private final SourceLocation location;
    private final String flag;
    private final String account;
    private final BigDecimal amountNumber;
    private final String amountCurrency;
    private final BigDecimal costNumber;
    private final String costCurrency;
    private final LocalDate costDate;
    private final String costLabel;
    private final BigDecimal priceNumber;
    private final String priceCurrency;
    private final int indent;
    private final List<TransactionMetadataNode> metadata;
    private final List<String> comments;

    public PostingNode(
            SourceLocation location,
            String flag,
            String account,
            BigDecimal amountNumber,
            String amountCurrency,
            BigDecimal costNumber,
            String costCurrency,
            LocalDate costDate,
            String costLabel,
            BigDecimal priceNumber,
            String priceCurrency,
            int indent,
            List<TransactionMetadataNode> metadata,
            List<String> comments) {
        this.location = location;
        this.flag = flag;
        this.account = Objects.requireNonNull(account, "account");
        this.amountNumber = amountNumber;
        this.amountCurrency = amountCurrency;
        this.costNumber = costNumber;
        this.costCurrency = costCurrency;
        this.costDate = costDate;
        this.costLabel = costLabel;
        this.priceNumber = priceNumber;
        this.priceCurrency = priceCurrency;
        this.indent = indent;
        this.metadata = new ArrayList<>(metadata);
        this.comments = new ArrayList<>(comments);
    }

    public SourceLocation getLocation() {
        return location;
    }

    public String getFlag() {
        return flag;
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

    public int getIndent() {
        return indent;
    }

    public List<TransactionMetadataNode> getMetadata() {
        return metadata;
    }

    public List<String> getComments() {
        return comments;
    }
}
