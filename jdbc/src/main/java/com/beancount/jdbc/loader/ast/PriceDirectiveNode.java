package com.beancount.jdbc.loader.ast;

import java.math.BigDecimal;
import java.util.List;

public final class PriceDirectiveNode extends DirectiveNode {

    private final String currency;
    private final BigDecimal amountNumber;
    private final String amountCurrency;

    public PriceDirectiveNode(
            SourceLocation location,
            String date,
            String directiveType,
            List<String> contentLines,
            String currency,
            BigDecimal amountNumber,
            String amountCurrency) {
        super(location, date, directiveType, contentLines);
        this.currency = currency;
        this.amountNumber = amountNumber;
        this.amountCurrency = amountCurrency;
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
