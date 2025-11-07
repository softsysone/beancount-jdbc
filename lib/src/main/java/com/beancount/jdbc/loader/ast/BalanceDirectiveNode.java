package com.beancount.jdbc.loader.ast;

import java.math.BigDecimal;
import java.util.List;

public final class BalanceDirectiveNode extends DirectiveNode {

    private final String account;
    private final BigDecimal amountNumber;
    private final String amountCurrency;
    private final BigDecimal diffNumber;
    private final String diffCurrency;
    private final BigDecimal toleranceNumber;
    private final String toleranceCurrency;

    public BalanceDirectiveNode(
            SourceLocation location,
            String date,
            String directiveType,
            List<String> contentLines,
            String account,
            BigDecimal amountNumber,
            String amountCurrency,
            BigDecimal diffNumber,
            String diffCurrency,
            BigDecimal toleranceNumber,
            String toleranceCurrency) {
        super(location, date, directiveType, contentLines);
        this.account = account == null ? "" : account;
        this.amountNumber = amountNumber;
        this.amountCurrency = amountCurrency;
        this.diffNumber = diffNumber;
        this.diffCurrency = diffCurrency;
        this.toleranceNumber = toleranceNumber;
        this.toleranceCurrency = toleranceCurrency;
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
