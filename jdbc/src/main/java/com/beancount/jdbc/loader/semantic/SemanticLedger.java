package com.beancount.jdbc.loader.semantic;

import com.beancount.jdbc.loader.semantic.display.DisplayContext;
import java.util.List;
import java.util.Set;

public final class SemanticLedger {
    private final List<SemanticTransaction> transactions;
    private final Set<String> openedAccounts;
    private final List<String> operatingCurrencies;
    private final DisplayContext displayContext;

    public SemanticLedger(
            List<SemanticTransaction> transactions,
            Set<String> openedAccounts,
            List<String> operatingCurrencies,
            DisplayContext displayContext) {
        this.transactions = List.copyOf(transactions);
        this.openedAccounts = Set.copyOf(openedAccounts);
        this.operatingCurrencies = List.copyOf(operatingCurrencies);
        this.displayContext = displayContext == null ? new DisplayContext() : displayContext;
    }

    public List<SemanticTransaction> getTransactions() {
        return transactions;
    }

    public Set<String> getOpenedAccounts() {
        return openedAccounts;
    }

    public List<String> getOperatingCurrencies() {
        return operatingCurrencies;
    }

    public DisplayContext getDisplayContext() {
        return displayContext.copy();
    }
}
