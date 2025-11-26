package com.beancount.jdbc.loader.ast;

import java.util.List;

public final class OpenDirectiveNode extends DirectiveNode {

    private final String account;
    private final List<String> currencies;

    public OpenDirectiveNode(
            SourceLocation location,
            String date,
            String directiveType,
            List<String> contentLines,
            String account,
            List<String> currencies) {
        super(location, date, directiveType, contentLines);
        this.account = account == null ? "" : account;
        this.currencies = List.copyOf(currencies);
    }

    public String getAccount() {
        return account;
    }

    public List<String> getCurrencies() {
        return currencies;
    }
}
