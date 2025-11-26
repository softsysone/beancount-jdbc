package com.beancount.jdbc.loader.ast;

import java.util.List;

public final class PadDirectiveNode extends DirectiveNode {

    private final String account;
    private final String sourceAccount;

    public PadDirectiveNode(
            SourceLocation location,
            String date,
            String directiveType,
            List<String> contentLines,
            String account,
            String sourceAccount) {
        super(location, date, directiveType, contentLines);
        this.account = account == null ? "" : account;
        this.sourceAccount = sourceAccount == null ? "" : sourceAccount;
    }

    public String getAccount() {
        return account;
    }

    public String getSourceAccount() {
        return sourceAccount;
    }
}
