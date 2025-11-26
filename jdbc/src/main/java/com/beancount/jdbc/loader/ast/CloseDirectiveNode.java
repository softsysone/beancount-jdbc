package com.beancount.jdbc.loader.ast;

import java.util.List;

public final class CloseDirectiveNode extends DirectiveNode {

    private final String account;

    public CloseDirectiveNode(
            SourceLocation location,
            String date,
            String directiveType,
            List<String> contentLines,
            String account) {
        super(location, date, directiveType, contentLines);
        this.account = account == null ? "" : account;
    }

    public String getAccount() {
        return account;
    }
}
