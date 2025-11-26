package com.beancount.jdbc.loader.ast;

import java.util.List;

public final class DocumentDirectiveNode extends DirectiveNode {

    private final String account;
    private final String filename;

    public DocumentDirectiveNode(
            SourceLocation location,
            String date,
            String directiveType,
            List<String> contentLines,
            String account,
            String filename) {
        super(location, date, directiveType, contentLines);
        this.account = account == null ? "" : account;
        this.filename = filename;
    }

    public String getAccount() {
        return account;
    }

    public String getFilename() {
        return filename;
    }
}
