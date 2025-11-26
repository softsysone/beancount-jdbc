package com.beancount.jdbc.loader.ast;

import java.util.List;

public final class NoteDirectiveNode extends DirectiveNode {

    private final String account;
    private final String comment;

    public NoteDirectiveNode(
            SourceLocation location,
            String date,
            String directiveType,
            List<String> contentLines,
            String account,
            String comment) {
        super(location, date, directiveType, contentLines);
        this.account = account == null ? "" : account;
        this.comment = comment;
    }

    public String getAccount() {
        return account;
    }

    public String getComment() {
        return comment;
    }
}
