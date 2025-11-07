package com.beancount.jdbc.loader.ast;

import java.util.List;

public final class QueryDirectiveNode extends DirectiveNode {

    private final String name;
    private final String queryString;

    public QueryDirectiveNode(
            SourceLocation location,
            String date,
            String directiveType,
            List<String> contentLines,
            String name,
            String queryString) {
        super(location, date, directiveType, contentLines);
        this.name = name;
        this.queryString = queryString;
    }

    public String getName() {
        return name;
    }

    public String getQueryString() {
        return queryString;
    }
}
