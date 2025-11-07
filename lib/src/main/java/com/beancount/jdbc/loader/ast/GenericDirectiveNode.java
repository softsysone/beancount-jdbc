package com.beancount.jdbc.loader.ast;

import java.util.List;

public final class GenericDirectiveNode extends DirectiveNode {

    public GenericDirectiveNode(
            SourceLocation location, String date, String directiveType, List<String> contentLines) {
        super(location, date, directiveType, contentLines);
    }
}
