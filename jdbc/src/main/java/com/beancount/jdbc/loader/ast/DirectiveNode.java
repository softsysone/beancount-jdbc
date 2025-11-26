package com.beancount.jdbc.loader.ast;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

public sealed abstract class DirectiveNode implements StatementNode
        permits GenericDirectiveNode,
                OpenDirectiveNode,
                CloseDirectiveNode,
                PadDirectiveNode,
                BalanceDirectiveNode,
                NoteDirectiveNode,
                DocumentDirectiveNode,
                EventDirectiveNode,
                QueryDirectiveNode,
                PriceDirectiveNode {

    private final SourceLocation location;
    private final String date;
    private final String directiveType;
    private final List<String> contentLines;

    protected DirectiveNode(
            SourceLocation location, String date, String directiveType, List<String> contentLines) {
        this.location = Objects.requireNonNull(location, "location");
        this.date = Objects.requireNonNull(date, "date");
        this.directiveType = Objects.requireNonNull(directiveType, "directiveType");
        this.contentLines = List.copyOf(contentLines);
    }

    public SourceLocation getLocation() {
        return location;
    }

    public String getDate() {
        return date;
    }

    public String getDirectiveType() {
        return directiveType;
    }

    public String getNormalizedDirectiveType() {
        return directiveType.toLowerCase(Locale.ROOT);
    }

    public List<String> getContentLines() {
        return contentLines;
    }
}
