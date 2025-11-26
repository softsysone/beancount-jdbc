package com.beancount.jdbc.loader.ast;

import java.util.List;

public final class EventDirectiveNode extends DirectiveNode {

    private final String eventType;
    private final String description;

    public EventDirectiveNode(
            SourceLocation location,
            String date,
            String directiveType,
            List<String> contentLines,
            String eventType,
            String description) {
        super(location, date, directiveType, contentLines);
        this.eventType = eventType;
        this.description = description;
    }

    public String getEventType() {
        return eventType;
    }

    public String getDescription() {
        return description;
    }
}
