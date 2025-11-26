package com.beancount.jdbc.loader.ast;

import java.util.List;
import java.util.Objects;

public final class GlobalDirectiveNode implements StatementNode {
    private final SourceLocation location;
    private final String directiveType;
    private final List<String> arguments;

    public GlobalDirectiveNode(SourceLocation location, String directiveType, List<String> arguments) {
        this.location = Objects.requireNonNull(location, "location");
        this.directiveType = Objects.requireNonNull(directiveType, "directiveType");
        this.arguments = arguments == null ? List.of() : List.copyOf(arguments);
    }

    public SourceLocation getLocation() {
        return location;
    }

    public String getDirectiveType() {
        return directiveType;
    }

    public List<String> getArguments() {
        return arguments;
    }
}
