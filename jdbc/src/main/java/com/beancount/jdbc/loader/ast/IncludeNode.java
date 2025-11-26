package com.beancount.jdbc.loader.ast;

import java.util.Objects;

public final class IncludeNode implements StatementNode {
    private final SourceLocation location;
    private final boolean includeOnce;
    private final String path;

    public IncludeNode(SourceLocation location, boolean includeOnce, String path) {
        this.location = Objects.requireNonNull(location, "location");
        this.includeOnce = includeOnce;
        this.path = Objects.requireNonNull(path, "path");
    }

    public SourceLocation getLocation() {
        return location;
    }

    public boolean isIncludeOnce() {
        return includeOnce;
    }

    public String getPath() {
        return path;
    }
}
