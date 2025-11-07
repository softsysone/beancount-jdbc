package com.beancount.jdbc.loader.ast;

import java.util.Objects;

public final class SourceLocation {
    private final String sourceName;
    private final int line;
    private final int column;

    public SourceLocation(String sourceName, int line, int column) {
        this.sourceName = sourceName;
        this.line = line;
        this.column = column;
    }

    public String getSourceName() {
        return sourceName;
    }

    public int getLine() {
        return line;
    }

    public int getColumn() {
        return column;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SourceLocation)) {
            return false;
        }
        SourceLocation other = (SourceLocation) obj;
        return line == other.line
                && column == other.column
                && Objects.equals(sourceName, other.sourceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceName, line, column);
    }

    @Override
    public String toString() {
        return sourceName + ":" + line + ":" + column;
    }
}
