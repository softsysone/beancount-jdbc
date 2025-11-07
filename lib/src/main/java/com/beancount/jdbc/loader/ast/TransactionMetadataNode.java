package com.beancount.jdbc.loader.ast;

import java.util.Objects;

public final class TransactionMetadataNode {
    private final String key;
    private final String value;
    private final SourceLocation location;

    public TransactionMetadataNode(String key, String value, SourceLocation location) {
        this.key = Objects.requireNonNull(key, "key");
        this.value = value;
        this.location = location;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public SourceLocation getLocation() {
        return location;
    }
}
