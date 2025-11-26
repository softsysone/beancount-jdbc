package com.beancount.jdbc.loader.semantic;

import java.util.Objects;

public final class SemanticMetadataEntry {
    private final String key;
    private final String value;

    public SemanticMetadataEntry(String key, String value) {
        this.key = Objects.requireNonNull(key, "key");
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
