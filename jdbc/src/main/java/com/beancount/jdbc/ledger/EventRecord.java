package com.beancount.jdbc.ledger;

public final class EventRecord {
    private final int entryId;
    private final String type;
    private final String description;

    public EventRecord(int entryId, String type, String description) {
        this.entryId = entryId;
        this.type = type;
        this.description = description;
    }

    public int getEntryId() {
        return entryId;
    }

    public String getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }
}
