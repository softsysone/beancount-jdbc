package com.beancount.jdbc.ledger;

public final class QueryRecord {
    private final int entryId;
    private final String name;
    private final String queryString;

    public QueryRecord(int entryId, String name, String queryString) {
        this.entryId = entryId;
        this.name = name;
        this.queryString = queryString;
    }

    public int getEntryId() {
        return entryId;
    }

    public String getName() {
        return name;
    }

    public String getQueryString() {
        return queryString;
    }
}
