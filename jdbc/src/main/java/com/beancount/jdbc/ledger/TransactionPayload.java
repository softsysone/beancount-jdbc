package com.beancount.jdbc.ledger;

import java.util.Objects;

public final class TransactionPayload {
    private final String flag;
    private final String payee;
    private final String narration;
    private final String tags;
    private final String links;

    public TransactionPayload(String flag, String payee, String narration, String tags, String links) {
        this.flag = flag;
        this.payee = payee;
        this.narration = narration;
        this.tags = tags;
        this.links = links;
    }

    public String getFlag() {
        return flag;
    }

    public String getPayee() {
        return payee;
    }

    public String getNarration() {
        return narration;
    }

    public String getTags() {
        return tags;
    }

    public String getLinks() {
        return links;
    }

    public boolean isPresent() {
        return flag != null || payee != null || narration != null || tags != null || links != null;
    }
}
