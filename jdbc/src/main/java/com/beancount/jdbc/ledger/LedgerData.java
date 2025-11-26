package com.beancount.jdbc.ledger;

import java.util.List;

public final class LedgerData {
    private final List<LedgerEntry> entries;
    private final List<PostingRecord> postings;
    private final List<PostingRecord> rawPostings;
    private final List<OpenRecord> opens;
    private final List<CloseRecord> closes;
    private final List<PadRecord> pads;
    private final List<BalanceRecord> balances;
    private final List<NoteRecord> notes;
    private final List<DocumentRecord> documents;
    private final List<EventRecord> events;
    private final List<QueryRecord> queries;
    private final List<PriceRecord> prices;

    public LedgerData(
            List<LedgerEntry> entries,
            List<PostingRecord> postings,
            List<PostingRecord> rawPostings,
            List<OpenRecord> opens,
            List<CloseRecord> closes,
            List<PadRecord> pads,
            List<BalanceRecord> balances,
            List<NoteRecord> notes,
            List<DocumentRecord> documents,
            List<EventRecord> events,
            List<QueryRecord> queries,
            List<PriceRecord> prices) {
        this.entries = entries;
        this.postings = postings;
        this.rawPostings = rawPostings;
        this.opens = opens;
        this.closes = closes;
        this.pads = pads;
        this.balances = balances;
        this.notes = notes;
        this.documents = documents;
        this.events = events;
        this.queries = queries;
        this.prices = prices;
    }

    public List<LedgerEntry> getEntries() {
        return entries;
    }

    public List<PostingRecord> getPostings() {
        return postings;
    }

    public List<PostingRecord> getRawPostings() {
        return rawPostings;
    }

    public List<OpenRecord> getOpens() {
        return opens;
    }

    public List<CloseRecord> getCloses() {
        return closes;
    }

    public List<PadRecord> getPads() {
        return pads;
    }

    public List<BalanceRecord> getBalances() {
        return balances;
    }

    public List<NoteRecord> getNotes() {
        return notes;
    }

    public List<DocumentRecord> getDocuments() {
        return documents;
    }

    public List<EventRecord> getEvents() {
        return events;
    }

    public List<QueryRecord> getQueries() {
        return queries;
    }

    public List<PriceRecord> getPrices() {
        return prices;
    }
}
