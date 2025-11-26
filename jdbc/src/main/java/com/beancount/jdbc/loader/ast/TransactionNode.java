package com.beancount.jdbc.loader.ast;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class TransactionNode implements StatementNode {
    private final SourceLocation location;
    private final String date;
    private final String directiveType;
    private final String flag;
    private final String payee;
    private final String narration;
    private final List<String> tags;
    private final List<String> links;
    private final List<TransactionMetadataNode> metadata;
    private final List<PostingNode> postings;
    private final List<String> comments;
    private final boolean usedPipeSeparator;

    public TransactionNode(
            SourceLocation location,
            String date,
            String directiveType,
            String flag,
            String payee,
            String narration,
            List<String> tags,
            List<String> links,
            List<TransactionMetadataNode> metadata,
            List<PostingNode> postings,
            List<String> comments,
            boolean usedPipeSeparator) {
        this.location = Objects.requireNonNull(location, "location");
        this.date = Objects.requireNonNull(date, "date");
        this.directiveType = Objects.requireNonNull(directiveType, "directiveType");
        this.flag = flag;
        this.payee = payee;
        this.narration = narration;
        this.tags = new ArrayList<>(tags);
        this.links = new ArrayList<>(links);
        this.metadata = new ArrayList<>(metadata);
        this.postings = new ArrayList<>(postings);
        this.comments = new ArrayList<>(comments);
        this.usedPipeSeparator = usedPipeSeparator;
    }

    public SourceLocation getLocation() {
        return location;
    }

    public String getDate() {
        return date;
    }

    public String getDirectiveType() {
        return directiveType;
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

    public List<String> getTags() {
        return tags;
    }

    public List<String> getLinks() {
        return links;
    }

    public List<TransactionMetadataNode> getMetadata() {
        return metadata;
    }

    public List<PostingNode> getPostings() {
        return postings;
    }

    public List<String> getComments() {
        return comments;
    }

    public boolean isUsingPipeSeparator() {
        return usedPipeSeparator;
    }
}
