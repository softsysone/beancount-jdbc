package com.beancount.jdbc.loader.semantic;

import com.beancount.jdbc.loader.ast.SourceLocation;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

public final class SemanticTransaction {
    private final LocalDate date;
    private final String directiveType;
    private final String flag;
    private final String payee;
    private final String narration;
    private final List<String> tags;
    private final List<String> links;
    private final List<SemanticMetadataEntry> metadata;
    private final List<SemanticPosting> postings;
    private final List<String> comments;
    private final SourceLocation location;

    public SemanticTransaction(
            LocalDate date,
            String directiveType,
            String flag,
            String payee,
            String narration,
            List<String> tags,
            List<String> links,
            List<SemanticMetadataEntry> metadata,
            List<SemanticPosting> postings,
            List<String> comments,
            SourceLocation location) {
        this.date = Objects.requireNonNull(date, "date");
        this.directiveType = Objects.requireNonNull(directiveType, "directiveType");
        this.flag = flag;
        this.payee = payee;
        this.narration = narration;
        this.tags = List.copyOf(tags);
        this.links = List.copyOf(links);
        this.metadata = List.copyOf(metadata);
        this.postings = List.copyOf(postings);
        this.comments = List.copyOf(comments);
        this.location = Objects.requireNonNull(location, "location");
    }

    public LocalDate getDate() {
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

    public List<SemanticMetadataEntry> getMetadata() {
        return metadata;
    }

    public List<SemanticPosting> getPostings() {
        return postings;
    }

    public List<String> getComments() {
        return comments;
    }

    public SourceLocation getLocation() {
        return location;
    }
}
