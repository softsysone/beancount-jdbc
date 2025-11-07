package com.beancount.jdbc.loader.ast;

import java.util.List;
import java.util.Objects;

public final class LedgerNode {
    private final List<StatementNode> statements;

    public LedgerNode(List<StatementNode> statements) {
        this.statements = List.copyOf(statements);
    }

    public List<StatementNode> getStatements() {
        return statements;
    }
}
