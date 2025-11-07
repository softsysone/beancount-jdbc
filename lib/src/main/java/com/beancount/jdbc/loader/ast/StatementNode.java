package com.beancount.jdbc.loader.ast;

public sealed interface StatementNode permits DirectiveNode, GlobalDirectiveNode, IncludeNode, TransactionNode {}
