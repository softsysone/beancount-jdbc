package com.beancount.jdbc.loader;

import com.beancount.jdbc.loader.semantic.SemanticAnalysis;
import com.beancount.jdbc.loader.semantic.SemanticAnalyzer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Entry point for loading Beancount ledgers via the ANTLR-backed evaluation pipeline. */
public final class BeancountLoader {

    public LoaderResult load(Path ledgerPath) throws LoaderException {
        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledgerPath);
        List<LoaderMessage> messages = new ArrayList<>(analysis.getMessages());
        return new LoaderResult(analysis.getLedgerData(), messages, analysis.getLedger());
    }
}
