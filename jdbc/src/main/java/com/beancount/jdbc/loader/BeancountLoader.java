package com.beancount.jdbc.loader;

import com.beancount.jdbc.loader.semantic.SemanticAnalysis;
import com.beancount.jdbc.loader.semantic.SemanticAnalyzer;
import com.beancount.jdbc.loader.validation.ValidationRunner;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Entry point for loading Beancount ledgers via the ANTLR-backed evaluation pipeline. */
public final class BeancountLoader {

    public LoaderResult load(Path ledgerPath) throws LoaderException {
        SemanticAnalysis analysis = new SemanticAnalyzer().analyze(ledgerPath);
        List<LoaderMessage> messages = new ArrayList<>(analysis.getMessages());

        List<LoaderMessage> validationMessages = ValidationRunner.defaultRules().run(analysis);
        messages.addAll(validationMessages);
        LoaderMessage firstError =
                validationMessages.stream()
                        .filter(message -> message.getLevel() == LoaderMessage.Level.ERROR)
                        .findFirst()
                        .orElse(null);
        if (firstError != null) {
            String location = firstError.getSourceFilename();
            if (firstError.getSourceLineno() > 0) {
                location = location + ":" + firstError.getSourceLineno();
            }
            throw new LoaderException(
                    "Validation failed: "
                            + firstError.getMessage()
                            + (location.isEmpty() ? "" : " (" + location + ")"));
        }

        return new LoaderResult(analysis.getLedgerData(), messages, analysis.getLedger());
    }
}
