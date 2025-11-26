package com.beancount.jdbc.loader.validation;

import com.beancount.jdbc.loader.LoaderMessage;
import com.beancount.jdbc.loader.semantic.SemanticAnalysis;
import java.util.List;

/**
 * A single validation rule that inspects the semantic analysis output and emits diagnostics. Rules
 * are expected to be deterministic and preserve the order they discover problems in, so results can
 * be compared directly against Beancount's checker.
 */
public interface ValidationRule {

    /**
     * Evaluate this rule against the given analysis.
     *
     * @param analysis Parsed and normalized ledger data.
     * @return A list of diagnostics, possibly empty. Implementations must not return null.
     */
    List<LoaderMessage> validate(SemanticAnalysis analysis);
}
