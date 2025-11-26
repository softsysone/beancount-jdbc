package com.beancount.jdbc.loader.validation;

import com.beancount.jdbc.loader.LoaderMessage;
import com.beancount.jdbc.loader.semantic.SemanticAnalysis;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Executes a list of validation rules and aggregates their diagnostics. This keeps a single place
 * to register rules so we can expand parity coverage incrementally.
 */
public final class ValidationRunner {

    private final List<ValidationRule> rules;

    public ValidationRunner(List<ValidationRule> rules) {
        this.rules = List.copyOf(Objects.requireNonNull(rules, "rules"));
    }

    /**
     * Convenience factory that wires in the default rule set.
     */
    public static ValidationRunner defaultRules() {
        return new ValidationRunner(List.of(new AccountNameValidationRule()));
    }

    /**
     * Run all configured rules against the provided analysis.
     *
     * @param analysis Parsed and normalized ledger data.
     * @return All diagnostics produced by all rules, in rule order.
     */
    public List<LoaderMessage> run(SemanticAnalysis analysis) {
        List<LoaderMessage> diagnostics = new ArrayList<>();
        for (ValidationRule rule : rules) {
            diagnostics.addAll(rule.validate(analysis));
        }
        return diagnostics;
    }
}
