package com.beancount.jdbc.loader;

import java.util.BitSet;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.DiagnosticErrorListener;
import org.antlr.v4.runtime.atn.ATNConfigSet;

/**
 * Diagnostic listener that records parser diagnostics without propagating them as errors.
 *
 * <p>ANTLR's {@link DiagnosticErrorListener} reports context sensitivity and ambiguity issues by
 * calling {@link Parser#notifyErrorListeners(String)}, which triggers our throwing error listener
 * and aborts the parse. For debugging we only want to capture these messages, so this subclass
 * mirrors the formatting logic but routes the output through {@link DebugFlags} instead.</p>
 */
final class DebugDiagnosticErrorListener extends DiagnosticErrorListener {

    DebugDiagnosticErrorListener() {
        super(true);
    }

    @Override
    public void reportAmbiguity(
            Parser recognizer,
            DFA dfa,
            int startIndex,
            int stopIndex,
            boolean exact,
            BitSet ambigAlts,
            ATNConfigSet configs) {
        if (super.exactOnly && !exact) {
            return;
        }
        String decision = getDecisionDescription(recognizer, dfa);
        BitSet conflicting = getConflictingAlts(ambigAlts, configs);
        String input = recognizer.getTokenStream().getText(Interval.of(startIndex, stopIndex));
        DebugFlags.captureDiagnostic(
                String.format("reportAmbiguity d=%s: ambigAlts=%s, input='%s'", decision, conflicting, input));
    }

    @Override
    public void reportAttemptingFullContext(
            Parser recognizer,
            DFA dfa,
            int startIndex,
            int stopIndex,
            BitSet conflictingAlts,
            ATNConfigSet configs) {
        String decision = getDecisionDescription(recognizer, dfa);
        String input = recognizer.getTokenStream().getText(Interval.of(startIndex, stopIndex));
        DebugFlags.captureDiagnostic(
                String.format("reportAttemptingFullContext d=%s, input='%s'", decision, input));
    }

    @Override
    public void reportContextSensitivity(
            Parser recognizer,
            DFA dfa,
            int startIndex,
            int stopIndex,
            int prediction,
            ATNConfigSet configs) {
        String decision = getDecisionDescription(recognizer, dfa);
        String input = recognizer.getTokenStream().getText(Interval.of(startIndex, stopIndex));
        DebugFlags.captureDiagnostic(
                String.format("reportContextSensitivity d=%s, input='%s'", decision, input));
    }
}
