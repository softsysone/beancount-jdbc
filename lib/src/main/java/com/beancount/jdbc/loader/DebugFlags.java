package com.beancount.jdbc.loader;

import com.beancount.jdbc.loader.grammar.BeancountLexer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;

public final class DebugFlags {
    private static final String TOKENS_PROPERTY = "beancount.jdbc.debugTokens";
    private static final String PARSER_PROPERTY = "beancount.jdbc.debugParser";
    /** Environment fallback kept for convenience; prefer using system properties. */
    private static final String TOKENS_ENV = "BEANCOUNT_JDBC_DEBUG_TOKENS";
    private static final String PARSER_ENV = "BEANCOUNT_JDBC_DEBUG_PARSER";
    private static final boolean FORCE_TOKEN_DEBUG = false;
    private static final boolean FORCE_PARSER_DEBUG = false;
    private static final ThreadLocal<List<String>> CAPTURED_TOKENS =
            ThreadLocal.withInitial(ArrayList::new);
    private static final ThreadLocal<List<String>> CAPTURED_DIAGNOSTICS =
            ThreadLocal.withInitial(ArrayList::new);

    private DebugFlags() {}

    public static boolean isTokenDebugEnabled() {
        if (FORCE_TOKEN_DEBUG) {
            return true;
        }
        String value = System.getProperty(TOKENS_PROPERTY);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return Boolean.parseBoolean(System.getenv(TOKENS_ENV));
    }

    public static boolean isParserTraceEnabled() {
        if (FORCE_PARSER_DEBUG) {
            return true;
        }
        String value = System.getProperty(PARSER_PROPERTY);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return Boolean.parseBoolean(System.getenv(PARSER_ENV));
    }

    public static void logTokens(CommonTokenStream tokens, BeancountLexer lexer) {
        System.err.println("[Beancount JDBC] Token dump for debugging:");
        for (Token token : tokens.getTokens()) {
            String symbolic = lexer.getVocabulary().getSymbolicName(token.getType());
            if (symbolic == null) {
                symbolic = String.format(Locale.ROOT, "#%d", token.getType());
            }
            String line =
                    String.format(
                            Locale.ROOT,
                            "%-25s @ %4d:%-3d -> %s",
                            symbolic,
                            token.getLine(),
                            token.getCharPositionInLine(),
                            token.getText());
            System.err.printf(Locale.ROOT, "  %s%n", line);
            CAPTURED_TOKENS.get().add(line);
        }
    }

    public static DebugDiagnosticErrorListener diagnosticListener() {
        return new DebugDiagnosticErrorListener();
    }

    public static List<String> drainCapturedTokens() {
        List<String> captured = new ArrayList<>(CAPTURED_TOKENS.get());
        CAPTURED_TOKENS.get().clear();
        return captured;
    }

    public static void captureDiagnostic(String message) {
        CAPTURED_DIAGNOSTICS.get().add(message);
    }

    public static List<String> drainCapturedDiagnostics() {
        List<String> captured = new ArrayList<>(CAPTURED_DIAGNOSTICS.get());
        CAPTURED_DIAGNOSTICS.get().clear();
        return captured;
    }
}
