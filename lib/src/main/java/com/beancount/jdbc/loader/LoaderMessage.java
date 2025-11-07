package com.beancount.jdbc.loader;

/**
 * Represents a diagnostic produced while loading a Beancount ledger. This is intentionally lightweight for the
 * initial loader skeleton and will be extended as the Beancount-compatible pipeline is implemented.
 */
public final class LoaderMessage {

    public enum Level {
        INFO,
        WARNING,
        ERROR
    }

    private final Level level;
    private final String message;
    private final String sourceFilename;
    private final int sourceLineno;

    public LoaderMessage(Level level, String message, String sourceFilename, int sourceLineno) {
        this.level = level;
        this.message = message;
        this.sourceFilename = sourceFilename;
        this.sourceLineno = sourceLineno;
    }

    public Level getLevel() {
        return level;
    }

    public String getMessage() {
        return message;
    }

    public String getSourceFilename() {
        return sourceFilename;
    }

    public int getSourceLineno() {
        return sourceLineno;
    }
}
