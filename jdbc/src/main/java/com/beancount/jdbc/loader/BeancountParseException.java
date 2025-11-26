package com.beancount.jdbc.loader;

public final class BeancountParseException extends Exception {
    public BeancountParseException(String message) {
        super(message);
    }

    public BeancountParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
