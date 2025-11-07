package com.beancount.jdbc.loader;

/**
 * Checked exception signalling that the loader failed to parse or evaluate a Beancount ledger.
 * This wraps IO failures for now and will expand to report semantic errors in future milestones.
 */
public final class LoaderException extends Exception {
    public LoaderException(String message) {
        super(message);
    }

    public LoaderException(String message, Throwable cause) {
        super(message, cause);
    }
}
