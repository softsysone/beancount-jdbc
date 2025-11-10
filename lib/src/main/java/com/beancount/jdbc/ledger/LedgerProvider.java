package com.beancount.jdbc.ledger;

import com.beancount.jdbc.loader.BeancountLoader;
import com.beancount.jdbc.loader.LoaderException;
import com.beancount.jdbc.loader.LoaderResult;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Shared entry point for loading Beancount ledgers.
 *
 * <p>This thin wrapper around {@link BeancountLoader} makes it easy for both the legacy JDBC path
 * and the upcoming Calcite-backed implementation to reuse the same parsing pipeline without
 * duplicating instantiation logic.</p>
 */
public final class LedgerProvider {

    private LedgerProvider() {}

    public static LoaderResult load(Path ledgerPath) throws LoaderException {
        Objects.requireNonNull(ledgerPath, "ledgerPath");
        return new BeancountLoader().load(ledgerPath);
    }
}
