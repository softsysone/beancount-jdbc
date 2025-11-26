package com.beancount.jdbc.loader;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beancount.jdbc.testing.TestResources;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

final class BeancountLoaderSmokeTest {

    @Test
    void loadsExampleLedgerEntries() throws Exception {
        Path ledger = TestResources.absolutePath("third_party/beancount/examples/example.beancount");
        LoaderResult result = new BeancountLoader().load(ledger);
        assertTrue(
                result.getLedgerData().getEntries().size() > 0,
                "Expected entries when loading " + ledger);
    }
}
