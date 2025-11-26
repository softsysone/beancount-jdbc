package com.beancount.jdbc.loader;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.beancount.jdbc.testing.TestResources;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

final class BeancountLoaderValidationTest {

    @Test
    void rejectsInvalidAccountName() {
        Path ledger = TestResources.absolutePath("jdbc/src/test/resources/validation/invalid-account.beancount");
        assertThrows(LoaderException.class, () -> new BeancountLoader().load(ledger));
    }
}
