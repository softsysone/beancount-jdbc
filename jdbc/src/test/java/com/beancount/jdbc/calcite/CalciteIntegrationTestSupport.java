package com.beancount.jdbc.calcite;

import com.beancount.jdbc.testing.TestResources;
import java.io.IOException;
import java.util.Properties;

/**
 * Small helper for Calcite integration tests so every test builds the same connection properties
 * and resolves ledger files in the same way.
 */
final class CalciteIntegrationTestSupport {

    private CalciteIntegrationTestSupport() {}

    static Properties newCalciteConnectionProperties(String ledgerPath) {
        Properties props = new Properties();
        props.setProperty("model", inlineModel(ledgerPath));
        props.setProperty("lex", "JAVA");
        props.setProperty("quoting", "DOUBLE_QUOTE");
        props.setProperty("caseSensitive", "true");
        return props;
    }

    static String moduleLedgerPath(String resourcePath) {
        try {
            return TestResources.resolveResource(resourcePath).toAbsolutePath().toString();
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to resolve ledger resource: " + resourcePath, ex);
        }
    }

    private static String inlineModel(String ledgerPath) {
        return """
                inline:{
                  "version":"1.0",
                  "defaultSchema":"beancount",
                  "schemas":[
                    {
                      "type":"custom",
                      "name":"beancount",
                      "factory":"com.beancount.jdbc.calcite.BeancountSchemaFactory",
                      "operand":{"ledger":"%s"}
                    }
                  ]
                }
                """
                .formatted(escapeLedgerPath(ledgerPath));
    }

    private static String escapeLedgerPath(String ledgerPath) {
        return ledgerPath.replace("\\", "\\\\");
    }
}
