package com.beancount.jdbc.calcite;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.Set;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;

final class BeancountSchemaFactoryTest {

    @Test
    void registersSchemaViaInlineModel() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Path repoRoot = Path.of("").toAbsolutePath();
        String ledger =
                repoRoot
                        .resolve("third_party/beancount/examples/example.beancount")
                        .toAbsolutePath()
                        .toString()
                        .replace("\\", "\\\\");
        String model =
                """
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
                        .formatted(ledger);

        Properties props = new Properties();
        props.setProperty("model", model);
        props.setProperty("lex", "JAVA");
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
            CalciteConnection calcite = connection.unwrap(CalciteConnection.class);
            SchemaPlus root = calcite.getRootSchema();
            Set<String> schemas = root.getSubSchemaNames();
            assertTrue(
                    schemas.contains("beancount"),
                    "Expected root schema to contain 'beancount' but found: " + schemas);
        }
    }
}
