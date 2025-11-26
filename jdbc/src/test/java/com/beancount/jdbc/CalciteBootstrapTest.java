package com.beancount.jdbc;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.Set;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;

final class CalciteBootstrapTest {

    @Test
    void calciteConnectionUnwrapsAndShowsMetadataSchema() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties props = new Properties();
        props.setProperty("lex", "JAVA");
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
            CalciteConnection calcite = connection.unwrap(CalciteConnection.class);
            SchemaPlus root = calcite.getRootSchema();
            Set<String> subSchemas = root.getSubSchemaNames();
            assertTrue(
                    subSchemas.contains("metadata"),
                    "Expected Calcite root schema to expose 'metadata' but found: " + subSchemas);
        }
    }
}

