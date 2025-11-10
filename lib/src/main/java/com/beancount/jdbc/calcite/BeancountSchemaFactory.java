package com.beancount.jdbc.calcite;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Calcite {@link SchemaFactory} that wires the Beancount schema into a Calcite connection.
 *
 * <p>The factory currently captures the ledger path from the operand map and creates an empty schema.
 * Tables will be registered incrementally as the Calcite integration evolves.</p>
 */
public final class BeancountSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(operand, "operand");
        Object ledgerOperand = operand.get("ledger");
        if (ledgerOperand == null) {
            throw new IllegalArgumentException("Missing 'ledger' operand for Beancount schema.");
        }
        Path ledgerPath = Paths.get(ledgerOperand.toString()).toAbsolutePath().normalize();
        return new BeancountSchema(parentSchema, name, ledgerPath);
    }
}
