package com.beancount.jdbc.calcite;

import java.util.Map;
import java.util.Objects;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Calcite {@link SchemaFactory} entry point that wires {@link BeancountSchema} into a Calcite
 * connection. The schema is currently empty; tables will be registered in later stages.
 */
public final class BeancountSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        Objects.requireNonNull(parentSchema, "parentSchema");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(operand, "operand");
        return new BeancountSchema(parentSchema, name, operand);
    }
}

