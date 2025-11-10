package com.beancount.jdbc.calcite;

import com.beancount.jdbc.schema.ColumnDescriptor;
import com.beancount.jdbc.schema.PadTable;
import java.sql.Types;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

final class PadDetailCalciteTable extends AbstractTable implements ScannableTable {

    private final List<Object[]> rows;

    PadDetailCalciteTable(List<Object[]> padRows) {
        this.rows = padRows;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ColumnDescriptor column : PadTable.getDetailDefinition().getColumns()) {
            builder.add(column.getName(), toRelDataType(typeFactory, column));
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(rows);
    }

    private RelDataType toRelDataType(RelDataTypeFactory factory, ColumnDescriptor column) {
        SqlTypeName sqlType = mapSqlType(column.getJdbcType());
        RelDataType baseType = factory.createSqlType(sqlType);
        return column.isNullable() ? factory.createTypeWithNullability(baseType, true) : baseType;
    }

    private SqlTypeName mapSqlType(int jdbcType) {
        return switch (jdbcType) {
            case Types.INTEGER -> SqlTypeName.INTEGER;
            case Types.VARCHAR -> SqlTypeName.VARCHAR;
            default -> SqlTypeName.ANY;
        };
    }
}
