package com.beancount.jdbc.calcite;

import com.beancount.jdbc.schema.ColumnDescriptor;
import com.beancount.jdbc.schema.PostingsTable;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

final class PostingsCalciteTable extends AbstractTable implements ScannableTable {

    private final List<Object[]> rows;

    PostingsCalciteTable(List<Object[]> postingRows) {
        this.rows = postingRows;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ColumnDescriptor column : PostingsTable.getDefinition().getColumns()) {
            builder.add(column.getName(), CalciteTypeMapper.toRelDataType(typeFactory, column));
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(rows);
    }
}
