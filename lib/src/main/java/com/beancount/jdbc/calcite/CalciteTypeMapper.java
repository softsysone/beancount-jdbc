package com.beancount.jdbc.calcite;

import com.beancount.jdbc.schema.ColumnDescriptor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

final class CalciteTypeMapper {

    private CalciteTypeMapper() {}

    static RelDataType toRelDataType(RelDataTypeFactory factory, ColumnDescriptor column) {
        SqlTypeName sqlType = mapSqlType(column.getJdbcType());
        RelDataType baseType;
        if (sqlType == SqlTypeName.DECIMAL && column.getSize() > 0) {
            int precision = column.getSize();
            int scale = Math.max(0, column.getScale());
            baseType = factory.createSqlType(sqlType, precision, scale);
        } else if (column.getSize() > 0 && sqlType.allowsPrec()) {
            baseType = factory.createSqlType(sqlType, column.getSize());
        } else {
            baseType = factory.createSqlType(sqlType);
        }
        return column.isNullable() ? factory.createTypeWithNullability(baseType, true) : baseType;
    }

    private static SqlTypeName mapSqlType(int jdbcType) {
        return switch (jdbcType) {
            case java.sql.Types.INTEGER -> SqlTypeName.INTEGER;
            case java.sql.Types.BIGINT -> SqlTypeName.BIGINT;
            case java.sql.Types.DECIMAL, java.sql.Types.NUMERIC -> SqlTypeName.DECIMAL;
            case java.sql.Types.DATE -> SqlTypeName.DATE;
            case java.sql.Types.CHAR -> SqlTypeName.CHAR;
            case java.sql.Types.VARCHAR -> SqlTypeName.VARCHAR;
            case java.sql.Types.DOUBLE -> SqlTypeName.DOUBLE;
            default -> SqlTypeName.ANY;
        };
    }
}
