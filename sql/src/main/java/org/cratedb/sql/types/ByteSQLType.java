package org.cratedb.sql.types;

public class ByteSQLType extends ScalarSQLType {

    public static final String NAME = "byte";

    @Override
    public String mappingTypeName() {
        return NAME;
    }

    @Override
    protected boolean checkRange(Number value) {
        return Byte.MIN_VALUE <= value.intValue() && value.intValue() <= Byte.MAX_VALUE;
    }

    @Override
    protected Object convertNumber(Number value) throws ConvertException {
        return value.intValue();
    }
}
