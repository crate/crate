package org.cratedb.sql.types;

public class ByteSQLType extends NumericSQLType {

    public static final String NAME = "byte";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected boolean checkRange(Number value) {
        return Byte.MIN_VALUE <= value.intValue() && value.intValue() <= Byte.MAX_VALUE;
    }

    @Override
    protected Object mapNumber(Number value) throws ConvertException {
        return value.intValue();
    }
}
