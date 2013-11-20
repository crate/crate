package org.cratedb.sql.types;

public class IntegerSQLType extends NumericSQLType {

    public static final String NAME = "integer";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected boolean checkRange(Number value) {
        return Integer.MIN_VALUE <= value.longValue() && value.longValue() <= Integer.MAX_VALUE;
    }

    @Override
    protected Object convertNumber(Number value) throws ConvertException {
        return value.intValue();
    }
}
