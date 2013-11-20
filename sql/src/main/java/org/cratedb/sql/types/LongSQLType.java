package org.cratedb.sql.types;

public class LongSQLType extends NumericSQLType {
    public static final String NAME = "long";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected boolean checkRange(Number value) {
        return true; // no range check
    }

    @Override
    protected Object convertNumber(Number value) throws ConvertException {
        return value.longValue();
    }
}
