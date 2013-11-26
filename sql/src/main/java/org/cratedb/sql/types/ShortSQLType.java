package org.cratedb.sql.types;


public class ShortSQLType extends NumericSQLType {

    public static final String NAME = "short";
    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected boolean checkRange(Number value) {
        return Short.MIN_VALUE <= value.longValue() && value.longValue() <= Short.MAX_VALUE;
    }

    @Override
    protected Object mapNumber(Number value) throws ConvertException {
        return value.shortValue();
    }

}
