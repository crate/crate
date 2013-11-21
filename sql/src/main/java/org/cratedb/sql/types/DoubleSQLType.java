package org.cratedb.sql.types;

public class DoubleSQLType extends SQLType {
    public static final String NAME = "double";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected Object doConvert(Object value) throws ConvertException {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else {
            throw new ConvertException(typeName());
        }
    }
}
