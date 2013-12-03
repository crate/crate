package org.cratedb.sql.types;

public class FloatSQLType extends SQLType {

    public static final String NAME = "float";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected Object doMapValue(Object value) throws ConvertException {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else {
            throw new ConvertException(typeName());
        }
    }
}
