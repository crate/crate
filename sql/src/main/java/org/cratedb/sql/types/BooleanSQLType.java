package org.cratedb.sql.types;

public class BooleanSQLType extends SQLType {

    public static final String NAME = "boolean";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected Object doConvert(Object value) throws ConvertException {
        try {
            return (Boolean)value;
        } catch (ClassCastException e) {
            throw new ConvertException(String.format("Invalid %s", typeName()));
        }
    }
}
