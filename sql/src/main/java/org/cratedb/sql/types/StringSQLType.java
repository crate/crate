package org.cratedb.sql.types;

public class StringSQLType extends SQLType {

    public static final String NAME = "string";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    public Object doConvert(Object value) throws ConvertException {
        if ((value instanceof CharSequence)||(value instanceof Character)) {
            return value.toString();
        }
        throw new ConvertException(String.format("Invalid %s", typeName()));
    }
}
