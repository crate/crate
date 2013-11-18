package org.cratedb.sql.types;

public class BooleanSQLType extends SQLType {

    public static final String NAME = "boolean";

    @Override
    public String mappingTypeName() {
        return NAME;
    }

    @Override
    protected Object doConvert(Object value) throws ConvertException {
        if (!(value instanceof Boolean)) {
            throw new ConvertException(String.format("Invalid %s", mappingTypeName()));
        }
        return value;
    }
}
