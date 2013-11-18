package org.cratedb.sql.types;

public class StringSQLType extends SQLType {

    public static final String NAME = "string";

    @Override
    public String mappingTypeName() {
        return NAME;
    }

    @Override
    public Object doConvert(Object value) throws ConvertException {
        return value.toString();
    }
}
