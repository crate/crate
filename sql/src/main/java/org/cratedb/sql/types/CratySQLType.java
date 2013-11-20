package org.cratedb.sql.types;

import java.util.Map;

public class CratySQLType extends SQLType {

    public static final String NAME = "craty";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected Object doConvert(Object value) throws ConvertException {
        try {
            return (Map)value;
        } catch(ClassCastException e) {
            throw new ConvertException(String.format("Invalid %s", typeName()));
        }
    }
}
