package org.cratedb.sql.types;

import java.util.Map;

public class ObjectSQLType extends SQLType {

    public static final String NAME = "object";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected Object doMapValue(Object value) throws ConvertException {
        try {
            return (Map)value;
        } catch(ClassCastException e) {
            throw new ConvertException(typeName());
        }
    }
}
