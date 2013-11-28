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
            if (value instanceof String) {
                // boolean gets returned as "T" or "F" from lucene DocLookUp
                if ("T".equalsIgnoreCase((String)value)) { return true; }
                else if ("F".equalsIgnoreCase((String)value)) { return false; }
            }
            throw new ConvertException(typeName());
        }
    }
}
