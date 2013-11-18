package org.cratedb.sql.types;

import org.elasticsearch.common.Nullable;

/**
 * SQL Type Descriptor used to validate and convert input from SQL commands
 */
public abstract class SQLType {

    /**
     * Exception thrown on conversion to XContent
     */
    public static class ConvertException extends Exception {
        public ConvertException(String msg) {
            super(msg);
        }
    }

    public abstract String mappingTypeName();
    public Object toXContent(@Nullable Object value, boolean allowNull) throws ConvertException {
        if (value == null) {
            if (allowNull) { return null; }
            else { throw new ConvertException("NULL value not allowed"); }
        }
        return doConvert(value);
    }

    protected abstract Object doConvert(Object value) throws ConvertException;
}
