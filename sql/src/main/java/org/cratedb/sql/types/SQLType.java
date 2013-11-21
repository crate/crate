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
        public ConvertException(String type) {
            super(String.format("Invalid %s", type));
        }

        public ConvertException(String type, String message) {
            super(String.format("Invalid %s: %s", type, message));
        }
    }

    public abstract String typeName();

    public Object toXContent(@Nullable Object value) throws ConvertException {
        if (value == null) { return null; }
        return doConvert(value);
    }

    protected abstract Object doConvert(Object value) throws ConvertException;

    public Object toDisplayValue(@Nullable Object value) {
        return value;
    }
}
