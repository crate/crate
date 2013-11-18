package org.cratedb.sql.types;

import org.joda.time.format.ISODateTimeFormat;

public class TimeStampSQLType extends SQLType {

    public static final String NAME = "timestamp";

    @Override
    public String mappingTypeName() {
        return NAME;
    }

    @Override
    protected Object doConvert(Object value) throws ConvertException {
        Long converted = null;
        if (value instanceof String) {
            // ISODate String
            try {
                converted = ISODateTimeFormat.dateOptionalTimeParser().parseMillis((String)value);
            } catch (IllegalArgumentException e) {
                throw new ConvertException(String.format("Invalid %s ISODate string",
                        mappingTypeName()));
            }
        }
        else if ((value instanceof Float)||(value instanceof Double)) {
            // interpret as seconds since epoque with millis as fractions
            double d = ((Number)value).doubleValue();
            converted = Math.round(d * 1000.0);
        }
        else if ((value instanceof Number)) {
            // interpret as milliseconds since epoque
            converted = ((Number) value).longValue();
        }
        return converted;
    }
}
