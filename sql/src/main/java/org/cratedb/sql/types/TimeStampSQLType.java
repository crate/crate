package org.cratedb.sql.types;

import org.elasticsearch.common.Nullable;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class TimeStampSQLType extends SQLType {

    public static final String NAME = "timestamp";
    private static final DateTimeFormatter formatter = ISODateTimeFormat.dateOptionalTimeParser()
            .withZoneUTC();
    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected Object doConvert(Object value) throws ConvertException {

        if (value instanceof String) {
            // ISODate String
            try {
                return formatter.parseMillis((String) value);
            } catch (IllegalArgumentException e) {
                throw new ConvertException(String.format("Invalid %s ISODate string",
                        typeName()));
            }
        }
        else if ((value instanceof Float)||(value instanceof Double)) {
            // interpret as seconds since the Java epoch of 1970-01-01T00:00:00Z with millis as fractions
            double d = ((Number)value).doubleValue();
            return Math.round(d * 1000.0);
        }
        else if ((value instanceof Number)) {
            // interpret as milliseconds since the Java epoch of 1970-01-01T00:00:00Z
            return ((Number) value).longValue();
        }
        throw new ConvertException(String.format("Invalid %s", typeName()));
    }

    @Override
    public Object toDisplayValue(@Nullable Object value) {
        Object result = value;
        try {
            result = toXContent(value);
        } catch (ConvertException e) {
            // :/
        }
        if ( Integer.MIN_VALUE < ((Long)result) && ((Long)result) < Integer.MAX_VALUE){
            result = ((Number)result).intValue();}
        return result;
    }
}
