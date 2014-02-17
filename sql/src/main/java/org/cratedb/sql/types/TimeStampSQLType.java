/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

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
    protected Object doMapValue(Object value) throws ConvertException {

        if (value instanceof String) {
            // ISODate String
            try {
                return formatter.parseMillis((String) value);
            } catch (IllegalArgumentException e) {
                throw new ConvertException(typeName(), "Invalid ISO-date string");
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
        throw new ConvertException(typeName());
    }

    @Override
    public Object toDisplayValue(@Nullable Object value) {
        Object result = value;
        try {
            result = mappedValue(value);
        } catch (ConvertException e) {
            // :/
        }
        if ( Integer.MIN_VALUE < ((Long)result) && ((Long)result) < Integer.MAX_VALUE){
            result = ((Number)result).intValue();}
        return result;
    }
}
