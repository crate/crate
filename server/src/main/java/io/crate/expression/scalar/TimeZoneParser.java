/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar;

import io.crate.expression.symbol.Literal;
import org.joda.time.DateTimeZone;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * parses timezone strings to {@linkplain DateTimeZone} instances
 */
public class TimeZoneParser {

    public static final DateTimeZone DEFAULT_TZ = DateTimeZone.UTC;
    public static final Literal<String> DEFAULT_TZ_LITERAL = Literal.of("UTC");

    private static final ConcurrentMap<String, DateTimeZone> TIME_ZONE_MAP = new ConcurrentHashMap<>();

    private TimeZoneParser() {
    }

    public static DateTimeZone parseTimeZone(String timezone) throws IllegalArgumentException {
        if (timezone == null) {
            throw new IllegalArgumentException("invalid time zone value NULL");
        }
        if (timezone.equals(DEFAULT_TZ_LITERAL.value())) {
            return DEFAULT_TZ;
        }

        DateTimeZone tz = TIME_ZONE_MAP.get(timezone);
        if (tz == null) {
            try {
                int index = timezone.indexOf(':');
                if (index != -1) {
                    int beginIndex = timezone.charAt(0) == '+' ? 1 : 0;
                    // format like -02:30
                    tz = DateTimeZone.forOffsetHoursMinutes(
                        Integer.parseInt(timezone.substring(beginIndex, index)),
                        Integer.parseInt(timezone.substring(index + 1))
                    );
                } else {
                    // id, listed here: http://joda-time.sourceforge.net/timezones.html
                    // or here: http://www.joda.org/joda-time/timezones.html
                    tz = DateTimeZone.forID(timezone);
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "invalid time zone value '%s'", timezone));
            }
            TIME_ZONE_MAP.putIfAbsent(timezone, tz);
        }
        return tz;
    }
}
