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

package org.cratedb;

import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class TimestampFormat {
    private static final FormatDateTimeFormatter dateTimeFormatter = Joda.forPattern("dateOptionalTime", Locale.ROOT);
    private static final TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    private static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

    public static long parseTimestampString(String value) throws UnsupportedOperationException {
        try {
            return dateTimeFormatter.parser().parseMillis(value);
        } catch (RuntimeException e) {
            throw new UnsupportedOperationException("failed to parse timestamp field [" + value + "], tried both date format [" + dateTimeFormatter.format() + "], and timestamp number with locale [" + dateTimeFormatter.locale() + "]", e);
        }
    }

    public static boolean isDateFormat(String value) {
        if (!NUMBER_PATTERN.matcher(value).matches()) {
            try {
                dateTimeFormatter.parser().parseMillis(value);
                return true;
            } catch (RuntimeException e) {
                //
            }
        }
        return false;
    }

    public static String printTimeStamp(long value) {
        return dateTimeFormatter.printer().print(value);
    }
}
