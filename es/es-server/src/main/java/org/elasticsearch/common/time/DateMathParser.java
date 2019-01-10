/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.time;

import org.joda.time.DateTimeZone;

import java.time.ZoneId;
import java.util.function.LongSupplier;

/**
 * An abstraction over date math parsing to allow different implementation for joda and java time.
 */
public interface DateMathParser {

    /**
     * Parse a date math expression without timzeone info and rounding down.
     */
    default long parse(String text, LongSupplier now) {
        return parse(text, now, false, (ZoneId) null);
    }

    // Note: we take a callable here for the timestamp in order to be able to figure out
    // if it has been used. For instance, the request cache does not cache requests that make
    // use of `now`.

    // exists for backcompat, do not use!
    @Deprecated
    default long parse(String text, LongSupplier now, boolean roundUp, DateTimeZone tz) {
        return parse(text, now, roundUp, tz == null ? null : ZoneId.of(tz.getID()));
    }

    /**
     * Parse text, that potentially contains date math into the milliseconds since the epoch
     *
     * Examples are
     *
     * <code>2014-11-18||-2y</code> subtracts two years from the input date
     * <code>now/m</code>           rounds the current time to minute granularity
     *
     * Supported rounding units are
     * y    year
     * M    month
     * w    week (beginning on a monday)
     * d    day
     * h/H  hour
     * m    minute
     * s    second
     *
     *
     * @param text      the input
     * @param now       a supplier to retrieve the current date in milliseconds, if needed for additions
     * @param roundUp   should the result be rounded up
     * @param tz        an optional timezone that should be applied before returning the milliseconds since the epoch
     * @return          the parsed date in milliseconds since the epoch
     */
    long parse(String text, LongSupplier now, boolean roundUp, ZoneId tz);
}
