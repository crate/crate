/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
package io.crate.operation.scalar;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.StringLiteral;
import io.crate.planner.symbol.SymbolFormatter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.TimeZoneRounding;
import org.joda.time.DateTimeZone;

public abstract class BaseDateTruncFunction implements Scalar<Long, Object> {

    public static final String NAME = "date_trunc";

    protected static final ImmutableMap<BytesRef, DateTimeUnit> DATE_FIELD_PARSERS = MapBuilder.<BytesRef, DateTimeUnit>newMapBuilder()
            // we only store timestamps in milliseconds since epoch.
            // therefore, we supporting 'milliseconds' and 'microseconds' wouldn't affect anything.
            .put(new BytesRef("year"), DateTimeUnit.YEAR_OF_CENTURY)
            .put(new BytesRef("quarter"), DateTimeUnit.QUARTER)
            .put(new BytesRef("month"), DateTimeUnit.MONTH_OF_YEAR)
            .put(new BytesRef("week"), DateTimeUnit.WEEK_OF_WEEKYEAR)
            .put(new BytesRef("day"), DateTimeUnit.DAY_OF_MONTH)
            .put(new BytesRef("hour"), DateTimeUnit.HOUR_OF_DAY)
            .put(new BytesRef("minute"), DateTimeUnit.MINUTES_OF_HOUR)
            .put(new BytesRef("second"), DateTimeUnit.SECOND_OF_MINUTE)
            .immutableMap();

    private final FunctionInfo info;

    public BaseDateTruncFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    /**
     * Check if <code>interval</code> is valid
     * @param interval
     * @param symbol required for generating an error message if <code>interval</code> is invalid
     * @throws java.lang.IllegalArgumentException in case of invalid <code>interval</code>
     * @return
     */
    protected boolean isValidInterval(StringLiteral interval, Function symbol) {
        if (!DATE_FIELD_PARSERS.containsKey(interval.value())) {
            throw new IllegalArgumentException(
                    SymbolFormatter.format("unknown interval %s for '%s'", interval, symbol));
        }
        return true;
    }

    /**
     * Truncates given <code>timestamp</code> down to the given <code>interval</code>.
     * The <code>timestamp</code> is expected to be in milliseconds.
     * @param interval
     * @param ts
     * @param tz
     * @return
     */
    protected Long truncate(DateTimeUnit interval, Long ts, DateTimeZone tz) {
        TimeZoneRounding.Builder tzRoundingBuilder;
        if (interval != null) {
            tzRoundingBuilder = TimeZoneRounding.builder(interval);
        } else {
            return null;
        }
        TimeZoneRounding tzRounding = tzRoundingBuilder
                .preZone(tz)
                .preZoneAdjustLargeInterval(true)
                .build();
        return tzRounding.round(ts);
    }

}
