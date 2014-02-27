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
package io.crate.operator.scalar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operator.Input;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.util.TimeZone;

public class DateTruncFunction implements Scalar<Long, Object> {

    public static final String NAME = "date_trunc";
    protected static final ImmutableMap<BytesRef, DateTimeFieldParser> DATE_FIELD_PARSERS =
            MapBuilder.<BytesRef, DateTimeFieldParser>newMapBuilder()
            // we only store timestamps in milliseconds since epoch.
            // therefore, we supporting 'milliseconds' and 'microseconds' wouldn't affect anything.
            .put(new BytesRef("second"), new DateTimeFieldParser.SecondOfMinute())
            .put(new BytesRef("minute"), new DateTimeFieldParser.MinuteOfHour())
            .put(new BytesRef("hour"), new DateTimeFieldParser.HourOfDay())
            .put(new BytesRef("day"), new DateTimeFieldParser.DayOfMonth())
            .put(new BytesRef("week"), new DateTimeFieldParser.WeekOfWeekyear())
            .put(new BytesRef("month"), new DateTimeFieldParser.MonthOfYear())
            .put(new BytesRef("quarter"), new DateTimeFieldParser.Quarter())
            .put(new BytesRef("year"), new DateTimeFieldParser.YearOfCentury())
            .put(new BytesRef("century"), new DateTimeFieldParser.CenturyOfEra())
            .immutableMap();
    private static final DateTimeZone DEFAULT_TZ =  DateTimeZone.forTimeZone(TimeZone.getDefault());
    private final FunctionInfo info;

    public DateTruncFunction(FunctionInfo info) {
        this.info = info;
    }

    public static void register(ScalarFunctionModule module) {
        FunctionIdent timestampFunctionIdent = new FunctionIdent(NAME,
                ImmutableList.of(DataType.STRING, DataType.TIMESTAMP));
        module.registerScalarFunction(new DateTruncFunction(
                new FunctionInfo(timestampFunctionIdent, DataType.TIMESTAMP)));
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert (symbol.arguments().size() == 2);

        StringLiteral interval = (StringLiteral) symbol.arguments().get(0);
        if (!DATE_FIELD_PARSERS.containsKey(interval.value())) {
            throw new IllegalArgumentException(
                    SymbolFormatter.format("unknown interval %s for '%s'", interval, symbol));
        }

        if (symbol.arguments().get(1).symbolType().isLiteral()) {
            Literal timestamp = (Literal)symbol.arguments().get(1);
            return new TimestampLiteral(evaluate((Input)interval, timestamp));
        }

        return symbol;
    }

    @Override
    public Long evaluate(Input<Object>... args) {
        assert (args.length == 2);
        assert (args[0].value() != null);
        if (args[1].value() == null) {
            return null;
        }
        DateTimeFieldParser fieldParser = DATE_FIELD_PARSERS.get(args[0].value());
        assert fieldParser != null;

        return truncate(fieldParser, (Long) args[1].value());
    }

    private Long truncate(DateTimeFieldParser fieldParser, Long ts) {
        TimeZoneRounding.Builder tzRoundingBuilder;
        if (fieldParser != null) {
            Chronology chronology = ISOChronology.getInstanceUTC();
            tzRoundingBuilder = TimeZoneRounding.builder(fieldParser.parse(chronology));
        } else {
            return null;
        }
        TimeZoneRounding tzRounding = tzRoundingBuilder
                .preZone(DEFAULT_TZ)
                .preZoneAdjustLargeInterval(true)
                .build();
        return tzRounding.calc(ts);
    }

    interface DateTimeFieldParser {

        DateTimeField parse(Chronology chronology);

        static class CenturyOfEra implements DateTimeFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.centuryOfEra();
            }
        }

        static class YearOfCentury implements DateTimeFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.yearOfCentury();
            }
        }

        static class WeekOfWeekyear implements DateTimeFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.weekOfWeekyear();
            }
        }

        static class Quarter implements DateTimeFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return Joda.QuarterOfYear.getField(chronology);
            }
        }

        static class MonthOfYear implements DateTimeFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.monthOfYear();
            }
        }

        static class DayOfMonth implements DateTimeFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.dayOfMonth();
            }
        }

        static class HourOfDay implements DateTimeFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.hourOfDay();
            }
        }

        static class MinuteOfHour implements DateTimeFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.minuteOfHour();
            }
        }

        static class SecondOfMinute implements DateTimeFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.secondOfMinute();
            }
        }

    }
}
