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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TimestampType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.rounding.TimeZoneRounding;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Locale;

public class DateTruncFunction extends Scalar<Long, Object> {

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

    public static void register(ScalarFunctionModule module) {
        List<DataType> supportedTimestampTypes = ImmutableList.<DataType>of(
                DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.STRING);
        for (DataType dataType : supportedTimestampTypes) {
            module.register(new DateTruncFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.STRING, dataType)),
                    DataTypes.TIMESTAMP)
            ));
            // time zone aware variant
            module.register(new DateTruncFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.STRING, dataType)),
                    DataTypes.TIMESTAMP)
            ));
        }
    }


    private FunctionInfo info;
    private Rounding tzRounding;

    public DateTruncFunction(FunctionInfo info) {
        this.info = info;
    }

    public DateTruncFunction(FunctionInfo info, Rounding tzRounding) {
        this(info);
        this.tzRounding = tzRounding;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Scalar<Long, Object> compile(List<Symbol> arguments) {
        assert arguments.size() > 1 && arguments.size() < 4 : "Invalid number of arguments";

        if (!arguments.get(0).symbolType().isValueSymbol()) {
            // arguments are no values, we can't compile
            return this;
        }

        // all validation is already done by {@link #normalizeSymbol()}
        BytesRef interval = (BytesRef)((Input)arguments.get(0)).value();
        BytesRef timeZone = TimeZoneParser.DEFAULT_TZ_BYTES_REF;
        if (arguments.size() == 3) {
            timeZone = (BytesRef)((Input)arguments.get(1)).value();
        }

        return new DateTruncFunction(this.info, rounding(interval, timeZone));
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert symbol.arguments().size() > 1 && symbol.arguments().size() < 4 : "Invalid number of arguments";

        if (containsNullLiteral(symbol.arguments())) {
            return Literal.NULL;
        }

        if (anyNonLiterals(symbol.arguments())) {
            return symbol;
        }

        Literal interval = (Literal) symbol.arguments().get(0);
        Literal tsSymbol;
        Literal timezone;

        if (symbol.arguments().size() == 2) {
            timezone = TimeZoneParser.DEFAULT_TZ_LITERAL;
            tsSymbol = (Literal)symbol.arguments().get(1);
        } else {
            timezone = (Literal) symbol.arguments().get(1);
            tsSymbol = (Literal)symbol.arguments().get(2);
        }

        return Literal.newLiteral(
                DataTypes.TIMESTAMP,
                evaluate(new Input[]{interval, timezone, tsSymbol})
        );
    }

    @Override
    public final Long evaluate(Input[] args) {
        assert args.length > 1 && args.length < 4 : "Invalid number of arguments";
        Object value;
        BytesRef timeZone = TimeZoneParser.DEFAULT_TZ_BYTES_REF;
        if (args.length == 2) {
            value = args[1].value();
        } else {
            timeZone = (BytesRef)args[1].value();
            value = args[2].value();
        }
        if (value == null) {
            return null;
        }
        if (tzRounding == null) {
            BytesRef interval = (BytesRef)args[0].value();
            return truncate(rounding(interval, timeZone), TimestampType.INSTANCE.value(value));
        }
        return truncate(tzRounding, TimestampType.INSTANCE.value(value));
    }

    protected Rounding rounding(BytesRef interval, BytesRef timeZoneString) {
        DateTimeUnit intervalAsUnit = intervalAsUnit(interval);
        DateTimeZone timeZone = TimeZoneParser.parseTimeZone(timeZoneString);

        TimeZoneRounding.Builder tzRoundingBuilder = TimeZoneRounding.builder(intervalAsUnit);
        return tzRoundingBuilder
                .preZone(timeZone)
                .preZoneAdjustLargeInterval(true)
                .build();
    }

    /**
     * Truncates given <code>timestamp</code> down to the given <code>interval</code>.
     * The <code>timestamp</code> is expected to be in milliseconds.
     */
    protected Long truncate(Rounding rounding, Long ts) {
        if (ts == null) {
            return null;
        }
        return rounding.round(ts);
    }

    protected DateTimeUnit intervalAsUnit(BytesRef interval) {
        if (interval == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "invalid interval NULL for scalar '%s'", NAME));
        }
        DateTimeUnit intervalAsUnit = DATE_FIELD_PARSERS.get(interval);
        if (intervalAsUnit == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "invalid interval '%s' for scalar '%s'", interval.utf8ToString(), NAME));
        }
        return intervalAsUnit;
    }


}
