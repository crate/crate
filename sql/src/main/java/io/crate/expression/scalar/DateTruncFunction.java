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

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TimestampType;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.crate.types.TypeSignature.parseTypeSignature;

public class DateTruncFunction extends Scalar<Long, Object> {

    public static final String NAME = "date_trunc";

    private static final Map<String, DateTimeUnit> DATE_FIELD_PARSERS = MapBuilder.<String, DateTimeUnit>newMapBuilder()
        // we only store timestamps in milliseconds since epoch.
        // therefore, we supporting 'milliseconds' and 'microseconds' wouldn't affect anything.
        .put("year", DateTimeUnit.YEAR_OF_CENTURY)
        .put("quarter", DateTimeUnit.QUARTER)
        .put("month", DateTimeUnit.MONTH_OF_YEAR)
        .put("week", DateTimeUnit.WEEK_OF_WEEKYEAR)
        .put("day", DateTimeUnit.DAY_OF_MONTH)
        .put("hour", DateTimeUnit.HOUR_OF_DAY)
        .put("minute", DateTimeUnit.MINUTES_OF_HOUR)
        .put("second", DateTimeUnit.SECOND_OF_MINUTE)
        .immutableMap();

    public static void register(ScalarFunctionModule module) {
        List<DataType<?>> supportedTimestampTypes = List.of(
            DataTypes.TIMESTAMP, DataTypes.TIMESTAMPZ, DataTypes.LONG, DataTypes.STRING);
        for (DataType<?> dataType : supportedTimestampTypes) {
            module.register(
                Signature.scalar(
                    NAME,
                    parseTypeSignature("text"),
                    dataType.getTypeSignature(),
                    parseTypeSignature("text")
                ),
                (signature, argumentTypes) ->
                    new DateTruncFunction(info(argumentTypes), signature)
            );

            // time zone aware variant
            module.register(
                Signature.scalar(
                    NAME,
                    parseTypeSignature("text"),
                    parseTypeSignature("text"),
                    dataType.getTypeSignature(),
                    parseTypeSignature("text")
                ),
                (signature, argumentTypes) ->
                    new DateTruncFunction(info(argumentTypes), signature)
            );
        }
    }

    private static FunctionInfo info(List<DataType> types) {
        return new FunctionInfo(
            new FunctionIdent(NAME, types),
            DataTypes.TIMESTAMPZ, FunctionInfo.Type.SCALAR, FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT);
    }


    private final FunctionInfo info;
    private final Signature signature;
    @Nullable
    private final Rounding tzRounding;

    DateTruncFunction(FunctionInfo info, Signature signature) {
        this(info, signature, null);
    }

    private DateTruncFunction(FunctionInfo info,
                              Signature signature,
                              @Nullable Rounding tzRounding) {
        this.info = info;
        this.signature = signature;
        this.tzRounding = tzRounding;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Scalar<Long, Object> compile(List<Symbol> arguments) {
        assert arguments.size() > 1 && arguments.size() < 4 : "Invalid number of arguments";

        if (!arguments.get(0).symbolType().isValueSymbol()) {
            // arguments are no values, we can't compile
            return this;
        }

        // all validation is already done by {@link #normalizeSymbol()}
        String interval = (String) ((Input<?>) arguments.get(0)).value();
        if (interval == null) {
            return this;
        }
        String timeZone = TimeZoneParser.DEFAULT_TZ_LITERAL.value();
        if (arguments.size() == 3) {
            timeZone = (String) ((Input<?>) arguments.get(1)).value();
        }

        return new DateTruncFunction(this.info, signature, rounding(interval, timeZone));
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx) {
        assert symbol.arguments().size() > 1 && symbol.arguments().size() < 4 : "Invalid number of arguments";

        if (anyNonLiterals(symbol.arguments())) {
            return symbol;
        }

        Literal<?> interval = (Literal<?>) symbol.arguments().get(0);
        Literal<?> tsSymbol;
        Literal<?> timezone;

        if (symbol.arguments().size() == 2) {
            timezone = TimeZoneParser.DEFAULT_TZ_LITERAL;
            tsSymbol = (Literal<?>) symbol.arguments().get(1);
        } else {
            timezone = (Literal<?>) symbol.arguments().get(1);
            tsSymbol = (Literal<?>) symbol.arguments().get(2);
        }

        return Literal.of(
            DataTypes.TIMESTAMPZ,
            evaluate(txnCtx, new Input[]{interval, timezone, tsSymbol})
        );
    }

    @Override
    public final Long evaluate(TransactionContext txnCtx, Input[] args) {
        assert args.length > 1 && args.length < 4 : "Invalid number of arguments";
        Object value;
        String timeZone = TimeZoneParser.DEFAULT_TZ_LITERAL.value();
        if (args.length == 2) {
            value = args[1].value();
        } else {
            timeZone = (String) args[1].value();
            value = args[2].value();
        }
        if (value == null) {
            return null;
        }
        if (tzRounding == null) {
            String interval = (String) args[0].value();
            if (interval == null) {
                return null;
            }
            return truncate(rounding(interval, timeZone),
                TimestampType.INSTANCE_WITH_TZ.value(value));
        }
        return truncate(tzRounding, TimestampType.INSTANCE_WITH_TZ.value(value));
    }

    private Rounding rounding(String interval, String timeZoneString) {
        DateTimeUnit intervalAsUnit = intervalAsUnit(interval);
        DateTimeZone timeZone = TimeZoneParser.parseTimeZone(timeZoneString);

        return Rounding.builder(intervalAsUnit).timeZone(timeZone).build();
    }

    /**
     * Truncates given <code>timestamp</code> down to the given <code>interval</code>.
     * The <code>timestamp</code> is expected to be in milliseconds.
     */
    private Long truncate(Rounding rounding, Long ts) {
        if (ts == null) {
            return null;
        }
        return rounding.round(ts);
    }

    private DateTimeUnit intervalAsUnit(String interval) {
        if (interval == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "invalid interval NULL for scalar '%s'", NAME));
        }
        DateTimeUnit intervalAsUnit = DATE_FIELD_PARSERS.get(interval);
        if (intervalAsUnit == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "invalid interval '%s' for scalar '%s'", interval, NAME));
        }
        return intervalAsUnit;
    }
}
