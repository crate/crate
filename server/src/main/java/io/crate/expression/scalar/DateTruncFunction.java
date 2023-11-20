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

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTimeZone;

import io.crate.common.collections.MapBuilder;
import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.user.RoleLookup;

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
            DataTypes.TIMESTAMP, DataTypes.TIMESTAMPZ, DataTypes.LONG);
        for (DataType<?> dataType : supportedTimestampTypes) {
            module.register(
                Signature.scalar(
                    NAME,
                    DataTypes.STRING.getTypeSignature(),
                    dataType.getTypeSignature(),
                    DataTypes.TIMESTAMPZ.getTypeSignature()
                ).withFeatures(Scalar.DETERMINISTIC_AND_COMPARISON_REPLACEMENT),
                DateTruncFunction::new
            );

            // time zone aware variant
            module.register(
                Signature.scalar(
                    NAME,
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    dataType.getTypeSignature(),
                    DataTypes.TIMESTAMPZ.getTypeSignature()
                ).withFeatures(Scalar.DETERMINISTIC_AND_COMPARISON_REPLACEMENT),
                DateTruncFunction::new
            );
        }
    }

    @Nullable
    private final Rounding tzRounding;

    DateTruncFunction(Signature signature, BoundSignature boundSignature) {
        this(signature, boundSignature, null);
    }

    private DateTruncFunction(Signature signature,
                              BoundSignature boundSignature,
                              @Nullable Rounding tzRounding) {
        super(signature, boundSignature);
        this.tzRounding = tzRounding;
    }

    @Override
    public Scalar<Long, Object> compile(List<Symbol> arguments, String currentUser, RoleLookup userLookup) {
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

        return new DateTruncFunction(signature, boundSignature, rounding(interval, timeZone));
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
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
            evaluate(txnCtx, nodeCtx, new Input[]{interval, timezone, tsSymbol})
        );
    }

    @Override
    public final Long evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
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
            return truncate(
                rounding(interval, timeZone),
                DataTypes.TIMESTAMPZ.sanitizeValue(value));
        }
        return truncate(tzRounding, DataTypes.TIMESTAMPZ.sanitizeValue(value));
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

    private static DateTimeUnit intervalAsUnit(String interval) {
        if (interval == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "invalid interval NULL for scalar '%s'", NAME));
        }
        DateTimeUnit intervalAsUnit = DATE_FIELD_PARSERS.get(interval.toLowerCase(Locale.ENGLISH));
        if (intervalAsUnit == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "invalid interval '%s' for scalar '%s'", interval, NAME));
        }
        return intervalAsUnit;
    }
}
