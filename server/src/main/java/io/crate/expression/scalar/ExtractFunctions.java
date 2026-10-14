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

import static io.crate.sql.tree.Extract.Field.CENTURY;
import static io.crate.sql.tree.Extract.Field.DAY;
import static io.crate.sql.tree.Extract.Field.DAY_OF_MONTH;
import static io.crate.sql.tree.Extract.Field.DAY_OF_WEEK;
import static io.crate.sql.tree.Extract.Field.DAY_OF_YEAR;
import static io.crate.sql.tree.Extract.Field.EPOCH;
import static io.crate.sql.tree.Extract.Field.HOUR;
import static io.crate.sql.tree.Extract.Field.MINUTE;
import static io.crate.sql.tree.Extract.Field.MONTH;
import static io.crate.sql.tree.Extract.Field.QUARTER;
import static io.crate.sql.tree.Extract.Field.SECOND;
import static io.crate.sql.tree.Extract.Field.WEEK;
import static io.crate.sql.tree.Extract.Field.YEAR;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import org.elasticsearch.common.joda.Joda;
import org.joda.time.DateTimeField;
import org.joda.time.DurationFieldType;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;

import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.Signature.Feature;
import io.crate.sql.tree.Extract;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class ExtractFunctions {

    public static final String NAME_PREFIX = "extract_";

    public static void register(Functions.Builder module) {
        ISOChronology utcChronology = ISOChronology.getInstanceUTC();
        for (var argType : List.of(DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMP)) {
            regExtractFromTS(module, argType, CENTURY, utcChronology.centuryOfEra());
            regExtractFromTS(module, argType, YEAR, utcChronology.year());
            regExtractFromTS(module, argType, QUARTER, Joda.QUARTER_OF_YEAR.getField(utcChronology));
            regExtractFromTS(module, argType, MONTH, utcChronology.monthOfYear());
            regExtractFromTS(module, argType, WEEK, utcChronology.weekOfWeekyear());
            regExtractFromTS(module, argType, DAY, utcChronology.dayOfMonth());
            regExtractFromTS(module, argType, DAY_OF_MONTH, utcChronology.dayOfMonth());
            regExtractFromTS(module, argType, DAY_OF_WEEK, utcChronology.dayOfWeek());
            regExtractFromTS(module, argType, DAY_OF_YEAR, utcChronology.dayOfYear());
            regExtractFromTS(module, argType, HOUR, utcChronology.hourOfDay());
            regExtractFromTS(module, argType, MINUTE, utcChronology.minuteOfHour());
            regExtractFromTS(module, argType, SECOND, utcChronology.secondOfMinute());
            // extract(epoch from ...) is different as is returns a `double precision`
            module.add(
                Signature.builder(functionNameFrom(EPOCH), FunctionType.SCALAR)
                    .argumentTypes(argType.getTypeSignature())
                    .returnType(DataTypes.DOUBLE.getTypeSignature())
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                (signature, boundSignature) ->
                    new UnaryScalar<>(signature, boundSignature, (Long v) -> (double) v / 1000)
            );
        }

        // Intervals
        regExtractFromInterval(module, YEAR, p -> p.get(DurationFieldType.years()));
        regExtractFromInterval(module, QUARTER, p -> p.get(DurationFieldType.months()) / 4);
        regExtractFromInterval(module, MONTH, p -> p.get(DurationFieldType.months()));
        regExtractFromInterval(module, DAY, p -> p.get(DurationFieldType.days()));
        regExtractFromInterval(module, HOUR, p -> p.get(DurationFieldType.hours()));
        regExtractFromInterval(module, MINUTE, p -> p.get(DurationFieldType.minutes()));
        regExtractFromInterval(module, SECOND, p -> p.get(DurationFieldType.seconds()));
        // extract(epoch from ...) is different as is returns a `double precision`
        module.add(
            Signature.builder(functionNameFrom(EPOCH), FunctionType.SCALAR)
                .argumentTypes(DataTypes.INTERVAL.getTypeSignature())
                .returnType(DataTypes.DOUBLE.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) ->
                new UnaryScalar<>(signature, boundSignature, ExtractFunctions::toMillis)
        );
    }

    private static void regExtractFromTS(Functions.Builder builder,
                                         DataType<Long> tzType,
                                         Extract.Field field,
                                         DateTimeField dtf) {
        Function<Long, Integer> extract = x -> dtf.get(x);
        builder.add(
            Signature.builder(functionNameFrom(field), FunctionType.SCALAR)
                .argumentTypes(tzType.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) -> new UnaryScalar<>(signature, boundSignature, extract)
        );
    }


    private static void regExtractFromInterval(Functions.Builder builder,
                                               Extract.Field field,
                                               Function<Period, Integer> func) {
        builder.add(
            Signature.builder(functionNameFrom(field), FunctionType.SCALAR)
                .argumentTypes(DataTypes.INTERVAL.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) -> new UnaryScalar<>(signature, boundSignature, func)
        );
    }

    private static final long YEAR_IN_SECONDS = 365 * 24 * 60 * 60L;
    private static final long MONTH_IN_SECONDS = 30 * 24 * 60 * 60L;
    private static final long WEEK_IN_SECONDS = 7 * 24 * 60 * 60L;
    private static final long DAY_IN_SECONDS = 24 * 60 * 60L;
    private static final long HOUR_IN_SECONDS = 60 * 60L;
    private static final long MINUTE_IN_SECONDS = 60L;

    private static Double toMillis(Period period) {
        double result = 0.0d;
        result += period.getYears() * YEAR_IN_SECONDS;
        result += period.getYears() * 6 * HOUR_IN_SECONDS; // + 6 hours per year
        result += period.getMonths() * MONTH_IN_SECONDS;
        result += period.getWeeks() * WEEK_IN_SECONDS;
        result += period.getDays() * DAY_IN_SECONDS;
        result += period.getHours() * HOUR_IN_SECONDS;
        result += period.getMinutes() * MINUTE_IN_SECONDS;
        result += period.getSeconds();
        result += period.getMillis() / 1000.0d;
        return result;
    }

    public static String functionNameFrom(Extract.Field field) {
        switch (field) {
            case CENTURY:
            case YEAR:
            case QUARTER:
            case MONTH:
            case WEEK:
            case HOUR:
            case MINUTE:
            case SECOND:
            case EPOCH:
                return NAME_PREFIX + field;
            case DAY:
                return NAME_PREFIX + DAY;
            case DAY_OF_MONTH:
                return NAME_PREFIX + DAY_OF_MONTH;
            case DAY_OF_WEEK:
            case DOW:
                return NAME_PREFIX + DAY_OF_WEEK;
            case DAY_OF_YEAR:
            case DOY:
                return NAME_PREFIX + DAY_OF_YEAR;
            case TIMEZONE_HOUR:
            case TIMEZONE_MINUTE:
            default:
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH, "Extract( %s from <expression>) is not supported", field));
        }
    }
}
