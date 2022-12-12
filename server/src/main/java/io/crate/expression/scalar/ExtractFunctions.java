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
import static io.crate.types.TypeSignature.parseTypeSignature;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import org.elasticsearch.common.joda.Joda;
import org.joda.time.DateTimeField;
import org.joda.time.DurationFieldType;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;

import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.Extract;
import io.crate.types.DataTypes;

public class ExtractFunctions {

    public static final String NAME_PREFIX = "extract_";

    private record TsFieldWithDateTimeField(Extract.Field extractField, DateTimeField dtf) {}

    private record IntervalFieldWithFunction(Extract.Field extractField, Function<Period, Integer> function) {}

    public static void register(ScalarFunctionModule module) {

        List<TsFieldWithDateTimeField> fieldsMapWithIntReturn = List.of(
            new TsFieldWithDateTimeField(CENTURY, ISOChronology.getInstanceUTC().centuryOfEra()),
            new TsFieldWithDateTimeField(YEAR, ISOChronology.getInstanceUTC().year()),
            new TsFieldWithDateTimeField(QUARTER, Joda.QUARTER_OF_YEAR.getField(ISOChronology.getInstanceUTC())),
            new TsFieldWithDateTimeField(MONTH, ISOChronology.getInstanceUTC().monthOfYear()),
            new TsFieldWithDateTimeField(WEEK, ISOChronology.getInstanceUTC().weekOfWeekyear()),
            new TsFieldWithDateTimeField(DAY, ISOChronology.getInstanceUTC().dayOfMonth()),
            new TsFieldWithDateTimeField(DAY_OF_MONTH, ISOChronology.getInstanceUTC().dayOfMonth()),
            new TsFieldWithDateTimeField(DAY_OF_WEEK, ISOChronology.getInstanceUTC().dayOfWeek()),
            new TsFieldWithDateTimeField(DAY_OF_YEAR, ISOChronology.getInstanceUTC().dayOfYear()),
            new TsFieldWithDateTimeField(HOUR, ISOChronology.getInstanceUTC().hourOfDay()),
            new TsFieldWithDateTimeField(MINUTE, ISOChronology.getInstanceUTC().minuteOfHour()),
            new TsFieldWithDateTimeField(SECOND, ISOChronology.getInstanceUTC().secondOfMinute())
        );

        for (var argType : List.of(DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMP)) {
            for (var entry : fieldsMapWithIntReturn) {
                final DateTimeField dtf = entry.dtf();
                module.register(
                    Signature.scalar(
                        functionNameFrom(entry.extractField()),
                        argType.getTypeSignature(),
                        parseTypeSignature("integer")
                    ),
                    (signature, boundSignature) ->
                        new UnaryScalar<Number, Long>(signature, boundSignature, argType, dtf::get)
                );
            }
            // extract(epoch from ...) is different as is returns a `double precision`
            module.register(
                Signature.scalar(
                    functionNameFrom(EPOCH),
                    argType.getTypeSignature(),
                    parseTypeSignature("double precision")
                ),
                (signature, boundSignature) ->
                    new UnaryScalar<Number, Long>(signature, boundSignature, argType, v -> (double) v / 1000)
            );
        }

        // Intervals
        List<IntervalFieldWithFunction> intervalFieldsMapWithIntReturn = List.of(
            new IntervalFieldWithFunction(YEAR, p -> p.get(DurationFieldType.years())),
            new IntervalFieldWithFunction(QUARTER, p -> p.get(DurationFieldType.months()) / 4),
            new IntervalFieldWithFunction(MONTH, p -> p.get(DurationFieldType.months())),
            new IntervalFieldWithFunction(DAY, p -> p.get(DurationFieldType.days())),
            new IntervalFieldWithFunction(HOUR, p -> p.get(DurationFieldType.hours())),
            new IntervalFieldWithFunction(MINUTE, p -> p.get(DurationFieldType.minutes())),
            new IntervalFieldWithFunction(SECOND, p -> p.get(DurationFieldType.seconds()))
        );

        for (var entry : intervalFieldsMapWithIntReturn) {
            final Function<Period, Integer> function = entry.function();
            module.register(
                Signature.scalar(
                    functionNameFrom(entry.extractField()),
                    DataTypes.INTERVAL.getTypeSignature(),
                    parseTypeSignature("integer")
                ),
                (signature, boundSignature) ->
                    new UnaryScalar<Number, Period>(signature, boundSignature, DataTypes.INTERVAL, function::apply)
            );
        }
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
