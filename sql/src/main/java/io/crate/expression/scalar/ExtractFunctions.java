/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.Extract;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.joda.Joda;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static io.crate.sql.tree.Extract.Field.CENTURY;
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
import static java.util.Map.entry;

public class ExtractFunctions {

    public static final String NAME_PREFIX = "extract_";

    // Keep a map from parser Field to DateTimeField here to avoid joda dependencies at the parser
    private static final Map<Extract.Field, DateTimeField> FIELDS_MAP_WITH_INT_RETURN = Map.ofEntries(
        entry(CENTURY, ISOChronology.getInstanceUTC().centuryOfEra()),
        entry(YEAR, ISOChronology.getInstanceUTC().year()),
        entry(QUARTER, Joda.QuarterOfYear.getField(ISOChronology.getInstanceUTC())),
        entry(MONTH, ISOChronology.getInstanceUTC().monthOfYear()),
        entry(WEEK, ISOChronology.getInstanceUTC().weekOfWeekyear()),
        entry(DAY_OF_MONTH, ISOChronology.getInstanceUTC().dayOfMonth()),
        entry(DAY_OF_WEEK, ISOChronology.getInstanceUTC().dayOfWeek()),
        entry(DAY_OF_YEAR, ISOChronology.getInstanceUTC().dayOfYear()),
        entry(HOUR, ISOChronology.getInstanceUTC().hourOfDay()),
        entry(MINUTE, ISOChronology.getInstanceUTC().minuteOfHour()),
        entry(SECOND, ISOChronology.getInstanceUTC().secondOfMinute())
    );

    public static void register(ScalarFunctionModule module) {
        for (var argType : List.of(DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMP)) {
            for (var entry : FIELDS_MAP_WITH_INT_RETURN.entrySet()) {
                module.register(
                    Signature.scalar(
                        functionNameFrom(entry.getKey()),
                        argType.getTypeSignature(),
                        parseTypeSignature("integer")
                    ),
                    (signature, argumentTypes) ->
                        new ExtractFunction(entry.getKey(), entry.getValue(), argType, signature)
                );
            }
            // extract(epoch from ...) is different as is returns a `double precision`
            module.register(
                Signature.scalar(
                    functionNameFrom(EPOCH),
                    argType.getTypeSignature(),
                    parseTypeSignature("double precision")
                ),
                (signature, argumentTypes) ->
                    new ExtractFunction(EPOCH, argType, DataTypes.DOUBLE, signature, v -> (double) v / 1000)
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
                return NAME_PREFIX + field.toString();
            case DAY_OF_MONTH:
            case DAY:
                return NAME_PREFIX + DAY_OF_MONTH.toString();
            case DAY_OF_WEEK:
            case DOW:
                return NAME_PREFIX + DAY_OF_WEEK.toString();
            case DAY_OF_YEAR:
            case DOY:
                return NAME_PREFIX + DAY_OF_YEAR.toString();
            case TIMEZONE_HOUR:
            case TIMEZONE_MINUTE:
            default:
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH, "Extract( %s from <expression>) is not supported", field));
        }
    }

    private static class ExtractFunction extends Scalar<Number, Long> {

        private final FunctionInfo info;
        private final Signature signature;
        private final Function<Long, Number> evaluate;

        ExtractFunction(Extract.Field field,
                        DateTimeField dateTimeField,
                        DataType<?> argumentType,
                        Signature signature) {
            this(field, argumentType, DataTypes.INTEGER, signature, dateTimeField::get);
        }

        ExtractFunction(Extract.Field field,
                        DataType<?> argumentType,
                        DataType<?> returnType,
                        Signature signature,
                        Function<Long, Number> evaluate) {
            info = FunctionInfo.of(functionNameFrom(field), List.of(argumentType), returnType);
            this.signature = signature;
            this.evaluate = evaluate;
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

        public Number evaluate(long value) {
            return evaluate.apply(value);
        }

        @Override
        @SafeVarargs
        public final Number evaluate(TransactionContext txnCtx, Input<Long>... args) {
            assert args.length == 1 : "extract only takes one argument";
            Long value = args[0].value();
            if (value == null) {
                return null;
            }
            return evaluate(value);
        }
    }
}
