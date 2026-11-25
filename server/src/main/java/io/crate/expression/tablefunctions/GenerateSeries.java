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

package io.crate.expression.tablefunctions;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.function.BinaryOperator;

import org.elasticsearch.common.settings.Settings;
import org.joda.time.Period;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.legacy.LegacySettings;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.Signature.Feature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.DataTypes;
import io.crate.types.IntervalType;
import io.crate.types.RowType;

/**
 * <pre>
 * {@code
 *      generate_series :: a -> a -> table a
 *      generate_series(start, stop)
 *
 *      generate_series :: a -> a -> a -> table a
 *      generate_series(start, stop, step)
 *
 *      where: a = Integer or Long
 * }
 * </pre>
 */
public final class GenerateSeries<T extends Number> extends TableFunctionImplementation<T> {

    public static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "generate_series");

    public static void register(Functions.Builder builder, Settings settings) {
        final List<String> fieldNames =
            LegacySettings.LEGACY_TABLE_FUNCTION_COLUMN_NAMING.get(settings) ? List.of() : List.of(NAME.name());

        // without step
        builder.add(
            Signature.builder(NAME, FunctionType.TABLE)
                .argumentTypes(DataTypes.LONG.getTypeSignature(),
                    DataTypes.LONG.getTypeSignature())
                .returnType(DataTypes.LONG.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.NOTNULL)
                .build(),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1L,
                Long::sum,
                Long::compare,
                new RowType(List.of(boundSignature.argTypes().get(0)), fieldNames))
        );
        builder.add(
            Signature.builder(NAME, FunctionType.TABLE)
                .argumentTypes(DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.NOTNULL)
                .build(),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1,
                Integer::sum,
                Integer::compare,
                new RowType(List.of(boundSignature.argTypes().get(0)), fieldNames))
        );

        // with step
        builder.add(
            Signature.builder(NAME, FunctionType.TABLE)
                .argumentTypes(DataTypes.LONG.getTypeSignature(),
                    DataTypes.LONG.getTypeSignature(),
                    DataTypes.LONG.getTypeSignature())
                .returnType(DataTypes.LONG.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.NOTNULL)
                .build(),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1L,
                Long::sum,
                Long::compare,
                new RowType(List.of(boundSignature.argTypes().get(0)), fieldNames))
        );
        builder.add(
            Signature.builder(NAME, FunctionType.TABLE)
                .argumentTypes(DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.NOTNULL)
                .build(),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1,
                Integer::sum,
                Integer::compare,
                new RowType(List.of(boundSignature.argTypes().get(0)), fieldNames))
        );

        // generate_series(ts, ts, interval)
        for (var supportedType : List.of(DataTypes.TIMESTAMP, DataTypes.TIMESTAMPZ)) {
            builder.add(
                Signature.builder(NAME, FunctionType.TABLE)
                    .argumentTypes(supportedType.getTypeSignature(),
                        supportedType.getTypeSignature(),
                        DataTypes.INTERVAL.getTypeSignature())
                    .returnType(supportedType.getTypeSignature())
                    .features(Feature.DETERMINISTIC, Feature.NOTNULL)
                    .build(),
                (signature, boundSignature) -> new GenerateSeriesIntervals(
                    signature,
                    boundSignature,
                    new RowType(List.of(boundSignature.argTypes().get(0)), fieldNames))
            );
            builder.add(
                Signature.builder(NAME, FunctionType.TABLE)
                    .argumentTypes(supportedType.getTypeSignature(),
                        supportedType.getTypeSignature())
                    .returnType(supportedType.getTypeSignature())
                    .features(Feature.DETERMINISTIC)
                    .build(),
                (signature, boundSignature) -> {
                    throw new IllegalArgumentException(
                        "generate_series(start, stop) has type `" + boundSignature.argTypes().get(0).getName() +
                            "` for start, but requires long/int values for start and stop, " +
                            "or if used with timestamps, it requires a third argument for the step (interval)");
                }
            );
        }
    }

    private final T defaultStep;
    private final BinaryOperator<T> plus;
    private final Comparator<T> comparator;
    private final RowType returnType;

    private GenerateSeries(Signature signature,
                           BoundSignature boundSignature,
                           T defaultStep,
                           BinaryOperator<T> plus,
                           Comparator<T> comparator,
                           RowType returnType) {
        super(signature, boundSignature);
        this.defaultStep = defaultStep;
        this.plus = plus;
        this.comparator = comparator;
        this.returnType = returnType;
    }

    @Override
    public Iterable<Row> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<T>... args) {
        assert args.length == 2 || args.length == 3 :
            "Signature must ensure that there are either two or three arguments";

        T startInclusive = args[0].value();
        T stopInclusive = args[1].value();
        T step = args.length == 3 ? args[2].value() : defaultStep;
        if (startInclusive == null || stopInclusive == null || step == null) {
            return List.of();
        }
        if (step.longValue() == 0) {
            throw new IllegalArgumentException("step size cannot equal zero");
        }
        boolean reverse = step.longValue() < 0;
        return new RangeIterable<>(
            startInclusive,
            stopInclusive,
            reverse,
            value -> plus.apply(value, step),
            comparator,
            t -> t
        );
    }

    @Override
    public RowType returnType() {
        return returnType;
    }

    @Override
    public boolean hasLazyResultSet() {
        return true;
    }

    private static class GenerateSeriesIntervals extends TableFunctionImplementation<Object> {

        private final RowType returnType;

        public GenerateSeriesIntervals(Signature signature, BoundSignature boundSignature, RowType returnType) {
            super(signature, boundSignature);
            this.returnType = returnType;
        }

        @Override
        public Iterable<Row> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
            Long startInclusive = (Long) args[0].value();
            Long stopInclusive = (Long) args[1].value();
            Period step = (Period) args[2].value();
            if (startInclusive == null || stopInclusive == null || step == null) {
                return List.of();
            }
            ZonedDateTime start = Instant.ofEpochMilli(startInclusive).atZone(ZoneOffset.UTC);
            ZonedDateTime stop = Instant.ofEpochMilli(stopInclusive).atZone(ZoneOffset.UTC);
            int stepSignum = IntervalType.toStandardDuration(step).signum();
            if (stepSignum == 0) {
                throw new IllegalArgumentException("step size cannot equal zero");
            }
            boolean reverse = stepSignum < 0;
            return new RangeIterable<>(
                start,
                stop,
                reverse,
                (value) -> add(value, step),
                ZonedDateTime::compareTo,
                value -> value.toInstant().toEpochMilli()
            );
        }

        private static ZonedDateTime add(ZonedDateTime dateTime, Period step) {
            return dateTime
                .plusYears(step.getYears())
                .plusMonths(step.getMonths())
                .plusWeeks(step.getWeeks())
                .plusDays(step.getDays())
                .plusHours(step.getHours())
                .plusMinutes(step.getMinutes())
                .plusSeconds(step.getSeconds())
                .plusNanos(step.getMillis() * 1_000_000L);
        }

        @Override
        public RowType returnType() {
            return returnType;
        }

        @Override
        public boolean hasLazyResultSet() {
            return true;
        }
    }
}
