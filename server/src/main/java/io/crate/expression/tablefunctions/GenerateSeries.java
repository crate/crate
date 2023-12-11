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

import org.joda.time.Period;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.legacy.LegacySettings;
import io.crate.metadata.FunctionName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.DataTypes;
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

    public static void register(TableFunctionModule module) {
        final List<String> fieldNames =
            LegacySettings.LEGACY_TABLE_FUNCTION_COLUMN_NAMING.get(module.settings()) ? List.of() : List.of(NAME.name());

        // without step
        module.register(
            Signature.table(
                NAME,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ).withFeature(Feature.NON_NULLABLE),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1L,
                (x, y) -> x - y,
                Long::sum,
                (x, y) -> x / y,
                Long::compare,
                new RowType(List.of(boundSignature.argTypes().get(0)), fieldNames))
        );
        module.register(
            Signature.table(
                NAME,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ).withFeature(Feature.NON_NULLABLE),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1,
                (x, y) -> x - y,
                Integer::sum,
                (x, y) -> x / y,
                Integer::compare,
                new RowType(List.of(boundSignature.argTypes().get(0)), fieldNames))
        );

        // with step
        module.register(
            Signature.table(
                NAME,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ).withFeature(Feature.NON_NULLABLE),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1L,
                (x, y) -> x - y,
                Long::sum,
                (x, y) -> x / y,
                Long::compare,
                new RowType(List.of(boundSignature.argTypes().get(0)), fieldNames))
        );
        module.register(
            Signature.table(
                NAME,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ).withFeature(Feature.NON_NULLABLE),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1,
                (x, y) -> x - y,
                Integer::sum,
                (x, y) -> x / y,
                Integer::compare,
                new RowType(List.of(boundSignature.argTypes().get(0)), fieldNames))
        );

        // generate_series(ts, ts, interval)
        for (var supportedType : List.of(DataTypes.TIMESTAMP, DataTypes.TIMESTAMPZ)) {
            module.register(
                Signature.table(
                    NAME,
                    supportedType.getTypeSignature(),
                    supportedType.getTypeSignature(),
                    DataTypes.INTERVAL.getTypeSignature(),
                    supportedType.getTypeSignature()
                ).withFeature(Feature.NON_NULLABLE),
                (signature, boundSignature) -> new GenerateSeriesIntervals(
                    signature,
                    boundSignature,
                    new RowType(List.of(boundSignature.argTypes().get(0)), fieldNames))
            );
            module.register(
                Signature.table(
                    NAME,
                    supportedType.getTypeSignature(),
                    supportedType.getTypeSignature(),
                    supportedType.getTypeSignature()
                ),
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
    private final BinaryOperator<T> minus;
    private final BinaryOperator<T> plus;
    private final BinaryOperator<T> divide;
    private final Comparator<T> comparator;
    private final RowType returnType;

    private GenerateSeries(Signature signature,
                           BoundSignature boundSignature,
                           T defaultStep,
                           BinaryOperator<T> minus,
                           BinaryOperator<T> plus,
                           BinaryOperator<T> divide,
                           Comparator<T> comparator,
                           RowType returnType) {
        super(signature, boundSignature);
        this.defaultStep = defaultStep;
        this.minus = minus;
        this.plus = plus;
        this.divide = divide;
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
        T diff = minus.apply(plus.apply(stopInclusive, step), startInclusive);
        final int numRows = Math.max(0, divide.apply(diff, step).intValue());
        if (numRows == 0) {
            return List.of();
        }
        return new RangeIterable<T>(
            startInclusive,
            stopInclusive,
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
            boolean reverse = start.compareTo(stop) > 0;
            if (reverse && add(start, step).compareTo(start) >= 0) {
                return List.of();
            }
            return new RangeIterable<>(
                start,
                stop,
                (value) -> add(value, step),
                ZonedDateTime::compareTo,
                value -> value.toEpochSecond() * 1000
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
                .plusNanos(step.getMillis() * 1000_0000L);
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
