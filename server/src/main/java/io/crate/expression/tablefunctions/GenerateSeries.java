/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.tablefunctions;

import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.FunctionName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.DataTypes;
import io.crate.types.RowType;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BinaryOperator;

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
        // without step
        module.register(
            Signature.table(
                NAME,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1L,
                (x, y) -> x - y,
                Long::sum,
                (x, y) -> x / y,
                Long::compare)
        );
        module.register(
            Signature.table(
                NAME,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1,
                (x, y) -> x - y,
                Integer::sum,
                (x, y) -> x / y,
                Integer::compare)
        );

        // with step
        module.register(
            Signature.table(
                NAME,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1L,
                (x, y) -> x - y,
                Long::sum,
                (x, y) -> x / y,
                Long::compare)
        );
        module.register(
            Signature.table(
                NAME,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ),
            (signature, boundSignature) -> new GenerateSeries<>(
                signature,
                boundSignature,
                1,
                (x, y) -> x - y,
                Integer::sum,
                (x, y) -> x / y,
                Integer::compare)
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
                ),
                GenerateSeriesIntervals::new
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
                        "generate_series(start, stop) has type `" + boundSignature.getArgumentDataTypes().get(0).getName() +
                        "` for start, but requires long/int values for start and stop, " +
                        "or if used with timestamps, it requires a third argument for the step (interval)");
                }
            );
        }
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final T defaultStep;
    private final BinaryOperator<T> minus;
    private final BinaryOperator<T> plus;
    private final BinaryOperator<T> divide;
    private final Comparator<T> comparator;
    private final RowType returnType;

    private GenerateSeries(Signature signature,
                           Signature boundSignature,
                           T defaultStep,
                           BinaryOperator<T> minus,
                           BinaryOperator<T> plus,
                           BinaryOperator<T> divide,
                           Comparator<T> comparator) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.defaultStep = defaultStep;
        this.minus = minus;
        this.plus = plus;
        this.divide = divide;
        this.comparator = comparator;
        this.returnType = new RowType(List.of(boundSignature.getArgumentDataTypes().get(0)));
    }

    @Override
    public Bucket evaluate(TransactionContext txnCtx, Input<T>... args) {
        T startInclusive = args[0].value();
        T stopInclusive = args[1].value();
        T step = args.length == 3 ? args[2].value() : defaultStep;
        if (startInclusive == null || stopInclusive == null || step == null) {
            return Bucket.EMPTY;
        }
        T diff = minus.apply(plus.apply(stopInclusive, step), startInclusive);
        final int numRows = Math.max(0, divide.apply(diff, step).intValue());
        final boolean reverseCompare = comparator.compare(startInclusive, stopInclusive) > 0 && numRows > 0;
        final Object[] cells = new Object[1];
        cells[0] = startInclusive;
        final RowN rowN = new RowN(cells);
        return new Bucket() {
            @Override
            public int size() {
                return numRows;
            }

            @Override
            @Nonnull
            public Iterator<Row> iterator() {
                return new Iterator<>() {
                    boolean doStep = false;
                    T val = startInclusive;

                    @Override
                    public boolean hasNext() {
                        if (doStep) {
                            val = plus.apply(val, step);
                            doStep = false;
                        }
                        int compare = comparator.compare(val, stopInclusive);
                        if (reverseCompare) {
                            return compare >= 0;
                        } else {
                            return compare <= 0;
                        }
                    }

                    @Override
                    public Row next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException("Iterator has no more elements");
                        }
                        doStep = true;
                        cells[0] = val;
                        return rowN;
                    }
                };
            }
        };
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
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
        private final Signature signature;
        private final Signature boundSignature;

        public GenerateSeriesIntervals(Signature signature, Signature boundSignature) {
            this.signature = signature;
            this.boundSignature = boundSignature;
            returnType = new RowType(List.of(boundSignature.getArgumentDataTypes().get(0)));
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public Signature boundSignature() {
            return boundSignature;
        }

        @Override
        public Iterable<Row> evaluate(TransactionContext txnCtx, Input<Object>... args) {
            Long startInclusive = (Long) args[0].value();
            Long stopInclusive = (Long) args[1].value();
            Period step = (Period) args[2].value();
            if (startInclusive == null || stopInclusive == null || step == null) {
                return Bucket.EMPTY;
            }
            ZonedDateTime start = Instant.ofEpochMilli(startInclusive).atZone(ZoneOffset.UTC);
            ZonedDateTime stop = Instant.ofEpochMilli(stopInclusive).atZone(ZoneOffset.UTC);
            boolean reverse = start.compareTo(stop) > 0;
            if (reverse && add(start, step).compareTo(start) >= 0) {
                return Bucket.EMPTY;
            }
            return () -> new Iterator<>() {

                final Object[] cells = new Object[1];
                final RowN rowN = new RowN(cells);

                ZonedDateTime value = start;
                boolean doStep = false;

                @Override
                public boolean hasNext() {
                    if (doStep) {
                        value = add(value, step);
                        doStep = false;
                    }
                    int compare = value.compareTo(stop);
                    return reverse
                        ? compare >= 0
                        : compare <= 0;
                }

                @Override
                public Row next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException("No more element in generate_series");
                    }
                    doStep = true;
                    cells[0] = (value.toEpochSecond() * 1000);
                    return rowN;
                }
            };
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
