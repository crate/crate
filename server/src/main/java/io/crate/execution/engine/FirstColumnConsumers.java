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

package io.crate.execution.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import io.crate.data.Row;
import io.crate.expression.symbol.SelectSymbol.ResultType;

/**
 * Collectors to retrieve either {@link AllValues} or a {@link SingleValue} of the first column of each row.
 */
public class FirstColumnConsumers {

    private static class AllValues implements Collector<Row, List<Object>, List<Object>> {

        public static final AllValues INSTANCE = new AllValues();

        private AllValues() {
        }

        @Override
        public Supplier<List<Object>> supplier() {
            return () -> new ArrayList<>(1);
        }

        @Override
        public BiConsumer<List<Object>, Row> accumulator() {
            return (agg, row) -> {
                agg.add(row.get(0));
            };
        }

        @Override
        public BinaryOperator<List<Object>> combiner() {
            throw new IllegalStateException("Combine is not implemented on this collector");
        }

        @Override
        public Function<List<Object>, List<Object>> finisher() {
            return Function.identity();
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }

    }

    private static class SingleValue implements Collector<Row, Object[], Object> {

        public static final SingleValue INSTANCE = new SingleValue();

        /* We need this Object to differentiate null values */
        private static final Object SENTINEL = new Object();

        private SingleValue() {
        }

        @Override
        public Supplier<Object[]> supplier() {
            return () -> new Object[] { SENTINEL };
        }

        @Override
        public BiConsumer<Object[], Row> accumulator() {
            return (agg, row) -> {
                if (agg[0] != SENTINEL) {
                    throw new UnsupportedOperationException("Subquery returned more than 1 row when it shouldn't.");
                }
                agg[0] = row.get(0);
            };
        }

        @Override
        public BinaryOperator<Object[]> combiner() {
            throw new IllegalStateException("Combine is not implemented on this collector");
        }

        @Override
        public Function<Object[], Object> finisher() {
            return agg -> {
                if (agg[0] == SENTINEL) {
                    return null;
                }
                return agg[0];
            };
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }

    }

    public static Collector<Row, ?, ?> getCollector(ResultType resultType) {
        return switch (resultType) {
            case SINGLE_COLUMN_MULTIPLE_VALUES -> AllValues.INSTANCE;
            case SINGLE_COLUMN_SINGLE_VALUE -> SingleValue.INSTANCE;
            case SINGLE_COLUMN_EXISTS -> AllValues.INSTANCE;
        };
    }
}
