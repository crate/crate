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

package io.crate.breaker;

import java.util.List;
import java.util.function.IntFunction;

import io.crate.data.Row;
import io.crate.types.DataType;

/**
 * Estimates the memory footprint of a row represented as an array of java objects
 */
public abstract class CellsSizeEstimator {

    protected CellsSizeEstimator() { }

    /**
     * Create a CellsSizeEstimator instance that uses an ordered list of column types
     * to estimate the memory footprint of a row, represented as an array of java objects
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static CellsSizeEstimator forColumns(List<? extends DataType<?>> columnTypes) {
        return new CellsSizeEstimator() {
            @Override
            protected long estimateSize(int valueCount, IntFunction<Object> values) {
                assert columnTypes.size() == valueCount
                    : "Size of incoming cells must match number of estimators. "
                    + "Cells=" + valueCount
                    + " estimators=" + columnTypes.size();
                long size = 0;
                for (int i = 0; i < valueCount; i++) {
                    DataType dataType = columnTypes.get(i);
                    size += dataType.valueBytes(values.apply(i));
                }
                return size;
            }
        };
    }

    /**
     * Create a CellSizeEstimator instance that always returns a constant value
     */
    public static CellsSizeEstimator constant(long value) {
        return new CellsSizeEstimator() {
            @Override
            protected long estimateSize(int valueCount, IntFunction<Object> values) {
                return value;
            }
        };
    }

    /**
     * Estimate the memory footprint of a set of values
     * @param valueCount    the number of values
     * @param values        a mapping from the value index to the value object
     */
    protected abstract long estimateSize(int valueCount, IntFunction<Object> values);

    /**
     * Estimate the memory footprint of a {@link Row}
     */
    public final long estimateSize(Row row) {
        return estimateSize(row.numColumns(), row::get);
    }

    /**
     * Estimate the memory footprint of a row, represented as an array of java objects
     */
    public final long estimateSize(Object[] cells) {
        return estimateSize(cells.length, i -> cells[i]);
    }
}
