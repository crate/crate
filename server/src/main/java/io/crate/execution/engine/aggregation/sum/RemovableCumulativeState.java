/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.aggregation.sum;

import java.util.function.BinaryOperator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import io.crate.types.DataType;

/**
 * State for SUM aggregations used with window functions.
 * Keeps track of a not-null elements in a window
 * to reset state back to NULL when last not null element is removed from the window.
 *
 * This is mainly needed to not "infect" next frame that has only NULL-s with a not null value.
 */
public final class RemovableCumulativeState<T> implements Accountable {

    public static long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(RemovableCumulativeState.class);
    private T value;
    private long count = 0;
    private final DataType<T> valueType;

    public RemovableCumulativeState(@Nullable T value, DataType<T> valueType, long count) {
        this.value = value;
        this.valueType = valueType;
        this.count = count;
    }

    public void add(BinaryOperator<T> addition, @NonNull T newValue) {
        if (count == 0) {
            value = newValue;
        } else {
            value = addition.apply(newValue, value);
        }
        count++;
    }

    public void remove(BinaryOperator<T> subtraction, @NonNull T valueToRemove) {
        value = subtraction.apply(value, valueToRemove);
        count--;
    }

    public void merge(BinaryOperator<T> addition, RemovableCumulativeState<T> other) {
        if (other.count == 0) {
            return;
        }
        if (count == 0) {
            value = other.value;
        } else {
            value = addition.apply(value, other.value);
        }
        count += other.count;
    }

    @Nullable
    public T value() {
        return count == 0 ? null : value;
    }

    public long count() {
        return count;
    }

    public DataType<T> valueType() {
        return valueType;
    }

    @Override
    public long ramBytesUsed() {
        // valueBytes is safe for null values.
        // For primitives and interval it's fixed, numeric has null check.
        return SHALLOW_SIZE + valueType.valueBytes(value);
    }
}
