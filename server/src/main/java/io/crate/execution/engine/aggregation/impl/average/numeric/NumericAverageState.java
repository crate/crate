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

package io.crate.execution.engine.aggregation.impl.average.numeric;

import java.math.BigDecimal;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;

import io.crate.execution.engine.aggregation.impl.util.NumericValueHolder;

public class NumericAverageState<T extends NumericValueHolder> implements Comparable<NumericAverageState<T>> {

    @NotNull
    T sum;

    long count;

    NumericAverageState(T initialValue, long count) {
        this.sum = initialValue;
        // Constructor argument for count is added since NumericAverageStateType.readValueFrom() can get value different from 0.
        this.count = count;
    }


    public BigDecimal value() {
        if (count > 0) {
            return sum.value().divide(BigDecimal.valueOf(count));
        } else {
            return null;
        }
    }

    @Override
    public int compareTo(NumericAverageState<T> o) {
        if (o == null) {
            return 1;
        } else {
            int compare = Long.compare(count, o.count);
            if (compare == 0) {
                return sum.value().compareTo(o.sum.value());
            }
            return compare;
        }
    }

    @Override
    public String toString() {
        return "sum: " + sum + " count: " + count;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NumericAverageState<T> that = (NumericAverageState<T>) o;
        return count == that.count && sum.value().equals(that.value());
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, sum);
    }
}
