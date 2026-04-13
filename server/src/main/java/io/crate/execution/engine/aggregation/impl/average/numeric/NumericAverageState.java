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
import java.math.MathContext;
import java.util.Objects;

public class NumericAverageState implements Comparable<NumericAverageState> {

    BigDecimal sum;

    long count;

    NumericAverageState(BigDecimal initialValue, long count) {
        this.sum = initialValue;
        // Constructor argument for count is added since NumericAverageStateType.readValueFrom() can get value different from 0.
        this.count = count;
    }


    public BigDecimal value() {
        if (count > 0) {
            // We need to use divide with a MathContext to avoid ArithmeticException on infinite fractions.
            // Using best (finite) precision as we want to compute final result with the best precision.
            // If users want to reduce the precision of the final result, they can use explicit cast.
            return sum.divide(BigDecimal.valueOf(count), MathContext.DECIMAL128);
        } else {
            return null;
        }
    }

    @Override
    public int compareTo(NumericAverageState o) {
        if (o == null) {
            return 1;
        } else {
            int compare = Long.compare(count, o.count);
            if (compare == 0) {
                return sum.compareTo(o.sum);
            }
            return compare;
        }
    }

    @Override
    public String toString() {
        return "sum: " + sum + " count: " + count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NumericAverageState that = (NumericAverageState) o;
        return count == that.count && value().equals(that.value());
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, sum);
    }
}
