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

package io.crate.execution.engine.aggregation.impl.util;

import org.jetbrains.annotations.VisibleForTesting;

import org.jetbrains.annotations.Nullable;
import java.math.BigDecimal;

public class OverflowAwareMutableLong implements NumericValueHolder {

    private long primitiveSum;
    private BigDecimal bigDecimalSum = BigDecimal.ZERO;
    private boolean hasValue;

    public OverflowAwareMutableLong(long value) {
        primitiveSum = value;
    }

    public boolean hasValue() {
        return hasValue;
    }

    @VisibleForTesting
    long primitiveSum() {
        return primitiveSum;
    }

    @Nullable
    @VisibleForTesting
    BigDecimal bigDecimalSum() {
        return bigDecimalSum;
    }

    public void add(long value) {
        hasValue = true;
        // Check for overflow before it happens, taken from Math.addExact
        long newSum = primitiveSum + value;
        if (((primitiveSum ^ newSum) & (value ^ newSum)) < 0) {
            // Overflow is about to happen, cannot add.
            // Flushing gathered primitive sum and value to BigDecimal
            bigDecimalSum = bigDecimalSum.add(BigDecimal.valueOf(primitiveSum)).add(BigDecimal.valueOf(value));
            primitiveSum = 0; //Reset primitive after the flushing
        } else {
            // Keep gathering in primitive, overflow is not happening here.
            primitiveSum += value;
        }
    }

    @Override
    public BigDecimal value() {
        // Adding residual unflushed value before return.
        return bigDecimalSum.add(BigDecimal.valueOf(primitiveSum));
    }

    @Override
    public void setValue(BigDecimal value) {
        throw new UnsupportedOperationException("setValue() is not supported");
    }
}
