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

package io.crate.execution.engine.aggregation.impl;

import io.crate.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Objects;

public class OverflowAwareMutableLong {

    private long primitiveSum;
    private BigDecimal bigDecimalSum = null;
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
        if (bigDecimalSum == null) {
            long newSum = primitiveSum + value;
            if (((primitiveSum ^ newSum) & (value ^ newSum)) < 0) {
                bigDecimalSum = BigDecimal.valueOf(primitiveSum).add(BigDecimal.valueOf(value));
            } else {
                primitiveSum = newSum;
            }
        } else {
            bigDecimalSum = bigDecimalSum.add(BigDecimal.valueOf(value));
        }
    }

    public BigDecimal value() {
        return Objects.requireNonNullElseGet(
            bigDecimalSum,
            () -> BigDecimal.valueOf(primitiveSum)
        );
    }
}
