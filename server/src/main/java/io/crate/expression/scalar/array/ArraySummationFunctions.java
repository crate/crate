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

package io.crate.expression.scalar.array;

import io.crate.execution.engine.aggregation.impl.util.KahanSummationForDouble;
import io.crate.execution.engine.aggregation.impl.util.KahanSummationForFloat;
import io.crate.execution.engine.aggregation.impl.util.OverflowAwareMutableLong;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;

public enum ArraySummationFunctions {

    NUMERIC(
        (Function<List<BigDecimal>, BigDecimal>) bigDecimals -> {
            BigDecimal sum = BigDecimal.ZERO;
            boolean hasNotNull = false;
            for (int i = 0; i < bigDecimals.size(); i++) {
                var value = bigDecimals.get(i);
                if (value != null) {
                    hasNotNull = true;
                    sum = sum.add(value);
                }
            }
            return hasNotNull ? sum : null;
        }
    ),
    FLOAT(
        (Function<List<Float>, Float>) floats -> {
            var kahanSummationForFloat = new KahanSummationForFloat();
            float sum = 0;
            boolean hasNotNull = false;
            for (int i = 0; i < floats.size(); i++) {
                var value = floats.get(i);
                if (value != null) {
                    hasNotNull = true;
                    sum = kahanSummationForFloat.sum(sum, value);
                }
            }
            return hasNotNull ? sum : null;
        }
    ),
    DOUBLE(
        (Function<List<Double>, Double>) list -> {
            var kahanSummationForDouble = new KahanSummationForDouble();
            double sum = 0;
            boolean hasNotNull = false;
            for (int i = 0; i < list.size(); i++) {
                var value = list.get(i);
                if (value != null) {
                    hasNotNull = true;
                    sum = kahanSummationForDouble.sum(sum, value);
                }
            }
            return hasNotNull ? sum : null;
        }
    ),
    PRIMITIVE_NON_FLOAT_OVERFLOWING(
        // Covers Byte, Short, Integer, Long types.
        (Function<List<Number>, Long>) list -> {
            Long sum = 0L;
            boolean hasNotNull = false;
            for (int i = 0; i < list.size(); i++) {
                var value = list.get(i);
                if (value != null) {
                    hasNotNull = true;
                    sum = Math.addExact(sum, value.longValue());
                }
            }
            return hasNotNull ? sum : null;
        }
    ),
    PRIMITIVE_NON_FLOAT_NOT_OVERFLOWING(
        // Covers Byte, Short, Integer, Long types.
        // Used for nominator calculation in array_avg as average is not supposed to overflow for primitive types.
        (Function<List<Number>, BigDecimal>) list -> {
            var overflowAwareMutableLong = new OverflowAwareMutableLong(0L);
            for (int i = 0; i < list.size(); i++) {
                var value = list.get(i);
                if (value != null) {
                    overflowAwareMutableLong.add(value.longValue());
                }
            }
            return overflowAwareMutableLong.hasValue() ? overflowAwareMutableLong.value() : null;
        }
    );

    private final Function function;

    ArraySummationFunctions(Function function) {
        this.function = function;
    }

    public Function getFunction() {
        return function;
    }
}
