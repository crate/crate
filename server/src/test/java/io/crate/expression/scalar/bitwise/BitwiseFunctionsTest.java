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

package io.crate.expression.scalar.bitwise;


import static io.crate.types.DataTypes.BYTE;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.SHORT;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.elasticsearch.common.TriFunction;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.sql.tree.BitString;
import io.crate.testing.DataTypeTesting;
import io.crate.types.BitStringType;
import io.crate.types.DataType;

public class BitwiseFunctionsTest extends ScalarTestCase {


    @Test
    public void test_bit_string_operands_must_have_equal_length() {
        Assertions.assertThatThrownBy(() -> assertEvaluate("B'10001' | B'001'", null))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot OR bit strings of different sizes");
    }

    @Test
    public void test_at_least_one_operand_is_null_return_null() {
        assertEvaluateNull("1 & null");
        assertEvaluateNull("null & 1");
        assertEvaluateNull("null & null");
    }

    @Test
    public void test_all_bitwise_operators_on_numeric_integral_types() {
        List<DataType> types = List.of(LONG, INTEGER, SHORT, BYTE);

        // For computing expected values.
        Map<String, TriFunction<DataType<Number>, Number, Number, Number>> referenceOperators = new HashMap<>();
        referenceOperators.put("&", (t, a, b) -> t.implicitCast(a.longValue() & b.longValue()));
        referenceOperators.put("|", (t, a, b) -> t.implicitCast(a.longValue() | b.longValue()));
        referenceOperators.put("#", (t, a, b) -> t.implicitCast(a.longValue() ^ b.longValue()));

        for (DataType type : types) {
            Supplier<Number> generator = DataTypeTesting.getDataGenerator(type); // Returns only not-null values.
            for (Map.Entry<String, TriFunction<DataType<Number>, Number, Number, Number>> entry: referenceOperators.entrySet()) {
                var a = generator.get();
                var b = generator.get();
                String expression = String.format(
                    Locale.ENGLISH,
                    "%d::%s %s %d::%s",
                    a, type.getName(), entry.getKey(), b, type.getName()
                );
                assertEvaluate(expression, entry.getValue().apply(type, a, b));
            }
        }
    }

    @Test
    public void test_all_bitwise_operators_on_bit_string() {
        // For computing expected values
        Map<String, BinaryOperator<BitString>> referenceOperators = new HashMap<>();

        referenceOperators.put("&", (bs1, bs2) -> {
            bs1.bitSet().and(bs2.bitSet());
            return bs1;
        });
        referenceOperators.put("|", (bs1, bs2) -> {
            bs1.bitSet().or(bs2.bitSet());
            return bs1;
        });
        referenceOperators.put("#", (bs1, bs2) -> {
            bs1.bitSet().xor(bs2.bitSet());
            return bs1;
        });

        var bitType = new BitStringType(randomIntBetween(1,100));
        Supplier<BitString> generator = DataTypeTesting.getDataGenerator(bitType);
        for (Map.Entry<String, BinaryOperator<BitString>> entry: referenceOperators.entrySet()) {
            var a = generator.get();
            var b = generator.get();
            String expression = String.format(
                Locale.ENGLISH,
                "%s %s %s",
                a.asPrefixedBitString(), entry.getKey(), b.asPrefixedBitString()
            );
            assertEvaluate(expression, entry.getValue().apply(a, b));
        }
    }
}
