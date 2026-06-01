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

package io.crate.protocols.postgres.types;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class NumericTypeTest extends BasePGTypeTest<BigDecimal> {

    public NumericTypeTest() {
        super(NumericType.INSTANCE);
    }

    private static final List<BigDecimal> TEST_NUMBERS = List.of(
        new BigDecimal("0.1"),
        new BigDecimal("0.10"),
        new BigDecimal("0.01"),
        new BigDecimal("0.001"),
        new BigDecimal("0.0001"),
        new BigDecimal("0.00001"),
        new BigDecimal("1.0"),
        new BigDecimal("0.000000000000000000000000000000000000000000000000000"),
        new BigDecimal("0.100000000000000000000000000000000000000000000009900"),
        new BigDecimal("-1.0"),
        new BigDecimal("-1"),
        new BigDecimal("1.2"),
        new BigDecimal("-2.05"),
        new BigDecimal("0.000000000000000000000000000990"),
        new BigDecimal("-0.000000000000000000000000000990"),
        new BigDecimal("10.0000000000099"),
        new BigDecimal(".10000000000000"),
        new BigDecimal("1.10000000000000"),
        new BigDecimal("99999.2"),
        new BigDecimal("99999"),
        new BigDecimal("-99999.2"),
        new BigDecimal("-99999"),
        new BigDecimal("2147483647"),
        new BigDecimal("-2147483648"),
        new BigDecimal("2147483648"),
        new BigDecimal("-2147483649"),
        new BigDecimal("9223372036854775807"),
        new BigDecimal("-9223372036854775808"),
        new BigDecimal("9223372036854775808"),
        new BigDecimal("-9223372036854775809"),
        new BigDecimal("10223372036850000000"),
        new BigDecimal("19223372036854775807"),
        new BigDecimal("19223372036854775807.300"),
        new BigDecimal("-19223372036854775807.300"),
        new BigDecimal(BigInteger.valueOf(1234567890987654321L), -1),
        new BigDecimal(BigInteger.valueOf(1234567890987654321L), -5),
        new BigDecimal(BigInteger.valueOf(-1234567890987654321L), -3),
        new BigDecimal(BigInteger.valueOf(6), -8),
        new BigDecimal("30000"),
        new BigDecimal("40000").setScale(15, MathContext.UNLIMITED.getRoundingMode()),
        new BigDecimal("20000.000000000000000000"),
        new BigDecimal("9990000").setScale(8, MathContext.UNLIMITED.getRoundingMode()),
        new BigDecimal("1000000").setScale(31, MathContext.UNLIMITED.getRoundingMode()),
        new BigDecimal("10000000000000000000000000000000000000").setScale(14, MathContext.UNLIMITED.getRoundingMode()),
        new BigDecimal("90000000000000000000000000000000000000"),
        new BigDecimal(new BigInteger("3411016753891"), 162),
        new BigDecimal(new BigInteger("3411016753891"), 162).negate(),
        new BigDecimal("-1.3046661500662875E-95"),
        new BigDecimal("-1.3046661500662875E195")
   );

    @Test
    public void test_read_and_write_numeric_text_value() {
        for (var expected : TEST_NUMBERS) {
            ByteBuf buffer = Unpooled.buffer();
            try {
                pgType.writeAsText(buffer, expected);
                var actual = pgType.readTextValue(buffer, buffer.readInt(), null);
                assertThat(actual).isEqualTo(expected);
                assertThat(actual.toString()).isEqualTo(expected.toString());
            } finally {
                buffer.release();
            }
        }
    }

    @Test
    public void test_read_and_write_numeric_binary_value() {
        for (var expected : TEST_NUMBERS) {
            ByteBuf buffer = Unpooled.buffer();
            try {
                pgType.writeAsBinary(buffer, expected);
                BigDecimal actual = pgType.readBinaryValue(buffer, buffer.readInt());
                assertThat(actual.scale()).isEqualTo(expected.scale());
                assertThat(actual.toString()).isEqualTo(expected.toString());
            } finally {
                buffer.release();
            }
        }
    }
}
