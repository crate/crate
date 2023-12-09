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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.junit.Test;

public class OverflowAwareMutableLongTest {

    @Test
    public void test_add_without_overflow_operates_on_primitive_long() {
        var value = new OverflowAwareMutableLong(5L);
        value.add(5L);

        assertThat(value.hasValue()).isTrue();
        assertThat(value.primitiveSum()).isEqualTo(10L);
        assertThat(value.bigDecimalSum()).isEqualTo(BigDecimal.ZERO);
        assertThat(value.value()).isEqualTo(BigDecimal.TEN);
    }

    @Test
    public void test_add_results_in_overflow_should_return_correct_big_decimal_sum() {
        var value = new OverflowAwareMutableLong(Long.MAX_VALUE);
        value.add(5L);

        var expected = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.valueOf(5));
        assertThat(value.hasValue()).isTrue();
        assertThat(value.bigDecimalSum()).isEqualTo(expected);
        assertThat(value.value()).isEqualTo(expected);
    }

    @Test
    public void test_add_after_overflow_should_return_correct_big_decimal_sum() {
        var value = new OverflowAwareMutableLong(Long.MAX_VALUE);
        value.add(5L); // overflow

        value.add(5L);

        var expected = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN);
        //Last 5 summand is added after the overflow, it's kept in primitive
        var expectedBigDecimal = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.valueOf(5));

        assertThat(value.hasValue()).isTrue();
        assertThat(value.bigDecimalSum()).isEqualTo(expectedBigDecimal);
        assertThat(value.primitiveSum()).isEqualTo(5L);
        assertThat(value.value()).isEqualTo(expected);
    }
}
