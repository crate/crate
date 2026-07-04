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

package io.crate.expression.scalar;

import java.math.BigDecimal;
import java.util.List;

import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.types.ArrayType;
import io.crate.types.DoubleType;
import io.crate.types.LongType;
import io.crate.types.NumericType;

public class CollectionSumFunctionTest extends ScalarTestCase {

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("collection_sum(long_array)",
            10L,
            Literal.of(List.of(3L, 7L), new ArrayType<>(LongType.INSTANCE)));
    }

    @Test
    public void testEvaluateDouble() throws Exception {
        assertEvaluate("collection_sum(double_array)",
            10.5,
            Literal.of(List.of(3.2, 7.3), new ArrayType<>(DoubleType.INSTANCE)));
    }

    @Test
    public void testEvaluateNumeric() throws Exception {
        assertEvaluate("collection_sum(numeric_array)",
            new BigDecimal("4.0"),
            Literal.of(List.of(new BigDecimal("1.5"), new BigDecimal("2.5")), new ArrayType<>(NumericType.INSTANCE)));
    }
}
