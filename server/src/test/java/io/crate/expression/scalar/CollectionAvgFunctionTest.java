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

package io.crate.expression.scalar;

import java.math.BigDecimal;
import java.util.List;

import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.types.ArrayType;
import io.crate.types.LongType;
import io.crate.types.NumericType;

public class CollectionAvgFunctionTest extends ScalarTestCase {

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate(
            "collection_avg(long_array)",
            5.0,
            Literal.of(List.of(3L, 7L), new ArrayType<>(LongType.INSTANCE))
        );
    }

    @Test
    public void test_evaluate_numeric() throws Exception {
        assertEvaluate(
            "collection_avg(numeric_array)",
            new BigDecimal("2.440"),
            Literal.of(
                List.of(new BigDecimal("1.325"), new BigDecimal("2.345"), new BigDecimal("3.65")),
                new ArrayType<>(new NumericType(5, 3)))
        );
    }
}
