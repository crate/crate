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

package io.crate.execution.engine.aggregation.impl;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataType;
import io.crate.types.NumericType;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(RandomizedRunner.class)
public class NumericMaxAggregationTest extends AggregationTestCase {

    private final String displayName;
    private final NumericType type;
    private final Object[][] data;
    private final BigDecimal expected;

    public NumericMaxAggregationTest(String displayName, NumericType type, Object[][] data, BigDecimal expected) {
        this.displayName = displayName;
        this.type = type;
        this.data = data;
        this.expected = expected;
    }

    @ParametersFactory(argumentFormatting = "%s")
    public static Iterable<Object[]> parameters() {
        // Numeric values with the precision <= 18 are stored as long values
        // in the column store, so we're testing both cases.
        // See NumericStorage for more info.
        return List.of(
            new Object[]{
                "compact precision, simple case",
                new NumericType(6, 4),
                new Object[][]{{new BigDecimal("97.6543")}, {new BigDecimal("97.6542")}},
                new BigDecimal("97.6543")
            },
            new Object[]{
                "large precision, simple case",
                new NumericType(22, 4),
                new Object[][]{{new BigDecimal("97.6543")}, {new BigDecimal("97.6542")}},
                new BigDecimal("97.6543")
            },
            // See: https://github.com/crate/crate/pull/17371
            new Object[]{
                "compact precision, string sorting different",
                new NumericType(6, 2),
                new Object[][]{{new BigDecimal("10.00")}, {new BigDecimal("9.00")}},
                new BigDecimal("10.00")
            },
            // See: https://github.com/crate/crate/pull/17371
            new Object[]{
                "large precision, string sorting different",
                new NumericType(22, 2),
                new Object[][]{{new BigDecimal("10.00")}, {new BigDecimal("9.00")}},
                new BigDecimal("10.00")
            },

            new Object[]{
                "compact precision, single input",
                new NumericType(6, 4),
                new Object[][]{{new BigDecimal("97.6542")}},
                new BigDecimal("97.6542")
            },
            new Object[]{
                "large precision, single input",
                new NumericType(22, 4),
                new Object[][]{{new BigDecimal("97.6542")}},
                new BigDecimal("97.6542")
            },
            new Object[]{
                "empty input",
                new NumericType(22, 2),
                new Object[][]{},
                null
            },
            new Object[]{
                "equal values",
                new NumericType(22, 2),
                new Object[][]{{new BigDecimal("10.00")}, {new BigDecimal("10.00")}},
                new BigDecimal("10.00")
            }
        );
    }

    @Test
    public void testHasDocValueAggregator() {
        assertHasDocValueAggregator(NumericMaxAggregation.NAME, List.of(type));
    }

    @Test
    public void testNumeric() throws Exception {
        Object result = executeAggregation(type, data);
        assertThat(result).isEqualTo(expected);
    }

    protected Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            Signature.builder(NumericMaxAggregation.NAME, FunctionType.AGGREGATE)
                .argumentTypes(argumentType.getTypeSignature())
                .returnType(argumentType.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            data,
            List.of()
        );
    }
}
