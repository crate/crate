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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataTypes;

public class CmpByAggregationTest extends AggregationTestCase {

    @Test
    public void test_max_by() throws Exception {
        Signature signature = Signature.builder("max_by", FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                        DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build();
        Object result = executeAggregation(
            signature,
            new Object[][] {
                new Object[] { "foo", 10 },
                new Object[] { "bar", 15 },
                new Object[] { "baz", 12 },
            },
            List.of()
        );
        assertThat(result).isEqualTo("bar");
    }

    @Test
    public void test_cmp_by_returns_null_if_all_search_fields_are_null() throws Exception {
        for (String func : List.of("max_by", "min_by")) {
            Signature signature = Signature.builder(func, FunctionType.AGGREGATE)
                    .argumentTypes(DataTypes.STRING.getTypeSignature(),
                            DataTypes.INTEGER.getTypeSignature())
                    .returnType(DataTypes.STRING.getTypeSignature())
                    .features(Scalar.Feature.DETERMINISTIC)
                    .build();
            Object result = executeAggregation(
                signature,
                new Object[][] {
                    new Object[] { "foo", null },
                    new Object[] { "bar", null },
                    new Object[] { "baz", null },
                },
                List.of()
            );
            assertThat(result).isNull();
        }
    }

    @Test
    public void test_cmp_by_returns_null_if_all_return_fields_are_null() throws Exception {
        for (String func : List.of("max_by", "min_by")) {
            Signature signature = Signature.builder(func, FunctionType.AGGREGATE)
                    .argumentTypes(DataTypes.STRING.getTypeSignature(),
                            DataTypes.INTEGER.getTypeSignature())
                    .returnType(DataTypes.STRING.getTypeSignature())
                    .features(Scalar.Feature.DETERMINISTIC)
                    .build();
            Object result = executeAggregation(
                signature,
                new Object[][] {
                    new Object[] { null, 1 },
                    new Object[] { null, 2 },
                    new Object[] { null, 3 },
                },
                List.of()
            );
            assertThat(result).isNull();
        }
    }

    @Test
    public void test_max_by_returns_null_if_return_field_of_max_search_field_is_null() throws Exception {
        Signature signature = Signature.builder("max_by", FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                        DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build();
        Object result = executeAggregation(
            signature,
            new Object[][] {
                new Object[] { "a", 1 },
                new Object[] { "b", 2 },
                new Object[] { null, 3 },
            },
            List.of()
        );
        assertThat(result).isNull();
    }

    @Test
    public void test_cmp_by_result_is_nondeterministic_on_ties() throws Exception {
        for (String func : List.of("max_by", "min_by")) {
            Signature signature = Signature.builder(func, FunctionType.AGGREGATE)
                    .argumentTypes(DataTypes.STRING.getTypeSignature(),
                            DataTypes.INTEGER.getTypeSignature())
                    .returnType(DataTypes.STRING.getTypeSignature())
                    .features(Scalar.Feature.DETERMINISTIC)
                    .build();
            Object result = executeAggregation(
                signature,
                new Object[][] {
                    new Object[] { "a", 2 },
                    new Object[] { "b", 2 },
                    new Object[] { "c", 2 },
                },
                List.of()
            );
            assertThat(result).isIn("a", "b", "c");
        }
    }

    @Test
    public void test_min_by() throws Exception {
        Signature signature = Signature.builder("min_by", FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                        DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build();
        Object result = executeAggregation(
            signature,
            new Object[][] {
                new Object[] { "foo", 10 },
                new Object[] { "bar", 15 },
                new Object[] { null, 15 },
                new Object[] { "baz", null },
            },
            List.of()
        );
        assertThat(result).isEqualTo("foo");
    }

    @Test
    public void test_cannot_use_max_by_on_non_comparable_types() throws Exception {
        Signature signature = Signature.builder("max_by", FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                        DataTypes.UNTYPED_OBJECT.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build();
        assertThatThrownBy(() -> executeAggregation(
            signature,
            new Object[][] {
                new Object[] { "foo", Map.of("x", 10) },
            },
            List.of()
        )).isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use `max_by` on values of type object");
    }


    @Test
    public void test_min_by_does_not_overflow_on_MIN_VALUE() throws Exception {
        Signature signature = Signature.builder("min_by", FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                        DataTypes.LONG.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build();
        Object result = executeAggregation(
            signature,
            new Object[][] {
                new Object[] { "a", 1L },
                new Object[] { "b", 2L },
                new Object[] { "c", Long.MIN_VALUE},
            },
            List.of()
        );
        assertThat(result).isEqualTo("c");
    }
}
