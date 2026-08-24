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

import java.util.List;

import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.testing.PlainRamAccounting;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;


public class ArrayAggTest extends AggregationTestCase {

    @SuppressWarnings("unchecked")
    @Test
    public void test_array_agg_adds_all_items_to_array() throws Exception {
        var result = executeAggregation(
            ArrayAgg.SIGNATURE,
            List.of(DataTypes.INTEGER),
            new ArrayType<>(DataTypes.INTEGER),
            new Object[][]{
                new Object[]{20},
                new Object[]{null},
                new Object[]{42},
                new Object[]{24}
            },
            true,
            List.of()
        );
        assertThat((List<Object>) result).containsExactly(20, null, 42, 24);
    }

    @Test
    public void test_array_agg_return_type_is_array_of_argument_type() {
        DataType<?> returnType = nodeCtx.functions().getQualified(
            ArrayAgg.SIGNATURE,
            List.of(DataTypes.LONG),
            DataTypes.BIGINT_ARRAY
        ).boundSignature().returnType();
        assertThat(returnType).isEqualTo(DataTypes.BIGINT_ARRAY);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_array_agg_accounts_memory_for_state() throws Exception {
        var impl = (AggregationFunction<Object, ?>) nodeCtx.functions().getQualified(
            Signature.builder(ArrayAgg.NAME, FunctionType.AGGREGATE)
                .argumentTypes(TypeSignature.E)
                .returnType(TypeSignature.ARRAY_E)
                .features(Scalar.Feature.DETERMINISTIC)
                .typeVariableConstraints(TypeVariableConstraint.E)
                .build(),
            List.of(DataTypes.STRING),
            DataTypes.STRING_ARRAY
        );
        RamAccounting ramAccounting = new PlainRamAccounting();
        Object state = impl.newState(ramAccounting, Version.CURRENT, memoryManager);
        assertThat(ramAccounting.totalBytes()).isEqualTo(40L);
        impl.iterate(ramAccounting, memoryManager, state, Literal.of("trillian"));
        impl.iterate(ramAccounting, memoryManager, state, Literal.of("arthur"));
        assertThat(ramAccounting.totalBytes()).isEqualTo(152L);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_array_agg_for_window_functions_accounts_memory_for_state() throws Exception {
        var agg = (AggregationFunction<Object, ?>) nodeCtx.functions().getQualified(
            Signature.builder(ArrayAgg.NAME, FunctionType.AGGREGATE)
                .argumentTypes(TypeSignature.E)
                .returnType(TypeSignature.ARRAY_E)
                .features(Scalar.Feature.DETERMINISTIC)
                .typeVariableConstraints(TypeVariableConstraint.E)
                .build(),
            List.of(DataTypes.STRING),
            DataTypes.STRING_ARRAY
        );
        var impl = (AggregationFunction<Object, Object>) agg.optimizeForExecutionAsWindowFunction(Version.CURRENT);
        RamAccounting ramAccounting = new PlainRamAccounting();
        Object state = impl.newState(ramAccounting, Version.CURRENT, memoryManager);
        assertThat(ramAccounting.totalBytes()).isEqualTo(24L);
        impl.iterate(ramAccounting, memoryManager, state, Literal.of("trillian"));
        impl.iterate(ramAccounting, memoryManager, state, Literal.of("arthur"));
        impl.iterate(ramAccounting, memoryManager, state, Literal.of("john"));
        assertThat(ramAccounting.totalBytes()).isEqualTo(184L);

        impl.removeFromAggregatedState(ramAccounting, state, new Input[] { Literal.of("trillian") });
        assertThat(ramAccounting.totalBytes()).isEqualTo(128L);

        impl.terminatePartial(ramAccounting, state);
>>>>>>> 863ba87570 (Derive size for ArrayList to consider JVM settings)
        assertThat(ramAccounting.totalBytes()).isEqualTo(152L);
    }
}
