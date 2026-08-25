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

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.testing.PlainRamAccounting;
import io.crate.types.DataTypes;

public class StringAggTest extends AggregationTestCase {

    @Test
    public void testAllValuesAreNull() throws Exception {
        var result = executeAggregation(
            StringAgg.SIGNATURE, new Object[][]{
                new Object[]{null, null},
                new Object[]{null, null},
                new Object[]{null, null},
            },
            List.of()
        );
        assertThat(result).isNull();
    }

    @Test
    public void testOneDelimiterIsNull() throws Exception {
        var result = executeAggregation(
            StringAgg.SIGNATURE, new Object[][]{
                new Object[]{"a", ","},
                new Object[]{"b", null},
                new Object[]{"c", ","},
            },
            List.of());
        assertThat(result).isEqualTo("ab,c");
    }

    @Test
    public void testOneExpressionIsNull() throws Exception {
        var result = executeAggregation(
            StringAgg.SIGNATURE, new Object[][]{
                new Object[]{"a", ";"},
                new Object[]{null, ","},
                new Object[]{"c", ","},
            },
            List.of());
        assertThat(result).isEqualTo("a,c");
    }

    @Test
    public void testMergeOf2States() throws Exception {
        var stringAgg = new StringAgg(
            StringAgg.SIGNATURE,
            BoundSignature.sameAsUnbound(StringAgg.SIGNATURE));
        var state1 = stringAgg.newState(RAM_ACCOUNTING, Version.CURRENT, memoryManager);
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state1, Literal.of("a"), Literal.of(","));
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state1, Literal.of("b"), Literal.of(";"));

        var state2 = stringAgg.newState(RAM_ACCOUNTING, Version.CURRENT, memoryManager);
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state2, Literal.of("c"), Literal.of(","));
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state2, Literal.of("d"), Literal.of(";"));

        var mergedState = stringAgg.reduce(RAM_ACCOUNTING, state1, state2);
        var result = stringAgg.terminatePartial(RAM_ACCOUNTING, mergedState);

        assertThat(result).isEqualTo("a;b,c;d");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_array_agg_accounts_memory_for_state() throws Exception {
        var impl = (AggregationFunction<Object, ?>) nodeCtx.functions().getQualified(
            Signature.builder(StringAgg.NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            List.of(DataTypes.STRING),
            DataTypes.STRING
        );
        RamAccounting ramAccounting = new PlainRamAccounting();
        Object state = impl.newState(ramAccounting, Version.CURRENT, memoryManager);
        assertThat(ramAccounting.totalBytes()).isEqualTo(24L);
        impl.iterate(ramAccounting, memoryManager, state, Literal.of("trillian"), Literal.of("delim"));
        impl.iterate(ramAccounting, memoryManager, state, Literal.of("arthur"), Literal.of("delimiter"));
        impl.iterate(ramAccounting, memoryManager, state, Literal.of("john"), Literal.NULL);
        assertThat(ramAccounting.totalBytes()).isEqualTo(408L);
        impl.removeFromAggregatedState(ramAccounting, state,
            new Input[] {Literal.of("trillian"), Literal.of("delim")});
        assertThat(ramAccounting.totalBytes()).isEqualTo(224L);
        impl.terminatePartial(ramAccounting, state);
        assertThat(ramAccounting.totalBytes()).isEqualTo(224L);
    }
}
