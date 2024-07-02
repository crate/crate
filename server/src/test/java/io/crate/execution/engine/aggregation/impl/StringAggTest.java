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

import io.crate.expression.symbol.Literal;
import io.crate.metadata.functions.BoundSignature;
import io.crate.operation.aggregation.AggregationTestCase;

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
        var state1 = stringAgg.newState(RAM_ACCOUNTING, Version.CURRENT, Version.CURRENT, memoryManager);
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state1, Literal.of("a"), Literal.of(","));
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state1, Literal.of("b"), Literal.of(";"));

        var state2 = stringAgg.newState(RAM_ACCOUNTING, Version.CURRENT, Version.CURRENT, memoryManager);
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state2, Literal.of("c"), Literal.of(","));
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state2, Literal.of("d"), Literal.of(";"));

        var mergedState = stringAgg.reduce(RAM_ACCOUNTING, state1, state2);
        var result = stringAgg.terminatePartial(RAM_ACCOUNTING, mergedState);

        assertThat(result).isEqualTo("a;b,c;d");
    }
}
