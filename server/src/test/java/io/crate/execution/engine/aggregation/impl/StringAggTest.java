/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.aggregation.impl;

import io.crate.expression.symbol.Literal;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class StringAggTest extends AggregationTest {

    @Test
    public void testAllValuesAreNull() throws Exception {
        var result = executeAggregation("string_agg", DataTypes.STRING, new Object[][] {
            new Object[] { null, null },
            new Object[] { null, null },
            new Object[] { null, null },
        }, List.of(DataTypes.STRING, DataTypes.STRING));
        assertThat(result, Matchers.nullValue());
    }

    @Test
    public void testOneDelimiterIsNull() throws Exception {
        var result = executeAggregation("string_agg", DataTypes.STRING, new Object[][] {
            new Object[] { "a", "," },
            new Object[] { "b", null },
            new Object[] { "c", "," },
        }, List.of(DataTypes.STRING, DataTypes.STRING));
        assertThat(result, is("ab,c"));
    }

    @Test
    public void testOneExpressionIsNull() throws Exception {
        var result = executeAggregation("string_agg", DataTypes.STRING, new Object[][] {
            new Object[] { "a", ";" },
            new Object[] { null, "," },
            new Object[] { "c", "," },
        }, List.of(DataTypes.STRING, DataTypes.STRING));
        assertThat(result, is("a,c"));
    }

    @Test
    public void testMergeOf2States() throws Exception {
        var stringAgg = new StringAgg(StringAgg.SIGNATURE);
        var state1 = stringAgg.newState(RAM_ACCOUNTING, Version.CURRENT, Version.CURRENT, memoryManager);
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state1, Literal.of("a"), Literal.of(","));
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state1, Literal.of("b"), Literal.of(";"));

        var state2 = stringAgg.newState(RAM_ACCOUNTING, Version.CURRENT, Version.CURRENT, memoryManager);
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state2, Literal.of("c"), Literal.of(","));
        stringAgg.iterate(RAM_ACCOUNTING, memoryManager, state2, Literal.of("d"), Literal.of(";"));

        var mergedState = stringAgg.reduce(RAM_ACCOUNTING, state1, state2);
        var result = stringAgg.terminatePartial(RAM_ACCOUNTING, mergedState);

        assertThat(result, is("a;b,c;d"));
    }
}
