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

package io.crate.analyze;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.sql.tree.FrameBound;
import io.crate.sql.tree.WindowFrame;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SymbolMatchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class SelectWindowFunctionAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int)")
            .build();
    }

    @Test
    public void testEmptyOverClause() {
        QueriedTable analysis = e.analyze("select avg(x) OVER () from t");

        List<Symbol> outputSymbols = analysis.querySpec().outputs();
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(WindowFunction.class));
        WindowFunction windowFunction = (WindowFunction) outputSymbols.get(0);
        assertThat(windowFunction.arguments().size(), is(1));
        WindowDefinition windowDefinition = windowFunction.windowDefinition();
        assertThat(windowDefinition.partitions().isEmpty(), is(true));
        assertThat(windowDefinition.orderBy(), is(nullValue()));
        assertThat(windowDefinition.windowFrameDefinition(), is(nullValue()));
    }

    @Test
    public void testOverWithPartitionByClause() {
        QueriedTable analysis = e.analyze("select avg(x) OVER (PARTITION BY x) from t");

        List<Symbol> outputSymbols = analysis.querySpec().outputs();
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(WindowFunction.class));
        WindowFunction windowFunction = (WindowFunction) outputSymbols.get(0);
        assertThat(windowFunction.arguments().size(), is(1));
        WindowDefinition windowDefinition = windowFunction.windowDefinition();
        assertThat(windowDefinition.partitions().size(), is(1));
    }

    @Test
    public void testInvalidPartitionByField() {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column zzz unknown");
        e.analyze("select avg(x) OVER (PARTITION BY zzz) from t");
    }

    @Test
    public void testInvalidOrderByField() {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column zzz unknown");
        e.analyze("select avg(x) OVER (ORDER BY zzz) from t");
    }

    @Test
    public void testOnlyAggregatesAndWindowFunctionsAreAllowedWithOver() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("OVER clause was specified, but abs is neither a window nor an aggregate function.");
        e.analyze("select abs(x) OVER() from t");
    }

    @Test
    public void testOverWithOrderByClause() {
        QueriedTable analysis = e.analyze("select avg(x) OVER (ORDER BY x) from t");

        List<Symbol> outputSymbols = analysis.querySpec().outputs();
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(WindowFunction.class));
        WindowFunction windowFunction = (WindowFunction) outputSymbols.get(0);
        assertThat(windowFunction.arguments().size(), is(1));
        WindowDefinition windowDefinition = windowFunction.windowDefinition();
        assertThat(windowDefinition.orderBy().orderBySymbols().size(), is(1));
    }

    @Test
    public void testOverWithPartitionAndOrderByClauses() {
        QueriedTable analysis = e.analyze("select avg(x) OVER (PARTITION BY x ORDER BY x) from t");

        List<Symbol> outputSymbols = analysis.querySpec().outputs();
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(WindowFunction.class));
        WindowFunction windowFunction = (WindowFunction) outputSymbols.get(0);
        assertThat(windowFunction.arguments().size(), is(1));
        WindowDefinition windowDefinition = windowFunction.windowDefinition();
        assertThat(windowDefinition.partitions().size(), is(1));
        assertThat(windowDefinition.orderBy().orderBySymbols().size(), is(1));
    }

    @Test
    public void testOverWithFrameDefinition() {
        QueriedTable analysis = e.analyze("select avg(x) OVER (PARTITION BY x ORDER BY x " +
                                          "RANGE BETWEEN 5 PRECEDING AND 6 FOLLOWING) from t");

        List<Symbol> outputSymbols = analysis.querySpec().outputs();
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(WindowFunction.class));
        WindowFunction windowFunction = (WindowFunction) outputSymbols.get(0);
        assertThat(windowFunction.arguments().size(), is(1));
        WindowFrameDefinition frameDefinition = windowFunction.windowDefinition().windowFrameDefinition();
        assertThat(frameDefinition.type(), is(WindowFrame.Type.RANGE));
        validateRangeFrameDefinition(frameDefinition.start(), FrameBound.Type.PRECEDING, 5L);
        validateRangeFrameDefinition(frameDefinition.end(),FrameBound.Type.FOLLOWING, 6L);
    }

    private void validateRangeFrameDefinition(FrameBoundDefinition boundDefinition, FrameBound.Type type, long boundValue) {
        assertThat(boundDefinition.type(), is(type));
        assertThat(boundDefinition.value(), SymbolMatchers.isLiteral(boundValue));
    }
}
