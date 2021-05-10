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

package io.crate.analyze;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.sql.tree.FrameBound;
import io.crate.sql.tree.WindowFrame;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

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
        QueriedSelectRelation analysis = e.analyze("select avg(x) OVER () from t");

        List<Symbol> outputSymbols = analysis.outputs();
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(WindowFunction.class));
        WindowFunction windowFunction = (WindowFunction) outputSymbols.get(0);
        assertThat(windowFunction.arguments().size(), is(1));
        WindowDefinition windowDefinition = windowFunction.windowDefinition();
        assertThat(windowDefinition.partitions().isEmpty(), is(true));
        assertThat(windowDefinition.orderBy(), is(nullValue()));
        assertThat(windowDefinition.windowFrameDefinition(), is(WindowDefinition.RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW));
    }

    @Test
    public void testOverWithPartitionByClause() {
        QueriedSelectRelation analysis = e.analyze("select avg(x) OVER (PARTITION BY x) from t");

        List<Symbol> outputSymbols = analysis.outputs();
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
        QueriedSelectRelation analysis = e.analyze("select avg(x) OVER (ORDER BY x) from t");

        List<Symbol> outputSymbols = analysis.outputs();
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(WindowFunction.class));
        WindowFunction windowFunction = (WindowFunction) outputSymbols.get(0);
        assertThat(windowFunction.arguments().size(), is(1));
        WindowDefinition windowDefinition = windowFunction.windowDefinition();
        assertThat(windowDefinition.orderBy().orderBySymbols().size(), is(1));
    }

    @Test
    public void testOverWithPartitionAndOrderByClauses() {
        QueriedSelectRelation analysis = e.analyze("select avg(x) OVER (PARTITION BY x ORDER BY x) from t");

        List<Symbol> outputSymbols = analysis.outputs();
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
        QueriedSelectRelation analysis = e.analyze("select avg(x) OVER (PARTITION BY x ORDER BY x " +
                                                   "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from t");

        List<Symbol> outputSymbols = analysis.outputs();
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(WindowFunction.class));
        WindowFunction windowFunction = (WindowFunction) outputSymbols.get(0);
        assertThat(windowFunction.arguments().size(), is(1));
        WindowFrameDefinition frameDefinition = windowFunction.windowDefinition().windowFrameDefinition();
        assertThat(frameDefinition.mode(), is(WindowFrame.Mode.RANGE));
        assertThat(frameDefinition.start().type(), is(FrameBound.Type.UNBOUNDED_PRECEDING));
        assertThat(frameDefinition.end().type(), is(FrameBound.Type.UNBOUNDED_FOLLOWING));
    }

    @Test
    public void test_over_with_order_by_references_window_with_partition_by() {
        AnalyzedRelation relation = e.analyze(
            "SELECT AVG(x) OVER (w ORDER BY x) " +
            "FROM t " +
            "WINDOW w AS (PARTITION BY x)");
        WindowFunction windowFunction = (WindowFunction) relation.outputs().get(0);
        WindowDefinition windowDefinition = windowFunction.windowDefinition();

        assertThat(windowDefinition.partitions(), contains(isReference("x")));

        OrderBy orderBy = windowDefinition.orderBy();
        assertThat(orderBy, not(nullValue()));
        assertThat(orderBy.orderBySymbols().size(), is(1));
    }

    @Test
    public void test_over_references_window_that_references_subsequent_window() {
        AnalyzedRelation relation = e.analyze(
            "SELECT AVG(x) OVER w2 " +
            "FROM t WINDOW w AS (PARTITION BY x)," +
            "             w2 AS (w ORDER BY x)");
        WindowFunction windowFunction = (WindowFunction) relation.outputs().get(0);
        WindowDefinition windowDefinition = windowFunction.windowDefinition();

        assertThat(windowDefinition.partitions(), contains(isReference("x")));

        OrderBy orderBy = windowDefinition.orderBy();
        assertThat(orderBy, not(nullValue()));
        assertThat(orderBy.orderBySymbols().size(), is(1));
    }

    @Test
    public void test_over_references_not_defined_window() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Window w does not exist");
        e.analyze("SELECT AVG(x) OVER w FROM t WINDOW ww AS ()");
    }

    @Test
    public void test_window_function_symbols_not_in_grouping_raises_an_error() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'x' must appear in the GROUP BY clause or be used in an aggregation function.");
        e.analyze("select y, sum(x) over(partition by x) " +
                "FROM unnest([1], [6]) as t (x, y) " +
                "group by 1");
    }
}
