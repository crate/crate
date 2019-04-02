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

package io.crate.planner.operators;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isField;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class WindowAggTest {

    private SqlExpressions expressions;

    @Before
    public void setUp() {
        expressions = new SqlExpressions(T3.SOURCES);
    }

    @Test
    public void testNoOrderByIfNoPartitionsAndNoOrderBy() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER ()"));
        assertThat(orderBy, Matchers.nullValue());
    }

    @Test
    public void testOrderByIsOverOrderByWithoutPartitions() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (ORDER BY x)"));
        assertThat(orderBy, notNullValue());
        assertThat(orderBy.orderBySymbols(), contains(isField("x")));
    }

    @Test
    public void testOrderByIsPartitionByWithoutExplicitOrderBy() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (PARTITION BY x)"));
        assertThat(orderBy, notNullValue());
        assertThat(orderBy.orderBySymbols(), contains(isField("x")));
    }

    @Test
    public void testOrderByIsMergedWithPartitionByWithFullColumnOverlap() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (PARTITION BY x ORDER BY x)"));
        assertThat(orderBy, notNullValue());
        assertThat(orderBy.orderBySymbols(), contains(isField("x")));
    }

    @Test
    public void testOrderByIsMergedWithPartitionByWithPartialColumnOverlap() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (PARTITION BY x, y ORDER BY x)"));
        assertThat(orderBy, notNullValue());
        assertThat(orderBy.orderBySymbols(), contains(isField("x"), isField("y")));
    }

    @Test
    public void testOrderByIsMergedWithPartitionByWithPartialColumnOverlapButReverseOrder() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (PARTITION BY y, x ORDER BY x)"));
        assertThat(orderBy, notNullValue());
        assertThat(orderBy.orderBySymbols(), contains(isField("y"), isField("x")));
    }

    @Test
    public void testOrderByIsMergedWithPartitionByWithNoOverlap() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (PARTITION BY y ORDER BY x)"));
        assertThat(orderBy, notNullValue());
        assertThat(orderBy.orderBySymbols(), contains(isField("y"), isField("x")));
    }

    private WindowDefinition wd(String expression) {
        Symbol symbol = expressions.asSymbol(expression);
        assertThat(symbol, instanceOf(WindowFunction.class));
        return ((WindowFunction) symbol).windowDefinition();
    }
}
