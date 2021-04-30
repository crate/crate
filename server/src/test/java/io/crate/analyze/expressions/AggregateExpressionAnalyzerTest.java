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

package io.crate.analyze.expressions;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.instanceOf;

public class AggregateExpressionAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int)")
            .build();
    }

    @Test
    public void test_aggregate_function_with_filter_expression_that_contains_fields() {
        var symbol = e.asSymbol("count(*) filter (where t.x > 1)");
        assertThat(symbol, instanceOf(Function.class));

        var function = (Function) symbol;
        assertThat(function.filter(), isFunction("op_>", isReference("x"), isLiteral(1)));
    }

    @Test
    public void test_distinct_aggregate_function_with_filter_expression() {
        var symbol = e.asSymbol("avg(distinct t.x) filter (where t.x < 1)");
        assertThat(symbol, instanceOf(Function.class));

        var outerFunc = (Function) symbol;
        assertThat(outerFunc.arguments().size(), is(1));
        var innerFunc = (Function) outerFunc.arguments().get(0);
        assertThat(innerFunc.filter(), isFunction("op_<", isReference("x"), isLiteral(1)));
    }

    @Test
    public void test_filter_expression_is_normalized_if_possible() {
        var symbol = e.asSymbol("count(*) filter (where 1 = 1)");
        assertThat(symbol, instanceOf(Function.class));

        var function = (Function) symbol;
        assertThat(function.filter(), isLiteral(true));
    }

    @Test
    public void test_aggregate_function_with_filter_expression_that_contains_subquery() {
        var symbol = e.asSymbol("count(*) filter (where 1 in (select x from t))");
        assertThat(symbol, instanceOf(Function.class));

        var function = (Function) symbol;
        assertThat(
            function.filter(),
            isFunction("any_=", isLiteral(1), instanceOf(SelectSymbol.class)));
    }

    @Test
    public void test_filter_expression_cannot_be_used_with_scalar_function() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Only aggregate functions allow a FILTER clause");
        e.asSymbol("ln(t.x) filter (where true)");
    }

    @Test
    public void test_filter_expression_cannot_be_used_with_table_function() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Only aggregate functions allow a FILTER clause");
        e.asSymbol("generate_series(1, 2) filter (where true)");
    }
}
