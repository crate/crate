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

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.symbol.Function;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;
import static io.crate.testing.SymbolMatchers.isFetchStub;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.is;

public class FetchRewriteTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_fetch_rewrite_on_eval_removes_eval_and_extends_replaced_outputs() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int)")
            .build();

        DocTableInfo tableInfo = e.resolveTableInfo("tbl");
        var x = e.asSymbol("x");
        var relation = new DocTableRelation(tableInfo);
        var collect = new Collect(false, relation, List.of(x), WhereClause.MATCH_ALL, 1L, DataTypes.INTEGER.fixedSize());
        var eval = new Eval(collect, List.of(Function.of("add", List.of(x, x), DataTypes.INTEGER)));

        FetchRewrite fetchRewrite = eval.rewriteToFetch(List.of());
        assertThat(fetchRewrite, Matchers.notNullValue());
        assertThat(fetchRewrite.newPlan(), isPlan("Collect[doc.tbl | [_fetchid] | true]"));
        assertThat(
            fetchRewrite.replacedOutputs(),
            Matchers.hasEntry(
                isFunction("add", isReference("x"), isReference("x")),
                isFunction("add", isFetchStub("x"), isFetchStub("x"))
            )
        );
        assertThat(List.copyOf(fetchRewrite.replacedOutputs().keySet()), is(eval.outputs()));
    }
}
