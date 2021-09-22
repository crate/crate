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

package io.crate.planner.operators;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.FetchStub;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.table.Operation;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;
import static io.crate.testing.SymbolMatchers.isAlias;
import static io.crate.testing.SymbolMatchers.isFetchMarker;
import static io.crate.testing.SymbolMatchers.isFetchStub;
import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.contains;
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
        var collect = new Collect(relation, List.of(x), WhereClause.MATCH_ALL, 1L, DataTypes.INTEGER.fixedSize());
        var eval = new Eval(
            collect,
            List.of(
                new Function(
                    Signature.scalar(
                        "add",
                        DataTypes.INTEGER.getTypeSignature(),
                        DataTypes.INTEGER.getTypeSignature(),
                        DataTypes.INTEGER.getTypeSignature()
                    ),
                    List.of(x, x),
                    DataTypes.INTEGER
                )
            )
        );

        FetchRewrite fetchRewrite = eval.rewriteToFetch(new TableStats(), List.of());
        assertThat(fetchRewrite, Matchers.notNullValue());
        assertThat(fetchRewrite.newPlan(), isPlan("Collect[doc.tbl | [_fetchid] | true]"));
        assertThat(
            fetchRewrite.replacedOutputs(),
            Matchers.hasEntry(
                isFunction("add", isReference("x"), isReference("x")),
                isFunction("add", isFetchStub("_doc['x']"), isFetchStub("_doc['x']"))
            )
        );
        assertThat(List.copyOf(fetchRewrite.replacedOutputs().keySet()), is(eval.outputs()));
    }

    @Test
    public void test_fetchrewrite_on_rename_puts_fetch_marker_into_alias_scope() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int)")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("tbl");
        Reference x = (Reference) e.asSymbol("x");
        var relation = new DocTableRelation(tableInfo);
        var alias = new AliasedAnalyzedRelation(relation, new RelationName(null, "t1"));
        var collect = new Collect(relation, List.of(x), WhereClause.MATCH_ALL, 1L, DataTypes.INTEGER.fixedSize());
        Symbol t1X = alias.getField(x.column(), Operation.READ, true);
        assertThat(t1X, Matchers.notNullValue());
        var rename = new Rename(List.of(t1X), alias.relationName(), alias, collect);

        FetchRewrite fetchRewrite = rename.rewriteToFetch(new TableStats(), List.of());
        assertThat(fetchRewrite, Matchers.notNullValue());
        LogicalPlan newRename = fetchRewrite.newPlan();
        assertThat(newRename, isPlan(
            "Rename[t1._fetchid] AS t1\n" +
            "  └ Collect[doc.tbl | [_fetchid] | true]"));
        assertThat(
            "fetchRewrite replacedOutputs.keySet() must always match the outputs of the operator prior to the rewrite",
            List.copyOf(fetchRewrite.replacedOutputs().keySet()),
            is(rename.outputs())
        );
        assertThat(
            fetchRewrite.replacedOutputs(),
            Matchers.hasEntry(isField("x", alias.relationName()), isFetchStub("_doc['x']"))
        );
        assertThat(newRename.outputs(), contains(
            isFetchMarker(alias.relationName(), contains(isReference("_doc['x']"))))
        );

        FetchStub fetchStub = (FetchStub) fetchRewrite.replacedOutputs().entrySet().iterator().next().getValue();
        assertThat(
            "FetchStub fetchMarker must be changed to the aliased marker",
            fetchStub.fetchMarker(),
            Matchers.sameInstance(newRename.outputs().get(0))
        );
    }

    @Test
    public void test_fetchrewrite_on_eval_with_nested_source_outputs() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int)")
            .build();

        DocTableInfo tableInfo = e.resolveTableInfo("tbl");
        var x = new AliasSymbol("x_alias", e.asSymbol("x"));
        var relation = new DocTableRelation(tableInfo);
        var collect = new Collect(relation, List.of(x), WhereClause.MATCH_ALL, 1L, DataTypes.INTEGER.fixedSize());
        var eval = new Eval(
            collect,
            List.of(x)
        );

        FetchRewrite fetchRewrite = eval.rewriteToFetch(new TableStats(), List.of());
        assertThat(fetchRewrite, Matchers.notNullValue());
        assertThat(fetchRewrite.newPlan(), isPlan("Collect[doc.tbl | [_fetchid] | true]"));
        assertThat(
            fetchRewrite.replacedOutputs(),
            Matchers.hasEntry(
                is(x),
                isAlias("x_alias", isFetchStub("_doc['x']"))
            )
        );
    }
}
