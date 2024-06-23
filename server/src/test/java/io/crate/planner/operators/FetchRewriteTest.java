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

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isAlias;
import static io.crate.testing.Asserts.isFetchMarker;
import static io.crate.testing.Asserts.isFetchStub;
import static io.crate.testing.Asserts.isField;
import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isReference;
import static io.crate.testing.Asserts.toCondition;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.assertj.core.api.Condition;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.FetchStub;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Scalar;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.table.Operation;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class FetchRewriteTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_fetch_rewrite_on_eval_removes_eval_and_extends_replaced_outputs() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");

        DocTableInfo tableInfo = e.resolveTableInfo("tbl");
        var x = e.asSymbol("x");
        var relation = new DocTableRelation(tableInfo);
        var collect = new Collect(relation, List.of(x), WhereClause.MATCH_ALL);
        var eval = new Eval(
            collect,
            List.of(
                new Function(
                    Signature.scalar(
                        "add",
                        Scalar.Feature.CONDITIONAL,
                        DataTypes.INTEGER.getTypeSignature(),
                        DataTypes.INTEGER.getTypeSignature(),
                        DataTypes.INTEGER.getTypeSignature()
                    ).withFeature(Scalar.Feature.DETERMINISTIC),
                    List.of(x, x),
                    DataTypes.INTEGER
                )
            )
        );

        FetchRewrite fetchRewrite = eval.rewriteToFetch(List.of());
        assertThat(fetchRewrite).isNotNull();
        assertThat(fetchRewrite.newPlan()).isEqualTo("Collect[doc.tbl | [_fetchid] | true]");
        assertThat(fetchRewrite.replacedOutputs()).hasEntrySatisfying(
                toCondition(isFunction("add", isReference("x"), isReference("x"))),
                toCondition(isFunction("add", isFetchStub("_doc['x']"), isFetchStub("_doc['x']"))));
        assertThat(List.copyOf(fetchRewrite.replacedOutputs().keySet())).isEqualTo(eval.outputs());
    }

    @Test
    public void test_fetchrewrite_on_rename_puts_fetch_marker_into_alias_scope() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");
        DocTableInfo tableInfo = e.resolveTableInfo("tbl");
        Reference x = (Reference) e.asSymbol("x");
        var relation = new DocTableRelation(tableInfo);
        var alias = new AliasedAnalyzedRelation(relation, new RelationName(null, "t1"));
        var collect = new Collect(relation, List.of(x), WhereClause.MATCH_ALL);
        Symbol t1X = alias.getField(x.column(), Operation.READ, true);
        assertThat(t1X).isNotNull();
        var rename = new Rename(List.of(t1X), alias.relationName(), alias, collect);

        FetchRewrite fetchRewrite = rename.rewriteToFetch(List.of());
        assertThat(fetchRewrite).isNotNull();
        LogicalPlan newRename = fetchRewrite.newPlan();
        assertThat(newRename).isEqualTo(
            """
            Rename[t1._fetchid] AS t1
              └ Collect[doc.tbl | [_fetchid] | true]
            """);
        assertThat(List.copyOf(fetchRewrite.replacedOutputs().keySet()))
            .as("fetchRewrite replacedOutputs.keySet() must always match the outputs of the operator prior to the rewrite")
            .isEqualTo(rename.outputs());
        assertThat(fetchRewrite.replacedOutputs()).hasEntrySatisfying(
            toCondition(isField("x", alias.relationName())),
            toCondition(isFetchStub("_doc['x']")));
        assertThat(newRename.outputs()).satisfiesExactly(
            isFetchMarker(alias.relationName(), isReference("_doc['x']")));

        FetchStub fetchStub = (FetchStub) fetchRewrite.replacedOutputs().entrySet().iterator().next().getValue();
        assertThat(fetchStub.fetchMarker())
            .as("FetchStub fetchMarker must be changed to the aliased marker")
            .isSameAs(newRename.outputs().get(0));
    }

    @Test
    public void test_fetchrewrite_on_eval_with_nested_source_outputs() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");

        DocTableInfo tableInfo = e.resolveTableInfo("tbl");
        var x = new AliasSymbol("x_alias", e.asSymbol("x"));
        var relation = new DocTableRelation(tableInfo);
        var collect = new Collect(relation, List.of(x), WhereClause.MATCH_ALL);
        var eval = new Eval(
            collect,
            List.of(x)
        );

        FetchRewrite fetchRewrite = eval.rewriteToFetch(List.of());
        assertThat(fetchRewrite).isNotNull();
        assertThat(fetchRewrite.newPlan()).isEqualTo("Collect[doc.tbl | [_fetchid] | true]");
        assertThat(fetchRewrite.replacedOutputs()).hasEntrySatisfying(
                new Condition<>(k -> k.equals(x), ""),
                toCondition(isAlias("x_alias", isFetchStub("_doc['x']"))));
    }
}
