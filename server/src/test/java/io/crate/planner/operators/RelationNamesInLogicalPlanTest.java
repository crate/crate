/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.elasticsearch.common.Randomness;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.where.DocKeys;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.JoinType;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class RelationNamesInLogicalPlanTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private DocTableRelation t1Relation;
    private DocTableRelation t2Relation;
    private RelationName t1RenamedRelationName;
    private RelationName t2RenamedRelationName;
    private Rename t1Rename;
    private Rename t2Rename;

    @Before
    public void setup() throws Exception {
        e = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(T3.T3_DEFINITION)
            .build();

        DocTableInfo t1 = e.resolveTableInfo("t1");
        Reference x = (Reference) e.asSymbol("x");
        t1Relation = new DocTableRelation(t1);
        t1RenamedRelationName = new RelationName("doc", "t1_renamed");
        var t1Alias = new AliasedAnalyzedRelation(t1Relation, t1RenamedRelationName);
        var t1Collect = new Collect(t1Relation, List.of(x), WhereClause.MATCH_ALL);
        Symbol t1Output = t1Alias.getField(x.column(), Operation.READ, true);
        t1Rename = new Rename(List.of(t1Output), t1Alias.relationName(), t1Alias, t1Collect);

        DocTableInfo t2 = e.resolveTableInfo("t2");
        Reference y = (Reference) e.asSymbol("y");
        t2Relation = new DocTableRelation(t2);
        t2RenamedRelationName = new RelationName("doc", "t2_renamed");
        var t2Alias = new AliasedAnalyzedRelation(t2Relation, t2RenamedRelationName);
        var t2Collect = new Collect(t2Relation, List.of(x), WhereClause.MATCH_ALL);
        Symbol t2Output = t2Alias.getField(y.column(), Operation.READ, true);
        t2Rename = new Rename(List.of(t2Output), t2Alias.relationName(), t2Alias, t2Collect);
    }

    @Test
    public void test_relationnames_are_based_on_sources_in_hashjoin() throws Exception {
        var hashJoin = new HashJoin(t1Rename, t2Rename, e.asSymbol("x = y"));
        assertThat(hashJoin.baseTables(), containsInAnyOrder(t1Relation, t2Relation));
        assertThat(hashJoin.getRelationNames(), containsInAnyOrder(t1RenamedRelationName, t2RenamedRelationName));
    }

    @Test
    public void test_relationnames_are_based_on_sources_in_nestedloopjoin() {
        var nestedLoopJoin = new NestedLoopJoin(t1Rename,
            t2Rename,
            JoinType.INNER,
            e.asSymbol("x = y"),
            false,
            false,
            false);
        assertThat(nestedLoopJoin.baseTables(), containsInAnyOrder(t1Relation, t2Relation));
        assertThat(nestedLoopJoin.getRelationNames(), containsInAnyOrder(t1RenamedRelationName, t2RenamedRelationName));
    }

    @Test
    public void test_relationnames_are_based_on_sources_in_union() {
        var union = new Union(t1Rename, t2Rename, List.of());
        assertThat(union.baseTables(), containsInAnyOrder(t1Relation, t2Relation));
        assertThat(union.getRelationNames(), containsInAnyOrder(t1RenamedRelationName, t2RenamedRelationName));
    }

    @Test
    public void test_relationnames_are_based_on_sources_in_table_function() {
        QueriedSelectRelation relation = e.analyze("select * from abs(1)");
        TableFunctionRelation tableFunctionRelation = (TableFunctionRelation) relation.from().get(0);
        var tableFunction = new TableFunction(tableFunctionRelation, List.of(), new WhereClause(null));
        assertThat(tableFunction.baseTables(), containsInAnyOrder());
        assertThat(tableFunction.getRelationNames(), containsInAnyOrder(new RelationName(null, "abs")));
    }

    @Test
    public void test_relationnames_are_based_on_sources_in_collect() {
        var collect = new Collect(t1Relation, List.of(), new WhereClause(null));
        assertThat(collect.baseTables(), containsInAnyOrder(t1Relation));
        assertThat(collect.getRelationNames(), containsInAnyOrder(t1Relation.relationName()));
    }

    @Test
    public void test_relationnames_are_based_on_sources_in_get() {
        var get = new Get(t1Relation, new DocKeys(List.of(List.of()), false, false, 1, null), null, List.of(), false);
        assertThat(get.baseTables(), containsInAnyOrder(t1Relation));
        assertThat(get.getRelationNames(), containsInAnyOrder(t1Relation.relationName()));
    }

    @Test
    public void test_relationnames_are_based_on_sources_in_count() {
        QueriedSelectRelation relation = e.analyze("select count(1)");
        Function function = (Function) relation.outputs().get(0);
        var count = new Count(function, t1Relation, new WhereClause(null));
        assertThat(count.baseTables(), containsInAnyOrder(t1Relation));
        assertThat(count.getRelationNames(), containsInAnyOrder(t1Relation.relationName()));
    }

}
