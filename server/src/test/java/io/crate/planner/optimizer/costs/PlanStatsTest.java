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

package io.crate.planner.optimizer.costs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.operators.Union;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.iterative.Memo;
import io.crate.sql.tree.JoinType;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class PlanStatsTest extends CrateDummyClusterServiceUnitTest {

    public static final PlanStats PLAN_STATS_EMPTY = new PlanStats(new TableStats());

    @Test
    public void test_collect() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .build();

        DocTableInfo a = e.resolveTableInfo("a");

        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a),
                                 List.of(x),
                                 WhereClause.MATCH_ALL,
                                 1L,
                                 DataTypes.INTEGER.fixedSize());

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(Map.of(a.ident(), new Stats(1, DataTypes.INTEGER.fixedSize(), Map.of())));

        var memo = new Memo(source);
        PlanStats planStats = new PlanStats(tableStats, memo);
        var result = planStats.get(source);
        assertThat(result.numDocs()).isEqualTo(1L);
        assertThat(result.sizeInBytes()).isEqualTo(DataTypes.INTEGER.fixedSize());
    }

    @Test
    public void test_group_reference() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .build();

        DocTableInfo a = e.resolveTableInfo("a");

        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a),
                                 List.of(x),
                                 WhereClause.MATCH_ALL,
                                 1L,
                                 DataTypes.INTEGER.fixedSize());
        var groupReference = new GroupReference(1, source.outputs(), Set.of());

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(Map.of(a.ident(), new Stats(1, DataTypes.INTEGER.fixedSize(), Map.of())));

        var memo = new Memo(source);
        PlanStats planStats = new PlanStats(tableStats, memo);
        var result = planStats.get(groupReference);
        assertThat(result.numDocs()).isEqualTo(1L);
        assertThat(result.sizeInBytes()).isEqualTo(DataTypes.INTEGER.fixedSize());
    }

    @Test
    public void test_limit() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .build();

        DocTableInfo a = e.resolveTableInfo("a");

        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a),
                                 List.of(x),
                                 WhereClause.MATCH_ALL,
                                 10L,
                                 DataTypes.INTEGER.fixedSize());

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(Map.of(a.ident(), new Stats(10L, 1, Map.of())));

        var limit = new Limit(source, Literal.of(5), Literal.of(0));

        var memo = new Memo(limit);
        PlanStats planStats = new PlanStats(tableStats, memo);
        var result = planStats.get(limit);
        assertThat(result.numDocs()).isEqualTo(5L);
    }

    @Test
    public void test_union() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (y int)")
            .build();

        DocTableInfo aDoc = e.resolveTableInfo("a");
        DocTableInfo bDoc = e.resolveTableInfo("b");

        var x = e.asSymbol("x");
        var y = e.asSymbol("x");

        var lhs = new Collect(new DocTableRelation(aDoc),
                                 List.of(x),
                                 WhereClause.MATCH_ALL,
                                 9L,
                                 DataTypes.INTEGER.fixedSize());

        var rhs = new Collect(new DocTableRelation(bDoc),
                                 List.of(y),
                                 WhereClause.MATCH_ALL,
                                 1L,
                                 DataTypes.INTEGER.fixedSize());

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(
            Map.of(
                aDoc.ident(), new Stats(9L, 1, Map.of()),
                bDoc.ident(), new Stats(1L, 1, Map.of())
            )
        );

        var union = new Union(lhs, rhs, List.of());

        var memo = new Memo(union);
        PlanStats planStats = new PlanStats(tableStats, memo);
        var result = planStats.get(union);
        assertThat(result.numDocs()).isEqualTo(10L);
        assertThat(result.sizeInBytes()).isEqualTo(1L);
    }

    @Test
    public void test_hash_join() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (y int)")
            .build();

        DocTableInfo aDoc = e.resolveTableInfo("a");
        DocTableInfo bDoc = e.resolveTableInfo("b");

        var x = e.asSymbol("x");
        var y = e.asSymbol("x");

        var lhs = new Collect(new DocTableRelation(aDoc),
                              List.of(x),
                              WhereClause.MATCH_ALL,
                              9L,
                              DataTypes.INTEGER.fixedSize());

        var rhs = new Collect(new DocTableRelation(bDoc),
                              List.of(y),
                              WhereClause.MATCH_ALL,
                              1L,
                              DataTypes.INTEGER.fixedSize());

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(
            Map.of(
                aDoc.ident(), new Stats(9L, 1, Map.of()),
                bDoc.ident(), new Stats(1L, 1, Map.of())
            )
        );

        var hashjoin = new HashJoin(lhs, rhs, x, false);

        var memo = new Memo(hashjoin);
        PlanStats planStats = new PlanStats(tableStats, memo);
        var result = planStats.get(hashjoin);
        // lhs is the larger table which 9 entries, so the join will at max emit 9 entries
        assertThat(result.numDocs()).isEqualTo(9L);
        assertThat(result.sizeInBytes()).isEqualTo(1L);
    }

    @Test
    public void test_nl_join() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (y int)")
            .build();

        DocTableInfo aDoc = e.resolveTableInfo("a");
        DocTableInfo bDoc = e.resolveTableInfo("b");

        var x = e.asSymbol("x");
        var y = e.asSymbol("x");

        DocTableRelation relation = new DocTableRelation(aDoc);
        var lhs = new Collect(relation,
                              List.of(x),
                              WhereClause.MATCH_ALL,
                              9L,
                              DataTypes.INTEGER.fixedSize());

        var rhs = new Collect(new DocTableRelation(bDoc),
                              List.of(y),
                              WhereClause.MATCH_ALL,
                              2L,
                              DataTypes.INTEGER.fixedSize());

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(
            Map.of(
                aDoc.ident(), new Stats(9L, DataTypes.INTEGER.fixedSize(), Map.of()),
                bDoc.ident(), new Stats(2L, DataTypes.INTEGER.fixedSize(), Map.of())
            )
        );

        var nestedLoopJoin = new NestedLoopJoin(lhs, rhs, JoinType.INNER, x, false, relation, false,false, false, false);

        var memo = new Memo(nestedLoopJoin);
        PlanStats planStats = new PlanStats(tableStats, memo);
        var result = planStats.get(nestedLoopJoin);
        // lhs is the larger table which 9 entries, so the join will at max emit 9 entries
        assertThat(result.numDocs()).isEqualTo(9L);
        assertThat(result.sizeInBytes()).isEqualTo(9L);

        nestedLoopJoin = new NestedLoopJoin(lhs, rhs, JoinType.CROSS, x, false, relation, false,false, false, false);

        memo = new Memo(nestedLoopJoin);
        planStats = new PlanStats(tableStats, memo);
        result = planStats.get(nestedLoopJoin);
        assertThat(result.numDocs()).isEqualTo(18L);
        assertThat(result.sizeInBytes()).isEqualTo(9L);
    }

}
