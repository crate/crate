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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.operators.Union;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.iterative.Memo;
import io.crate.role.Role;
import io.crate.sql.tree.JoinType;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.MostCommonValues;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;


public class PlanStatsTest extends CrateDummyClusterServiceUnitTest {

    private CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private NodeContext nodeContext = new NodeContext(new Functions(Map.of()), () -> List.of(Role.CRATE_USER));

    @Test
    public void test_collect() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table a (x int)");

        DocTableInfo a = e.resolveTableInfo("a");

        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a),
                                 List.of(x),
                                 WhereClause.MATCH_ALL);

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(Map.of(a.ident(), new Stats(1, DataTypes.INTEGER.fixedSize(), Map.of())));

        var memo = new Memo(source);
        PlanStats planStats = new PlanStats(nodeContext, txnCtx, tableStats, memo);
        var result = planStats.get(source);
        assertThat(result.numDocs()).isEqualTo(1L);
        assertThat(result.sizeInBytes()).isEqualTo(DataTypes.INTEGER.fixedSize());
    }

    @Test
    public void test_group_reference() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table a (x int)");

        DocTableInfo a = e.resolveTableInfo("a");

        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a), List.of(x), WhereClause.MATCH_ALL);
        var groupReference = new GroupReference(1, source.outputs(), Set.of());

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(Map.of(a.ident(), new Stats(1, DataTypes.INTEGER.fixedSize(), Map.of())));

        var memo = new Memo(source);
        PlanStats planStats = new PlanStats(nodeContext, txnCtx, tableStats, memo);
        var result = planStats.get(groupReference);
        assertThat(result.numDocs()).isEqualTo(1L);
        assertThat(result.sizeInBytes()).isEqualTo(DataTypes.INTEGER.fixedSize());
    }

    @Test
    public void test_limit() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table a (x int)");

        DocTableInfo a = e.resolveTableInfo("a");

        var x = e.asSymbol("x");
        var source = new Collect(new DocTableRelation(a), List.of(x), WhereClause.MATCH_ALL);

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(Map.of(a.ident(), new Stats(10L, 1, Map.of())));

        var limit = new Limit(source, Literal.of(5), Literal.of(0));

        var memo = new Memo(limit);
        PlanStats planStats = new PlanStats(nodeContext, txnCtx, tableStats, memo);
        var result = planStats.get(limit);
        assertThat(result.numDocs()).isEqualTo(5L);
        assertThat(limit).withPlanStats(planStats).hasOperators("Limit[5;0] (rows=5)",
                                                                "  └ Collect[doc.a | [x] | true] (rows=10)");

        // now the source is smaller than the limit
        tableStats.updateTableStats(Map.of(a.ident(), new Stats(3L, 1, Map.of())));

        result = planStats.get(limit);
        assertThat(result.numDocs()).isEqualTo(3L);
        assertThat(limit).withPlanStats(planStats).hasOperators("Limit[5;0] (rows=3)",
                                                                "  └ Collect[doc.a | [x] | true] (rows=3)");
    }

    @Test
    public void test_union() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (y int)");

        DocTableInfo aDoc = e.resolveTableInfo("a");
        DocTableInfo bDoc = e.resolveTableInfo("b");

        var x = e.asSymbol("x");
        var y = e.asSymbol("x");

        var lhs = new Collect(new DocTableRelation(aDoc), List.of(x), WhereClause.MATCH_ALL);

        var rhs = new Collect(new DocTableRelation(bDoc), List.of(y), WhereClause.MATCH_ALL);

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(
            Map.of(
                aDoc.ident(), new Stats(9L, 9 * DataTypes.INTEGER.fixedSize(), Map.of()),
                bDoc.ident(), new Stats(1L, 1 * DataTypes.INTEGER.fixedSize(), Map.of())
            )
        );

        var union = new Union(lhs, rhs, List.of());

        var memo = new Memo(union);
        PlanStats planStats = new PlanStats(nodeContext, txnCtx, tableStats, memo);
        var result = planStats.get(union);
        assertThat(result.numDocs()).isEqualTo(10L);
        assertThat(result.sizeInBytes()).isEqualTo(160L);
    }

    @Test
    public void test_hash_join() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (y int)");

        DocTableInfo aDoc = e.resolveTableInfo("a");
        DocTableInfo bDoc = e.resolveTableInfo("b");

        var x = e.asSymbol("x");
        var y = e.asSymbol("y");
        var joinCondition = e.asSymbol("a.x = b.y");

        var lhs = new Collect(new DocTableRelation(aDoc), List.of(x), WhereClause.MATCH_ALL);
        var rhs = new Collect(new DocTableRelation(bDoc), List.of(y), WhereClause.MATCH_ALL);

        TableStats tableStats = new TableStats();
        Map<ColumnIdent, ColumnStats<?>> columnStats = Map.of(
            new ColumnIdent("x"),
            new ColumnStats<>(
                0,
                DataTypes.INTEGER.fixedSize(),
                9,
                DataTypes.INTEGER,
                MostCommonValues.EMPTY,
                List.of()
            ),
            new ColumnIdent("y"),
            new ColumnStats<>(
                0,
                DataTypes.INTEGER.fixedSize(),
                1,
                DataTypes.INTEGER,
                MostCommonValues.EMPTY,
                List.of()
            )
        );
        tableStats.updateTableStats(
            Map.of(
                aDoc.ident(), new Stats(9L, 9 * DataTypes.INTEGER.fixedSize(), columnStats),
                bDoc.ident(), new Stats(1L, 1 * DataTypes.INTEGER.fixedSize(), columnStats)
            )
        );

        var hashjoin = new HashJoin(lhs, rhs, joinCondition);

        var memo = new Memo(hashjoin);
        PlanStats planStats = new PlanStats(nodeContext, txnCtx, tableStats, memo);
        var result = planStats.get(hashjoin);
        assertThat(result.numDocs()).isEqualTo(1L);
        assertThat(result.sizeInBytes()).isEqualTo(32L);
    }

    @Test
    public void test_nl_join() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (y int)");

        DocTableInfo aDoc = e.resolveTableInfo("a");
        DocTableInfo bDoc = e.resolveTableInfo("b");

        var x = e.asSymbol("x");
        var y = e.asSymbol("y");

        DocTableRelation relation = new DocTableRelation(aDoc);
        var lhs = new Collect(relation, List.of(x), WhereClause.MATCH_ALL);
        var rhs = new Collect(new DocTableRelation(bDoc), List.of(y), WhereClause.MATCH_ALL);
        ColumnStats<Integer> xStats = ColumnStats.fromSortedValues(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9), DataTypes.INTEGER, 0, 9);
        ColumnStats<Integer> yStats = ColumnStats.fromSortedValues(List.of(1), DataTypes.INTEGER, 0, 1);

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(
            Map.of(
                aDoc.ident(), new Stats(9L, 9 * DataTypes.INTEGER.fixedSize(), Map.of(new ColumnIdent("x"), xStats)),
                bDoc.ident(), new Stats(2L, 2 * DataTypes.INTEGER.fixedSize(), Map.of(new ColumnIdent("y"), yStats))
            )
        );

        var nestedLoopJoin = new NestedLoopJoin(
            lhs, rhs, JoinType.INNER, Literal.BOOLEAN_TRUE, false, false, false);

        var memo = new Memo(nestedLoopJoin);
        PlanStats planStats = new PlanStats(nodeContext, txnCtx, tableStats, memo);
        var result = planStats.get(nestedLoopJoin);
        // lhs is the larger table which 9 entries, so the join will at max emit 9 entries
        assertThat(result.numDocs()).isEqualTo(9L);
        assertThat(result.sizeInBytes()).isEqualTo(288L);

        var joinCondition = e.asSymbol("x = y");
        nestedLoopJoin = new NestedLoopJoin(
            lhs, rhs, JoinType.INNER, joinCondition, false, false, false);
        result = planStats.get(nestedLoopJoin);
        assertThat(result.numDocs()).isEqualTo(1L);
        assertThat(result.sizeInBytes()).isEqualTo(32L);

        nestedLoopJoin = new NestedLoopJoin(
            lhs, rhs, JoinType.CROSS, x, false, false, false);

        memo = new Memo(nestedLoopJoin);
        planStats = new PlanStats(nodeContext, txnCtx, tableStats, memo);
        result = planStats.get(nestedLoopJoin);
        assertThat(result.numDocs()).isEqualTo(18L);
        assertThat(result.sizeInBytes()).isEqualTo(576L);
    }

    @Test
    public void test_filter() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        Symbol x = e.asSymbol("x");
        Collect source = new Collect(new DocTableRelation(tbl), List.of(x), WhereClause.MATCH_ALL);
        Filter filter = new Filter(source, e.asSymbol("x = 10"));

        TableStats tableStats = new TableStats();
        Map<ColumnIdent, ColumnStats<?>> columnStats = Map.of(
            new ColumnIdent("x"),
            new ColumnStats<>(
                0.0,
                DataTypes.INTEGER.fixedSize(),
                2500.0,
                DataTypes.INTEGER,
                new MostCommonValues(new Object[0], new double[0]),
                List.of()
            )
        );
        tableStats.updateTableStats(Map.of(
            tbl.ident(),
            new Stats(5_000, 5_000 * DataTypes.INTEGER.fixedSize(), columnStats)
        ));

        PlanStats planStats = new PlanStats(nodeContext, txnCtx, tableStats, null);
        Stats stats = planStats.get(filter);
        assertThat(stats.numDocs()).isEqualTo(2);
    }
}
