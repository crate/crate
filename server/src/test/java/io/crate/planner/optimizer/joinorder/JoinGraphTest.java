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

package io.crate.planner.optimizer.joinorder;

import static io.crate.common.collections.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.function.Function;

import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.sql.tree.JoinType;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;

public class JoinGraphTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_single_join() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (y int)")
            .build();

        DocTableInfo aDoc = e.resolveTableInfo("a");
        DocTableInfo bDoc = e.resolveTableInfo("b");

        var x = e.asSymbol("x");
        var y = e.asSymbol("y");

        var a = new Collect(new DocTableRelation(aDoc), List.of(x), WhereClause.MATCH_ALL);
        var b = new Collect(new DocTableRelation(bDoc), List.of(y), WhereClause.MATCH_ALL);

        var joinCondition = e.asSymbol("a.x = b.y");
        var join = new JoinPlan(a, b, JoinType.INNER, joinCondition);

        assertThat(join).hasOperators(
            "JoinPlan[INNER | (x = y)]",
            "  ├ Collect[doc.a | [x] | true]",
            "  └ Collect[doc.b | [y] | true]"
        );

        JoinGraph joinGraph = JoinGraph.create(join, Function.identity());
        assertThat(joinGraph.nodes()).containsExactly(a, b);
        assertThat(joinGraph.edges()).hasSize(2);

        var edges = joinGraph.edges().get(a);
        assertThat(edges).hasSize(1);
        var edge = getOnlyElement(edges);
        assertThat(edge.to()).isEqualTo(b);
        assertThat(edge.left()).isEqualTo(x);
        assertThat(edge.right()).isEqualTo(y);

        edges = joinGraph.edges().get(b);
        assertThat(edges).hasSize(1);
        edge = getOnlyElement(edges);
        assertThat(edge.to()).isEqualTo(a);
        assertThat(edge.left()).isEqualTo(x);
        assertThat(edge.right()).isEqualTo(y);

        var originalOrder = joinGraph.buildLogicalPlan();
        assertThat(originalOrder).hasOperators(
            "JoinPlan[INNER | (x = y)]",
            "  ├ Collect[doc.a | [x] | true]",
            "  └ Collect[doc.b | [y] | true]"
        );

        var reordered = joinGraph.reorder(List.of(b, a));
        assertThat(reordered).isEqualTo(
            "JoinPlan[INNER | (x = y)]\n" +
            "  ├ Collect[doc.b | [y] | true]\n" +
            "  └ Collect[doc.a | [x] | true]"
        );
    }

    @Test
    public void test_double_join() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (y int)")
            .addTable("create table c (z int)")
            .build();

        DocTableInfo aDoc = e.resolveTableInfo("a");
        DocTableInfo bDoc = e.resolveTableInfo("b");
        DocTableInfo cDoc = e.resolveTableInfo("c");

        var x = e.asSymbol("x");
        var y = e.asSymbol("y");
        var z = e.asSymbol("z");

        var a = new Collect(new DocTableRelation(aDoc), List.of(x), WhereClause.MATCH_ALL);
        var b = new Collect(new DocTableRelation(bDoc), List.of(y), WhereClause.MATCH_ALL);
        var c = new Collect(new DocTableRelation(cDoc), List.of(z), WhereClause.MATCH_ALL);

        Symbol firstJoinCondition = e.asSymbol("a.x = b.y");
        var firstJoin = new JoinPlan(a, b, JoinType.INNER, firstJoinCondition);
        Symbol secondJoinCondition = e.asSymbol("b.y = c.z");
        var join = new JoinPlan(firstJoin, c, JoinType.INNER, secondJoinCondition);

        Asserts.assertThat(join).hasOperators(
            "JoinPlan[INNER | (y = z)]",
            "  ├ JoinPlan[INNER | (x = y)]",
            "  │  ├ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.b | [y] | true]",
            "  └ Collect[doc.c | [z] | true]"
        );

        JoinGraph joinGraph = JoinGraph.create(join, Function.identity());
        // This builds the following graph:
        // [a]--[a.x = b.y]--[b]--[b.y = c.z]--[c]
        assertThat(joinGraph.nodes()).containsExactly(a, b, c);

        var edges = joinGraph.edges().get(a);
        assertThat(edges).hasSize(1);
        // `a.x = b.y` creates an edge from a to b
        assertThat(edges).contains(
            new JoinGraph.Edge(b, x, y)
        );

        // `b.y = c.z` creates an edge from b to c
        edges = joinGraph.edges().get(b);
        assertThat(edges).hasSize(2);
        assertThat(edges).contains(
            new JoinGraph.Edge(a, x, y),
            new JoinGraph.Edge(c, y, z)
        );

        var originalOrder = joinGraph.buildLogicalPlan();
        assertThat(originalOrder).hasOperators(
            "JoinPlan[INNER | (y = z)]",
            "  ├ JoinPlan[INNER | (x = y)]",
            "  │  ├ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.b | [y] | true]",
            "  └ Collect[doc.c | [z] | true]"
        );

        var reordered = joinGraph.reorder(List.of(c, b, a));
        assertThat(reordered).hasOperators(
            "JoinPlan[INNER | (x = y)]",
            "  ├ JoinPlan[INNER | (y = z)]",
            "  │  ├ Collect[doc.c | [z] | true]",
            "  │  └ Collect[doc.b | [y] | true]",
            "  └ Collect[doc.a | [x] | true]"
        );

        List<LogicalPlan> invalidOrder = List.of(a, c, b);
        assertThatThrownBy(() -> joinGraph.reorder(invalidOrder))
            .hasMessage("JoinPlan cannot be built with the provided order [doc.a, doc.c, doc.b]");
    }

    @Test
    public void test_triple_join_reordering() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (w int)")
            .addTable("create table b (x int)")
            .addTable("create table c (y int)")
            .addTable("create table d (z int)")
            .build();

        DocTableInfo aDoc = e.resolveTableInfo("a");
        DocTableInfo bDoc = e.resolveTableInfo("b");
        DocTableInfo cDoc = e.resolveTableInfo("c");
        DocTableInfo dDoc = e.resolveTableInfo("d");

        var w = e.asSymbol("w");
        var x = e.asSymbol("x");
        var y = e.asSymbol("y");
        var z = e.asSymbol("z");

        var a = new Collect(new DocTableRelation(aDoc), List.of(w), WhereClause.MATCH_ALL);
        var b = new Collect(new DocTableRelation(bDoc), List.of(x), WhereClause.MATCH_ALL);
        var c = new Collect(new DocTableRelation(cDoc), List.of(y), WhereClause.MATCH_ALL);
        var d = new Collect(new DocTableRelation(dDoc), List.of(z), WhereClause.MATCH_ALL);

        Symbol firstJoinCondition = e.asSymbol("a.w = b.x");
        var firstJoin = new JoinPlan(a, b, JoinType.INNER, firstJoinCondition);

        Symbol secondJoinCondition = e.asSymbol("a.w = c.y");
        var secondJoin = new JoinPlan(firstJoin, c, JoinType.INNER, secondJoinCondition);

        Symbol topJoinCondition = e.asSymbol("b.x = d.z");
        var topJoin = new JoinPlan(secondJoin, d, JoinType.INNER, topJoinCondition);

        Asserts.assertThat(topJoin).isEqualTo(
            "JoinPlan[INNER | (x = z)]\n" +
            "  ├ JoinPlan[INNER | (w = y)]\n" +
            "  │  ├ JoinPlan[INNER | (w = x)]\n" +
            "  │  │  ├ Collect[doc.a | [w] | true]\n" +
            "  │  │  └ Collect[doc.b | [x] | true]\n" +
            "  │  └ Collect[doc.c | [y] | true]\n" +
            "  └ Collect[doc.d | [z] | true]"
        );

        JoinGraph joinGraph = JoinGraph.create(topJoin, Function.identity());

        Asserts.assertThat(joinGraph.buildLogicalPlan()).isEqualTo(
            "JoinPlan[INNER | (x = z)]\n" +
            "  ├ JoinPlan[INNER | (w = y)]\n" +
            "  │  ├ JoinPlan[INNER | (w = x)]\n" +
            "  │  │  ├ Collect[doc.a | [w] | true]\n" +
            "  │  │  └ Collect[doc.b | [x] | true]\n" +
            "  │  └ Collect[doc.c | [y] | true]\n" +
            "  └ Collect[doc.d | [z] | true]"
        );

        Asserts.assertThat(joinGraph.reorder(List.of(a, c, b, d))).isEqualTo(
            "JoinPlan[INNER | (x = z)]\n" +
            "  ├ JoinPlan[INNER | (w = x)]\n" +
            "  │  ├ JoinPlan[INNER | (w = y)]\n" +
            "  │  │  ├ Collect[doc.a | [w] | true]\n" +
            "  │  │  └ Collect[doc.c | [y] | true]\n" +
            "  │  └ Collect[doc.b | [x] | true]\n" +
            "  └ Collect[doc.d | [z] | true]"
        );

        Asserts.assertThat(joinGraph.reorder(List.of(b, d, a, c))).isEqualTo(
            "JoinPlan[INNER | (w = y)]\n" +
            "  ├ JoinPlan[INNER | (w = x)]\n" +
            "  │  ├ JoinPlan[INNER | (x = z)]\n" +
            "  │  │  ├ Collect[doc.b | [x] | true]\n" +
            "  │  │  └ Collect[doc.d | [z] | true]\n" +
            "  │  └ Collect[doc.a | [w] | true]\n" +
            "  └ Collect[doc.c | [y] | true]"
        );
    }

    @Test
    public void test_reordering_with_filter() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (y int)")
            .addTable("create table c (z int)")
            .build();

        DocTableInfo aDoc = e.resolveTableInfo("a");
        DocTableInfo bDoc = e.resolveTableInfo("b");
        DocTableInfo cDoc = e.resolveTableInfo("c");

        var x = e.asSymbol("x");
        var y = e.asSymbol("y");
        var z = e.asSymbol("z");

        var a = new Collect(new DocTableRelation(aDoc), List.of(x), WhereClause.MATCH_ALL);
        var b = new Collect(new DocTableRelation(bDoc), List.of(y), WhereClause.MATCH_ALL);
        var c = new Collect(new DocTableRelation(cDoc), List.of(z), WhereClause.MATCH_ALL);

        Symbol firstJoinCondition = e.asSymbol("a.x = b.y");
        var firstJoin = new JoinPlan(a, b, JoinType.INNER, firstJoinCondition);
        Symbol secondJoinCondition = e.asSymbol("b.y = c.z");
        var join = new JoinPlan(firstJoin, c, JoinType.INNER, secondJoinCondition);
        var filter = new Filter(join, e.asSymbol("a.x > 1"));

        Asserts.assertThat(filter).isEqualTo(
            "Filter[(x > 1)]\n" +
            "  └ JoinPlan[INNER | (y = z)]\n" +
            "    ├ JoinPlan[INNER | (x = y)]\n" +
            "    │  ├ Collect[doc.a | [x] | true]\n" +
            "    │  └ Collect[doc.b | [y] | true]\n" +
            "    └ Collect[doc.c | [z] | true]"
        );

        JoinGraph joinGraph = JoinGraph.create(filter, Function.identity());

        Asserts.assertThat(joinGraph.buildLogicalPlan()).isEqualTo(
          "Filter[(x > 1)]\n" +
          "  └ JoinPlan[INNER | (y = z)]\n" +
          "    ├ JoinPlan[INNER | (x = y)]\n" +
          "    │  ├ Collect[doc.a | [x] | true]\n" +
          "    │  └ Collect[doc.b | [y] | true]\n" +
          "    └ Collect[doc.c | [z] | true]"
        );

        Asserts.assertThat(joinGraph.reorder(List.of(c,b,a))).isEqualTo(
            "Filter[(x > 1)]\n" +
            "  └ JoinPlan[INNER | (x = y)]\n" +
            "    ├ JoinPlan[INNER | (y = z)]\n" +
            "    │  ├ Collect[doc.c | [z] | true]\n" +
            "    │  └ Collect[doc.b | [y] | true]\n" +
            "    └ Collect[doc.a | [x] | true]"
        );

        var secondFilter = new Filter(filter, e.asSymbol("b.y < 10"));

        joinGraph = JoinGraph.create(secondFilter, Function.identity());

        Asserts.assertThat(joinGraph.buildLogicalPlan()).isEqualTo(
            "Filter[(y < 10)]\n" +
            "  └ Filter[(x > 1)]\n" +
            "    └ JoinPlan[INNER | (y = z)]\n" +
            "      ├ JoinPlan[INNER | (x = y)]\n" +
            "      │  ├ Collect[doc.a | [x] | true]\n" +
            "      │  └ Collect[doc.b | [y] | true]\n" +
            "      └ Collect[doc.c | [z] | true]"
        );

        Asserts.assertThat(joinGraph.reorder(List.of(c,b,a))).isEqualTo(
            "Filter[(y < 10)]\n" +
            "  └ Filter[(x > 1)]\n" +
            "    └ JoinPlan[INNER | (x = y)]\n" +
            "      ├ JoinPlan[INNER | (y = z)]\n" +
            "      │  ├ Collect[doc.c | [z] | true]\n" +
            "      │  └ Collect[doc.b | [y] | true]\n" +
            "      └ Collect[doc.a | [x] | true]"
        );
    }
}
