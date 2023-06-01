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

import java.util.List;
import java.util.function.Function;

import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.HashJoin;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class JoinGraphTest extends CrateDummyClusterServiceUnitTest {

    /*
     *  This builds the following graph:
     *  [a]---(a.x = b.y)---[b]
     */
    @Test
    public void test_simple_join() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (y int)")
            .build();

        DocTableInfo aDoc = e.resolveTableInfo("a");
        DocTableInfo bDoc = e.resolveTableInfo("b");

        var x = e.asSymbol("x");
        var y = e.asSymbol("y");

        var lhs = new Collect(1, new DocTableRelation(aDoc), List.of(x), WhereClause.MATCH_ALL);
        var rhs = new Collect(2, new DocTableRelation(bDoc), List.of(y), WhereClause.MATCH_ALL);

        var joinCondition = e.asSymbol("a.x = b.y");
        var hashjoin = new HashJoin(3, lhs, rhs, joinCondition);
        JoinGraph joinGraph = JoinGraph.create(hashjoin, Function.identity());
       assertThat(joinGraph.nodes()).containsExactly(lhs, rhs);
        var edges = joinGraph.edges().get(lhs.id());
        assertThat(edges).hasSize(1);
        var edge = getOnlyElement(edges);
        assertThat(edge.from).containsExactly(lhs.id());
        assertThat(edge.to).containsExactly(rhs.id());
    }

    /*
     *  This builds the following graph:
     *  [a]---(a.x = b.y)---[b]---(b.y = c.z)---[c]
     */
    @Test
    public void test_nested_join() throws Exception {
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

        var a = new Collect(1, new DocTableRelation(aDoc), List.of(x), WhereClause.MATCH_ALL);
        var b = new Collect(2, new DocTableRelation(bDoc), List.of(y), WhereClause.MATCH_ALL);
        var c = new Collect(3, new DocTableRelation(cDoc), List.of(z), WhereClause.MATCH_ALL);

        var firstJoin = new HashJoin(4, a, b, e.asSymbol("a.x = b.y"));
        var secondJoin = new HashJoin(5, firstJoin, c, e.asSymbol("b.y = c.z"));
        JoinGraph joinGraph = JoinGraph.create(secondJoin, Function.identity());
        // [a]--[a.x = b.y]--[b]--[b.y = c.z]--[c]
        assertThat(joinGraph.nodes()).containsExactly(a, b, c);
        var edges = joinGraph.edges().get(a.id());
        assertThat(edges).hasSize(1);
        var edge = getOnlyElement(edges);
        // `a.x = b.y` creates a edge from a to b
        assertThat(edge.from).containsExactly(a.id());
        assertThat(edge.to).containsExactly(b.id());

        // `b.y = c.z` creates a edge from b to c
        edges = joinGraph.edges().get(b.id());
        assertThat(edges).hasSize(1);
        edge = getOnlyElement(edges);
        assertThat(edge.from).containsExactly(b.id());
        assertThat(edge.to).containsExactly(c.id());

    }
}
