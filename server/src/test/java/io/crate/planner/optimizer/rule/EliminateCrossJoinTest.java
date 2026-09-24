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

package io.crate.planner.optimizer.rule;

import static io.crate.testing.Asserts.assertThat;

import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.common.collections.Lists;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.DocTableInfo;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.Order;
import io.crate.planner.operators.Rename;
import io.crate.planner.operators.Union;
import io.crate.planner.optimizer.joinorder.JoinGraph;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.sql.tree.JoinType;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class EliminateCrossJoinTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private Reference x;
    private Reference y;
    private Reference z;
    private Reference w;
    private Collect a;
    private Collect b;
    private Collect c;
    private Collect d;
    private DocTableInfo aDoc;
    private DocTableInfo bDoc;
    private DocTableInfo cDoc;
    private DocTableInfo dDoc;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table a (x int)")
            .addTable("create table b (y int)")
            .addTable("create table c (z int)")
            .addTable("create table d (w int)");

        aDoc = e.resolveTableInfo("a");
        bDoc = e.resolveTableInfo("b");
        cDoc = e.resolveTableInfo("c");
        dDoc = e.resolveTableInfo("d");

        x = (Reference) e.asSymbol("x");
        y = (Reference) e.asSymbol("y");
        z = (Reference) e.asSymbol("z");
        w = (Reference) e.asSymbol("w");

        a = new Collect(new DocTableRelation(aDoc), List.of(x), WhereClause.MATCH_ALL);
        b = new Collect(new DocTableRelation(bDoc), List.of(y), WhereClause.MATCH_ALL);
        c = new Collect(new DocTableRelation(cDoc), List.of(z), WhereClause.MATCH_ALL);
        d = new Collect(new DocTableRelation(dDoc), List.of(w), WhereClause.MATCH_ALL);
    }

    @Test
    public void test_cannot_apply_if_there_are_no_cross_joins() throws Exception {
        var joinCondition = e.asSymbol("a.x = b.y");
        var join = new JoinPlan(a, b, JoinType.INNER, joinCondition);

        assertThat(join).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ Collect[doc.a | [x] | true]",
            "  └ Collect[doc.b | [y] | true]"
        );

        var rule = new EliminateCrossJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());
        LogicalPlan result = rule.apply(join, match.captures(), e.ruleContext());
        assertThat(result).isNull();
    }

    @Test
    public void test_eliminate_cross_join() throws Exception {
        var firstJoin = new JoinPlan(a, b, JoinType.CROSS, null);
        Symbol joinCondition = e.asSymbol("c.z = a.x AND c.z = b.y");
        var join = new JoinPlan(firstJoin, c, JoinType.INNER, joinCondition);

        assertThat(join).hasOperators(
            "Join[INNER | ((x = z) AND (y = z))]",
            "  ├ Join[CROSS]",
            "  │  ├ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.b | [y] | true]",
            "  └ Collect[doc.c | [z] | true]"
        );

        var joinGraph = JoinGraph.create(join, UnaryOperator.identity());
        var originalOrder = joinGraph.nodes();
        assertThat(originalOrder).isEqualTo(List.of(a, b, c));
        var newOrder = EliminateCrossJoin.orderNodes(joinGraph);
        assertThat(newOrder).isEqualTo(List.of(a, c, b));

        var rule = new EliminateCrossJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(join);

        var result = rule.apply(match.value(),
                                match.captures(),
                                e.ruleContext());

        assertThat(result).hasOperators(
            "Eval[x, y, z]",
            "  └ Join[INNER | (y = z)]",
            "    ├ Join[INNER | (x = z)]",
            "    │  ├ Collect[doc.a | [x] | true]",
            "    │  └ Collect[doc.c | [z] | true]",
            "    └ Collect[doc.b | [y] | true]"
        );
    }

    @Test
    public void test_eliminate_cross_join_when_order_does_not_change() throws Exception {
        var firstJoin = new JoinPlan(a, c, JoinType.CROSS, null);
        Symbol joinCondition = e.asSymbol("c.z = a.x AND c.z = b.y");
        var join = new JoinPlan(firstJoin, b, JoinType.INNER, joinCondition);

        assertThat(join).hasOperators(
            "Join[INNER | ((x = z) AND (y = z))]",
            "  ├ Join[CROSS]",
            "  │  ├ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.c | [z] | true]",
            "  └ Collect[doc.b | [y] | true]"
        );

        var joinGraph = JoinGraph.create(join, UnaryOperator.identity());
        var newOrder = EliminateCrossJoin.orderNodes(joinGraph);
        var originalOrder = joinGraph.nodes();
        assertThat(originalOrder).isEqualTo(newOrder);

        var rule = new EliminateCrossJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(join);

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).hasOperators(
            "Join[INNER | (y = z)]",
            "  ├ Join[INNER | (x = z)]",
            "  │  ├ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.c | [z] | true]",
            "  └ Collect[doc.b | [y] | true]"
        );
    }

    @Test
    public void test_cannot_apply_cross_join_has_no_equi_join() throws Exception {
        var firstJoin = new JoinPlan(a, b, JoinType.INNER, e.asSymbol("a.x = b.y"));
        var join = new JoinPlan(firstJoin, c, JoinType.CROSS, null);

        assertThat(join).hasOperators(
            "Join[CROSS]",
            "  ├ Join[INNER | (x = y)]",
            "  │  ├ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.b | [y] | true]",
            "  └ Collect[doc.c | [z] | true]"
        );

        var rule = new EliminateCrossJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());
        var result = rule.apply(match.value(), match.captures(), e.ruleContext());

        // `c` has no equi-condition anywhere, so no edge for it can ever exist in the JoinGraph we build internally.
        // The rule fires (num of relations >= 3, has a cross join), but it's not applied.
        assertThat(result).isNull();
    }

    @Test
    public void test_cannot_apply_cross_join_with_partial_match() throws Exception {
        var firstJoin = new JoinPlan(a, b, JoinType.CROSS, null);
        var join = new JoinPlan(firstJoin, c, JoinType.INNER, e.asSymbol("a.x = c.z"));

        assertThat(join).hasOperators(
            "Join[INNER | (x = z)]",
            "  ├ Join[CROSS]",
            "  │  ├ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.b | [y] | true]",
            "  └ Collect[doc.c | [z] | true]"
        );

        var rule = new EliminateCrossJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());
        var result = rule.apply(match.value(), match.captures(), e.ruleContext());

        assertThat(result).isNull();
    }

    @Test
    public void test_do_not_reorder_with_outer_joins() throws Exception {
        var firstJoin = new JoinPlan(a, b, JoinType.CROSS, null);
        Symbol joinCondition = e.asSymbol("c.z = a.x AND c.z = b.y");
        var join = new JoinPlan(firstJoin, c, JoinType.LEFT, joinCondition);

        assertThat(join).hasOperators(
            "Join[LEFT | ((x = z) AND (y = z))]",
            "  ├ Join[CROSS]",
            "  │  ├ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.b | [y] | true]",
            "  └ Collect[doc.c | [z] | true]"
        );

        var rule = new EliminateCrossJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(join);

        LogicalPlan result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).isNull();

        joinCondition = e.asSymbol("a.x = b.y");
        firstJoin = new JoinPlan(a, b, JoinType.LEFT, joinCondition);
        join = new JoinPlan(firstJoin, c, JoinType.CROSS, null);

        match = rule.pattern().accept(join, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(join);

        result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).isNull();
    }

    @Test
    public void test_do_not_reorder_without_a_crossjoin() throws Exception {
        var firstJoin = new JoinPlan(a, b, JoinType.LEFT, e.asSymbol("a.x = b.y"));
        var secondJoin = new JoinPlan(firstJoin, c, JoinType.INNER, e.asSymbol("a.x = b.y"));

        assertThat(secondJoin).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ Join[LEFT | (x = y)]",
            "  │  ├ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.b | [y] | true]",
            "  └ Collect[doc.c | [z] | true]"
        );

        var rule = new EliminateCrossJoin();
        Match<JoinPlan> match = rule.pattern().accept(secondJoin, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(secondJoin);

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).isNull();
    }

    // todo adapt this too
    @Test
    public void test_resolve_multiple_aliases_with_same_name_in_logical_join_plan() throws Exception {

        var relation_a = new DocTableRelation(aDoc);
        var relation_b = new DocTableRelation(bDoc);
        var relation_c = new DocTableRelation(cDoc);
        var relation_d = new DocTableRelation(dDoc);

        var aliased_a = new AliasedAnalyzedRelation(relation_a, new RelationName(null, "alias"));
        var collect_a = new Collect(relation_a, List.of(x), WhereClause.MATCH_ALL);

        var collect_b = new Collect(relation_b, List.of(y), WhereClause.MATCH_ALL);

        var aliased_c = new AliasedAnalyzedRelation(relation_c, new RelationName(null, "alias"));
        var collect_c = new Collect(relation_c, List.of(z), WhereClause.MATCH_ALL);

        var collect_d = new Collect(relation_d, List.of(w), WhereClause.MATCH_ALL);

        Symbol scoped_x = aliased_a.getField(x.column(), Operation.READ, true);
        assertThat(scoped_x).isNotNull();

        Symbol scoped_z = aliased_c.getField(z.column(), Operation.READ, true);
        assertThat(scoped_z).isNotNull();

        var rename_a = new Rename(List.of(scoped_x), aliased_a.relationName(), aliased_a, collect_a);
        var rename_c = new Rename(List.of(scoped_z), aliased_c.relationName(), aliased_c, collect_c);

        var firstJoin = new JoinPlan(rename_a, collect_b, JoinType.INNER, EqOperator.of(scoped_x, y));
        var secondJoin = new JoinPlan(firstJoin, rename_c, JoinType.INNER, EqOperator.of(scoped_z, y));
        var thirdJoin = new JoinPlan(secondJoin, collect_d, JoinType.INNER, EqOperator.of(scoped_x, w));

        assertThat(thirdJoin).hasOperators(
            "Join[INNER | (x = w)]",
            "  ├ Join[INNER | (z = y)]",
            "  │  ├ Join[INNER | (x = y)]",
            "  │  │  ├ Rename[x] AS alias",
            "  │  │  │  └ Collect[doc.a | [x] | true]",
            "  │  │  └ Collect[doc.b | [y] | true]",
            "  │  └ Rename[z] AS alias",
            "  │    └ Collect[doc.c | [z] | true]",
            "  └ Collect[doc.d | [w] | true]"
        );

        var graph = JoinGraph.create(thirdJoin, UnaryOperator.identity());
        assertThat(graph.size()).isEqualTo(4);
        assertThat(graph.edges()).hasSize(4);
        assertThat(graph.nodes().get(0)).isEqualTo(rename_a);
        assertThat(graph.nodes().get(1)).isEqualTo(b);
        assertThat(graph.nodes().get(2)).isEqualTo(rename_c);
        assertThat(graph.nodes().get(3)).isEqualTo(d);
    }

    @Test
    public void test_eliminate_cross_join_with_order() throws Exception {
        var order = new Order(a, new OrderBy(List.of(x)));
        var firstJoin = new JoinPlan(order, b, JoinType.CROSS, null);
        Symbol joinCondition = e.asSymbol("a.x = b.y AND b.y = c.z");
        var join = new JoinPlan(firstJoin, c, JoinType.INNER, joinCondition);

        assertThat(join).hasOperators(
            "Join[INNER | ((x = y) AND (y = z))]",
            "  ├ Join[CROSS]",
            "  │  ├ OrderBy[x ASC]",
            "  │  │  └ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.b | [y] | true]",
            "  └ Collect[doc.c | [z] | true]"
        );

        var rule = new EliminateCrossJoin();
        var match = rule.pattern().accept(join, Captures.empty());
        var result = rule.apply(match.value(), match.captures(), e.ruleContext());

        // The CROSS join is eliminated (converted to INNER via a.x = b.y),
        // and the OrderBy wrapping `a` survives intact at its original position.
        assertThat(result).hasOperators(
            "Join[INNER | (y = z)]",
            "  ├ Join[INNER | (x = y)]",
            "  │  ├ OrderBy[x ASC]",
            "  │  │  └ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.b | [y] | true]",
            "  └ Collect[doc.c | [z] | true]"
        );
    }

    @Test
    public void test_eliminate_cross_join_with_union() throws Exception {
        Union union = new Union(a, b, Lists.concat(a.outputs(), b.outputs()));
        var firstJoin = new JoinPlan(union, c, JoinType.CROSS, null);
        Symbol joinCondition = e.asSymbol("b.y = c.z AND a.x = d.w");
        var join = new JoinPlan(firstJoin, d, JoinType.INNER, joinCondition);

        assertThat(join).hasOperators(
            "Join[INNER | ((y = z) AND (x = w))]",
            "  ├ Join[CROSS]",
            "  │  ├ Union[x, y]",
            "  │  │  ├ Collect[doc.a | [x] | true]",
            "  │  │  └ Collect[doc.b | [y] | true]",
            "  │  └ Collect[doc.c | [z] | true]",
            "  └ Collect[doc.d | [w] | true]"
        );

        var rule = new EliminateCrossJoin();
        var match = rule.pattern().accept(join, Captures.empty());
        var result = rule.apply(match.value(), match.captures(), e.ruleContext());

        // The CROSS join is eliminated (converted to INNER via b.y = c.z),
        // and the Union wrapping a/b survives intact as a single leaf.
        assertThat(result).hasOperators(
            "Join[INNER | (x = w)]",
            "  ├ Join[INNER | (y = z)]",
            "  │  ├ Union[x, y]",
            "  │  │  ├ Collect[doc.a | [x] | true]",
            "  │  │  └ Collect[doc.b | [y] | true]",
            "  │  └ Collect[doc.c | [z] | true]",
            "  └ Collect[doc.d | [w] | true]"
        );
    }

    /// [Filter from IN-Operator in Join condition is ignored in query plan](https://github.com/crate/crate/issues/14854)
    @Test
    public void test_eliminate_cross_join_with_constant_join_conditions_become_filters() throws Exception {
        var firstJoin = new JoinPlan(a, b, JoinType.CROSS, null);
        Symbol joinCondition = e.asSymbol("a.x = b.y AND a.x > 1 AND b.y = c.z");
        var join = new JoinPlan(firstJoin, c, JoinType.INNER, joinCondition);

        assertThat(join).hasOperators(
            "Join[INNER | (((x = y) AND (x > 1)) AND (y = z))]",
            "  ├ Join[CROSS]",
            "  │  ├ Collect[doc.a | [x] | true]",
            "  │  └ Collect[doc.b | [y] | true]",
            "  └ Collect[doc.c | [z] | true]"
        );

        var rule = new EliminateCrossJoin();
        var match = rule.pattern().accept(join, Captures.empty());
        var result = rule.apply(match.value(), match.captures(), e.ruleContext());

        // The CROSS join is eliminated (converted to INNER via a.x = b.y),
        // and the non-equi, single-relation condition (a.x > 1) survives as
        // a Filter wrapping the rebuilt join, instead of being dropped.
        assertThat(result).hasOperators(
            "Filter[(x > 1)]",
            "  └ Join[INNER | (y = z)]",
            "    ├ Join[INNER | (x = y)]",
            "    │  ├ Collect[doc.a | [x] | true]",
            "    │  └ Collect[doc.b | [y] | true]",
            "    └ Collect[doc.c | [z] | true]"
        );
    }

    @Test
    public void test_eliminate_cross_joins_with_remaining_cross_joins() throws Exception {
        var join1 = new JoinPlan(a, b, JoinType.CROSS, null);
        var join2 = new JoinPlan(join1, c, JoinType.CROSS, null);
        var join3 = new JoinPlan(join2, d, JoinType.INNER, e.asSymbol("a.x = b.y AND c.z = d.w"));

        assertThat(join3).hasOperators(
            "Join[INNER | ((x = y) AND (z = w))]",
            "  ├ Join[CROSS]",
            "  │  ├ Join[CROSS]",
            "  │  │  ├ Collect[doc.a | [x] | true]",
            "  │  │  └ Collect[doc.b | [y] | true]",
            "  │  └ Collect[doc.c | [z] | true]",
            "  └ Collect[doc.d | [w] | true]"
        );

        var rule = new EliminateCrossJoin();
        var match = rule.pattern().accept(join3, Captures.empty());

        assertThat(match.isPresent()).isTrue();

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).hasOperators(
            "Join[INNER | (z = w)]",
            "  ├ Join[CROSS]",
            "  │  ├ Join[INNER | (x = y)]",
            "  │  │  ├ Collect[doc.a | [x] | true]",
            "  │  │  └ Collect[doc.b | [y] | true]",
            "  │  └ Collect[doc.c | [z] | true]",
            "  └ Collect[doc.d | [w] | true]"
        );
    }
}
