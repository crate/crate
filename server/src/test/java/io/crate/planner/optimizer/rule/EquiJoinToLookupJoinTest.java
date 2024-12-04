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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.EquiJoinDetector;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.sql.tree.JoinType;
import io.crate.statistics.Stats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

import static io.crate.testing.Asserts.assertThat;

public class EquiJoinToLookupJoinTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private Reference x;
    private Reference y;
    private Collect lhs;
    private Collect rhs;
    private DocTableInfo lhsDocTableInfo;
    private DocTableInfo rhsDocTableInfo;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .build()
            .addTable("create table doc.lhs (x int)")
            .addTable("create table doc.rhs (y int)");

        lhsDocTableInfo = e.resolveTableInfo("lhs");
        rhsDocTableInfo = e.resolveTableInfo("rhs");

        x = (Reference) e.asSymbol("x");
        y = (Reference) e.asSymbol("y");

        lhs = new Collect(new DocTableRelation(lhsDocTableInfo), List.of(x), WhereClause.MATCH_ALL);
        rhs = new Collect(new DocTableRelation(rhsDocTableInfo), List.of(y), WhereClause.MATCH_ALL);
    }

    @Test
    public void test_lookup_join_lhs_is_larger() throws Exception {
        var joinCondition = e.asSymbol("lhs.x = rhs.y");
        var join = new JoinPlan(lhs, rhs, JoinType.INNER, joinCondition);

        assertThat(join).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ Collect[doc.lhs | [x] | true]",
            "  └ Collect[doc.rhs | [y] | true]"
        );

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(lhsDocTableInfo.ident(), new Stats(10000, 0, Map.of()));
        rowCountByTable.put(rhsDocTableInfo.ident(), new Stats(10, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        var rule = new EquiJoinToLookupJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());

        Assertions.assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(join);

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ MultiPhase",
            "  │  └ Filter[(x = ANY((doc.rhs)))]",
            "  │    └ Collect[doc.lhs | [x] | true]",
            "  │  └ Collect[doc.rhs | [y] | true]",
            "  └ Collect[doc.rhs | [y] | true]"
        );
    }

    @Test
    public void test_lookup_join_rhs_is_larger() throws Exception {
        var joinCondition = e.asSymbol("lhs.x = rhs.y");
        var join = new JoinPlan(lhs, rhs, JoinType.INNER, joinCondition);

        assertThat(join).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ Collect[doc.lhs | [x] | true]",
            "  └ Collect[doc.rhs | [y] | true]"
        );

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(lhsDocTableInfo.ident(), new Stats(10, 0, Map.of()));
        rowCountByTable.put(rhsDocTableInfo.ident(), new Stats(10000, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        var rule = new EquiJoinToLookupJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());

        Assertions.assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(join);

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ Collect[doc.lhs | [x] | true]",
            "  └ MultiPhase",
            "    └ Filter[(y = ANY((doc.lhs)))]",
            "      └ Collect[doc.rhs | [y] | true]",
            "    └ Collect[doc.lhs | [x] | true]"
        );
    }

    @Test
    public void test_filter_on_smaller_side() throws Exception {
        var joinCondition = e.asSymbol("lhs.x = rhs.y");
        var whereClause = lhs.where().add(e.asSymbol("lhs.x > 0"));
        var newLhsCollect = new Collect(lhs.relation(), lhs.outputs(), whereClause);
        var join = new JoinPlan(newLhsCollect, rhs, JoinType.INNER, joinCondition);

        assertThat(join).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ Collect[doc.lhs | [x] | (x > 0)]",
            "  └ Collect[doc.rhs | [y] | true]"
        );

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(lhsDocTableInfo.ident(), new Stats(10, 0, Map.of()));
        rowCountByTable.put(rhsDocTableInfo.ident(), new Stats(10000, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        var rule = new EquiJoinToLookupJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());

        Assertions.assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(join);

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ Collect[doc.lhs | [x] | (x > 0)]",
            "  └ MultiPhase",
            "    └ Filter[(y = ANY((doc.lhs)))]",
            "      └ Collect[doc.rhs | [y] | true]",
            "    └ Collect[doc.lhs | [x] | (x > 0)]"
        );
    }

    @Test
    public void test_filter_on_larger_side() throws Exception {
        var joinCondition = e.asSymbol("lhs.x = rhs.y");
        var whereClause = lhs.where().add(e.asSymbol("lhs.x > 0"));
        var newLhsCollect = new Collect(lhs.relation(), lhs.outputs(), whereClause);
        var join = new JoinPlan(newLhsCollect, rhs, JoinType.INNER, joinCondition);

        assertThat(join).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ Collect[doc.lhs | [x] | (x > 0)]",
            "  └ Collect[doc.rhs | [y] | true]"
        );

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(lhsDocTableInfo.ident(), new Stats(100000, 0, Map.of()));
        rowCountByTable.put(rhsDocTableInfo.ident(), new Stats(10, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        var rule = new EquiJoinToLookupJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());

        Assertions.assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(join);

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ MultiPhase",
            "  │  └ Filter[(x = ANY((doc.rhs)))]",
            "  │    └ Collect[doc.lhs | [x] | (x > 0)]",
            "  │  └ Collect[doc.rhs | [y] | true]",
            "  └ Collect[doc.rhs | [y] | true]"
        );
    }

    @Test
    public void test_skip_non_equi_joins() throws Exception {
        var joinCondition = e.asSymbol("lhs.x = 2");
        Assertions.assertThat(EquiJoinDetector.isEquiJoin(joinCondition)).isFalse();
        var join = new JoinPlan(lhs, rhs, JoinType.INNER, joinCondition);

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(lhsDocTableInfo.ident(), new Stats(10, 0, Map.of()));
        rowCountByTable.put(rhsDocTableInfo.ident(), new Stats(10000, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        var rule = new EquiJoinToLookupJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());

        Assertions.assertThat(match.isPresent()).isFalse();
    }

    @Test
    public void test_skip_no_stats() throws Exception {
        var joinCondition = e.asSymbol("lhs.x = rhs.y");
        var join = new JoinPlan(lhs, rhs, JoinType.INNER, joinCondition);

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(lhsDocTableInfo.ident(), Stats.EMPTY);
        rowCountByTable.put(rhsDocTableInfo.ident(), Stats.EMPTY);
        e.updateTableStats(rowCountByTable);

        var rule = new EquiJoinToLookupJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());

        Assertions.assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(join);

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).isNull();
    }

    public void test_skip_if_source_is_not_a_collect() throws Exception {
        var joinCondition = e.asSymbol("lhs.x = rhs.y");
        var lhsFilter = new Filter(lhs, e.asSymbol("lhs.x > 1"));
        var join = new JoinPlan(lhsFilter, rhs, JoinType.INNER, joinCondition);

        var rule = new EquiJoinToLookupJoin();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());

        Assertions.assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(join);

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).isNull();
    }

}
