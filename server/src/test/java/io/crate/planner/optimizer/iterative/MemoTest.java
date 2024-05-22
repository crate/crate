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

package io.crate.planner.optimizer.iterative;

import static io.crate.common.collections.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;
import java.util.function.IntSupplier;

import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import io.crate.analyze.OrderBy;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.PlanHint;
import io.crate.planner.operators.SubQueryResults;
import io.crate.statistics.Stats;


public class MemoTest {

    private int id = 0;
    private IntSupplier ids = () -> id++;

    @Test
    public void testInitialization() {
        var plan = plan(plan());
        Memo memo = new Memo(plan);

        assertThat(memo.groupCount()).isEqualTo(2);
        assertMatchesStructure(plan, memo.extract());
    }

    /*
      From: X -> Y  -> Z
      To:   X -> Y' -> Z'
     */
    @Test
    public void testReplaceSubtree() {
        var plan = plan(plan(plan()));

        Memo memo = new Memo(plan);
        assertThat(memo.groupCount()).isEqualTo(3);

        // replace child of root node with subtree
        LogicalPlan transformed = plan(plan());
        memo.replace(getChildGroup(memo, memo.getRootGroup()), transformed);
        assertThat(memo.groupCount()).isEqualTo(3);
        assertMatchesStructure(memo.extract(), plan(plan.id(), transformed));
    }

    /*
      From: X -> Y  -> Z
      To:   X -> Y' -> Z
     */
    @Test
    public void testReplaceNode() {
        var z = plan();
        var y = plan(z);
        var x = plan(y);

        Memo memo = new Memo(x);
        assertThat(memo.groupCount()).isEqualTo(3);

        // replace child of root node with another node, retaining child's child
        int yGroup = getChildGroup(memo, memo.getRootGroup());
        GroupReference zRef = (GroupReference) getOnlyElement(memo.resolve(yGroup).sources());
        var transformed = plan(zRef);
        memo.replace(yGroup, transformed);
        assertThat(memo.groupCount()).isEqualTo(3);
        assertMatchesStructure(memo.extract(), plan(x.id(), plan(transformed.id(), z)));
    }

    /*
      From: X -> Y  -> Z  -> W
      To:   X -> Y' -> Z' -> W
     */
    @Test
    public void testReplaceNonLeafSubtree() {
        var w = plan();
        var z = plan(w);
        var y = plan(z);
        var x = plan(y);

        Memo memo = new Memo(x);

        assertThat(memo.groupCount()).isEqualTo(4);

        int yGroup = getChildGroup(memo, memo.getRootGroup());
        int zGroup = getChildGroup(memo, yGroup);

        LogicalPlan rewrittenW = memo.resolve(zGroup).sources().get(0);

        TestPlan newZ = plan(rewrittenW);
        TestPlan newY = plan(newZ);

        memo.replace(yGroup, newY);

        assertThat(memo.groupCount()).isEqualTo(4);

        assertMatchesStructure(
            memo.extract(),
            plan(x.id(),
                 plan(newY.id(),
                      plan(newZ.id(),
                           plan(w.id())))));
    }

    /*
      From: X -> Y -> Z
      To:   X -> Z
     */
    @Test
    public void testRemoveNode() {
        var z = plan();
        var y = plan(z);
        var x = plan(y);

        Memo memo = new Memo(x);

        assertThat(3).isEqualTo(memo.groupCount());
        assertThat(memo.groupCount()).isEqualTo(3);

        int yGroup = getChildGroup(memo, memo.getRootGroup());
        memo.replace(yGroup, memo.resolve(yGroup).sources().get(0));

        assertThat(memo.groupCount()).isEqualTo(2);

        assertMatchesStructure(
            memo.extract(),
            plan(x.id(),
                 plan(z.id())));
    }

    /*
       From: X -> Z
       To:   X -> Y -> Z
     */
    @Test
    public void testInsertNode() {
        var z = plan();
        var x = plan(z);

        Memo memo = new Memo(x);

        assertThat(memo.groupCount()).isEqualTo(2);

        int zGroup = getChildGroup(memo, memo.getRootGroup());
        var y = plan(memo.resolve(zGroup));
        memo.replace(zGroup, y);

        assertThat(memo.groupCount()).isEqualTo(3);

        assertMatchesStructure(
            memo.extract(),
            plan(x.id(),
                 plan(y.id(),
                      plan(z.id()))));
    }

    /*
      From: X -> Y -> Z
      To:   X --> Y1' --> Z
              \-> Y2' -/
     */
    @Test
    public void testMultipleReferences() {
        var z = plan();
        var y = plan(z);
        var x = plan(y);

        Memo memo = new Memo(x);
        assertThat(memo.groupCount()).isEqualTo(3);

        int yGroup = getChildGroup(memo, memo.getRootGroup());

        LogicalPlan rewrittenZ = memo.resolve(yGroup).sources().get(0);
        var y1 = plan(rewrittenZ);
        var y2 = plan(rewrittenZ);

        var newX = plan(y1, y2);
        memo.replace(memo.getRootGroup(), newX);
        assertThat(memo.groupCount()).isEqualTo(4);

        assertMatchesStructure(
            memo.extract(),
            plan(newX.id(),
                 plan(y1.id(), plan(z.id())),
                 plan(y2.id(), plan(z.id()))));
    }

    @Test
    public void test_store_and_evict_stats() {
        var y = plan();
        var x = plan(y);

        Memo memo = new Memo(x);
        int xGroup = memo.getRootGroup();
        int yGroup = getChildGroup(memo, memo.getRootGroup());
        var xStats = new Stats(1, 1, Map.of());
        var yStats = new Stats(3, 3, Map.of());

        memo.addStats(yGroup, yStats);
        memo.addStats(xGroup, xStats);

        assertThat(yStats).isEqualTo(memo.stats(yGroup));
        assertThat(xStats).isEqualTo(memo.stats(xGroup));

        memo.replace(yGroup, plan());

        assertThat(memo.stats(yGroup)).isNull();
        assertThat(memo.stats(xGroup)).isNull();
    }


    private static void assertMatchesStructure(LogicalPlan a, LogicalPlan e) {
        if (a instanceof TestPlan actual && e instanceof TestPlan expected) {
            assertThat(actual.getClass()).isEqualTo(expected.getClass());
            assertThat(actual.id()).isEqualTo(expected.id());
            assertThat(expected.sources().size()).isEqualTo(actual.sources().size());
            for (int i = 0; i < actual.sources().size(); i++) {
                assertMatchesStructure(actual.sources().get(i), expected.sources().get(i));
            }
        } else {
            fail("LogicalPlan is not a TestPlan");
        }
    }

    private int getChildGroup(Memo memo, int group) {
        LogicalPlan node = memo.resolve(group);
        GroupReference child = (GroupReference) node.sources().get(0);

        return child.groupId();
    }

    private TestPlan plan(int id, LogicalPlan... children) {
        return new TestPlan(id, List.of(children));
    }

    private TestPlan plan(LogicalPlan... children) {
        return plan(ids.getAsInt(), children);
    }

    public static class TestPlan implements LogicalPlan {
        private final List<LogicalPlan> sources;
        private final int id;

        public TestPlan(int id, List<LogicalPlan> sources) {
            this.id = id;
            this.sources = List.copyOf(sources);
        }

        @Override
        public List<LogicalPlan> sources() {
            return sources;
        }

        @Override
        public List<Symbol> outputs() {
            return List.of();
        }

        @Override
        public ExecutionPlan build(DependencyCarrier dependencyCarrier,
                                   PlannerContext plannerContext,
                                   Set<PlanHint> planHints,
                                   ProjectionBuilder projectionBuilder,
                                   int limit,
                                   int offset,
                                   @Nullable OrderBy order,
                                   @Nullable Integer pageSizeHint,
                                   Row params,
                                   SubQueryResults subQueryResults) {
            return null;
        }

        @Override
        public LogicalPlan replaceSources(List<LogicalPlan> sources) {
            return new TestPlan(id(), sources);
        }

        public int id() {
            return id;
        }

        @Override
        public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
            return null;
        }

        @Override
        public Map<LogicalPlan, SelectSymbol> dependencies() {
            return Map.of();
        }

        @Override
        public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
            return visitor.visitPlan(this, context);
        }

        @Override
        public List<RelationName> relationNames() {
            return List.of();
        }
    }
}

