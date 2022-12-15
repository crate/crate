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
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.junit.Test;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanId;
import io.crate.planner.operators.LogicalPlanIdAllocator;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.PlanHint;
import io.crate.planner.operators.SubQueryResults;
import io.crate.statistics.TableStats;


public class MemoTest {
    private final LogicalPlanIdAllocator idAllocator = new LogicalPlanIdAllocator();

    @Test
    public void testInitialization() {
        LogicalPlan plan = node(node());
        Memo memo = new Memo(idAllocator, plan);

        assertEquals(memo.getGroupCount(), 2);
        assertMatchesStructure(plan, memo.extract());
    }

    /*
      From: X -> Y  -> Z
      To:   X -> Y' -> Z'
     */
    @Test
    public void testReplaceSubtree() {
        LogicalPlan plan = node(node(node()));

        Memo memo = new Memo(idAllocator, plan);
        assertEquals(memo.getGroupCount(), 3);

        // replace child of root node with subtree
        LogicalPlan transformed = node(node());
        memo.replace(getChildGroup(memo, memo.getRootGroup()), transformed, "rule");
        assertEquals(memo.getGroupCount(), 3);
        assertMatchesStructure(memo.extract(), node(plan.id(), transformed));
    }

    /*
      From: X -> Y  -> Z
      To:   X -> Y' -> Z
     */
    @Test
    public void testReplaceNode() {
        LogicalPlan z = node();
        LogicalPlan y = node(z);
        LogicalPlan x = node(y);

        Memo memo = new Memo(idAllocator, x);
        assertEquals(memo.getGroupCount(), 3);

        // replace child of root node with another node, retaining child's child
        int yGroup = getChildGroup(memo, memo.getRootGroup());
        GroupReference zRef = (GroupReference) getOnlyElement(memo.getNode(yGroup).sources());
        LogicalPlan transformed = node(zRef);
        memo.replace(yGroup, transformed, "rule");
        assertEquals(memo.getGroupCount(), 3);
        assertMatchesStructure(memo.extract(), node(x.id(), node(transformed.id(), z)));
    }

    /*
      From: X -> Y  -> Z  -> W
      To:   X -> Y' -> Z' -> W
     */
    @Test
    public void testReplaceNonLeafSubtree() {
        LogicalPlan w = node();
        LogicalPlan z = node(w);
        LogicalPlan y = node(z);
        LogicalPlan x = node(y);

        Memo memo = new Memo(idAllocator, x);

        assertEquals(memo.getGroupCount(), 4);

        int yGroup = getChildGroup(memo, memo.getRootGroup());
        int zGroup = getChildGroup(memo, yGroup);

        LogicalPlan rewrittenW = memo.getNode(zGroup).sources().get(0);

        LogicalPlan newZ = node(rewrittenW);
        LogicalPlan newY = node(newZ);

        memo.replace(yGroup, newY, "rule");

        assertEquals(memo.getGroupCount(), 4);

        assertMatchesStructure(
            memo.extract(),
            node(x.id(),
                 node(newY.id(),
                      node(newZ.id(),
                           node(w.id())))));
    }

    /*
      From: X -> Y -> Z
      To:   X -> Z
     */
    @Test
    public void testRemoveNode() {
        LogicalPlan z = node();
        LogicalPlan y = node(z);
        LogicalPlan x = node(y);

        Memo memo = new Memo(idAllocator, x);

        assertEquals(memo.getGroupCount(), 3);

        int yGroup = getChildGroup(memo, memo.getRootGroup());
        memo.replace(yGroup, memo.getNode(yGroup).sources().get(0), "rule");

        assertEquals(memo.getGroupCount(), 2);

        assertMatchesStructure(
            memo.extract(),
            node(x.id(),
                 node(z.id())));
    }

    /*
       From: X -> Z
       To:   X -> Y -> Z
     */
    @Test
    public void testInsertNode() {
        LogicalPlan z = node();
        LogicalPlan x = node(z);

        Memo memo = new Memo(idAllocator, x);

        assertEquals(memo.getGroupCount(), 2);

        int zGroup = getChildGroup(memo, memo.getRootGroup());
        LogicalPlan y = node(memo.getNode(zGroup));
        memo.replace(zGroup, y, "rule");

        assertEquals(memo.getGroupCount(), 3);

        assertMatchesStructure(
            memo.extract(),
            node(x.id(),
                 node(y.id(),
                      node(z.id()))));
    }

    /*
      From: X -> Y -> Z
      To:   X --> Y1' --> Z
              \-> Y2' -/
     */
    @Test
    public void testMultipleReferences() {
        LogicalPlan z = node();
        LogicalPlan y = node(z);
        LogicalPlan x = node(y);

        Memo memo = new Memo(idAllocator, x);
        assertEquals(memo.getGroupCount(), 3);

        int yGroup = getChildGroup(memo, memo.getRootGroup());

        LogicalPlan rewrittenZ = memo.getNode(yGroup).sources().get(0);
        LogicalPlan y1 = node(rewrittenZ);
        LogicalPlan y2 = node(rewrittenZ);

        LogicalPlan newX = node(y1, y2);
        memo.replace(memo.getRootGroup(), newX, "rule");
        assertEquals(memo.getGroupCount(), 4);

        assertMatchesStructure(
            memo.extract(),
            node(newX.id(),
                 node(y1.id(), node(z.id())),
                 node(y2.id(), node(z.id()))));
    }


    private static void assertMatchesStructure(LogicalPlan actual, LogicalPlan expected) {
        assertEquals(actual.getClass(), expected.getClass());
        assertEquals(actual.id(), expected.id());
        assertEquals(actual.sources().size(), expected.sources().size());

        for (int i = 0; i < actual.sources().size(); i++) {
            assertMatchesStructure(actual.sources().get(i), expected.sources().get(i));
        }
    }

    private int getChildGroup(Memo memo, int group) {
        LogicalPlan node = memo.getNode(group);
        GroupReference child = (GroupReference) node.sources().get(0);

        return child.groupId();
    }

    private TestPlan node(LogicalPlanId id, LogicalPlan... children) {
        return new TestPlan(id, List.of(children));
    }

    private TestPlan node(LogicalPlan... children) {
        return node(idAllocator.nextId(), children);
    }

    private static class TestPlan implements LogicalPlan {
        private final List<LogicalPlan> sources;
        private final LogicalPlanId id;

        public TestPlan(LogicalPlanId id, List<LogicalPlan> sources) {
            this.sources = List.copyOf(sources);
            this.id = id;
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
        public List<AbstractTableRelation<?>> baseTables() {
            return null;
        }


        @Override
        public LogicalPlan replaceSources(List<LogicalPlan> sources) {
            return new TestPlan(id(), sources);
        }

        @Override
        public LogicalPlanId id() {
            return id;
        }

        @Override
        public LogicalPlan pruneOutputsExcept(TableStats tableStats, Collection<Symbol> outputsToKeep) {
            return null;
        }

        @Override
        public Map<LogicalPlan, SelectSymbol> dependencies() {
            return null;
        }

        @Override
        public long numExpectedRows() {
            return 0;
        }

        @Override
        public long estimatedRowSize() {
            return 0;
        }

        @Override
        public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
            return visitor.visitPlan(this, context);
        }

        @Override
        public Set<RelationName> getRelationNames() {
            return null;
        }
    }
}

