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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;
import static io.crate.testing.Asserts.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.Insert;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.Order;
import io.crate.planner.operators.TableFunction;
import io.crate.planner.operators.Union;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.DefaultMatcher;
import io.crate.planner.optimizer.matcher.GroupReferencedMatcher;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.planner.optimizer.memo.GroupReference;
import io.crate.planner.optimizer.memo.GroupReferenceResolver;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class MergeFiltersTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions e;
    private AbstractTableRelation<?> tr1;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Map<RelationName, AnalyzedRelation> sources = T3.sources(clusterService);
        e = new SqlExpressions(sources);
        tr1 = (AbstractTableRelation<?>) sources.get(T3.T1);
    }

    @Test
    public void testMergeFiltersMatchesOnAFilterWithAnotherFilterAsChild() {
        Collect source = new Collect(tr1, Collections.emptyList(), WhereClause.MATCH_ALL, 100, 10);
        Filter sourceFilter = new Filter(source, e.asSymbol("x > 10"));
        Filter parentFilter = new Filter(sourceFilter, e.asSymbol("y > 10"));

        MergeFilters mergeFilters = new MergeFilters();
        Match<Filter> match = mergeFilters.pattern().accept(DefaultMatcher.DEFAULT_MATCHER,
                                                            parentFilter,
                                                            Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isSameAs(parentFilter);

        Filter mergedFilter = (Filter) mergeFilters.apply(match.value(),
                                                          match.captures(),
                                                          new TableStats(),
                                                          CoordinatorTxnCtx.systemTransactionContext(),
                                                          e.nodeCtx);
        assertThat(mergedFilter.query()).isSQL("((doc.t2.y > 10) AND (doc.t1.x > 10))");
    }

    @Test
    public void test_matching_no_match() {
        Collect source = new Collect(tr1, Collections.emptyList(), WhereClause.MATCH_ALL, 100, 10);
        LogicalPlan groupReference = new GroupReference(1, source.outputs());
        Order order = new Order(groupReference, new OrderBy(List.of()));

        GroupReferenceResolver groupReferenceResolver = node -> {
            if (node instanceof GroupReference) {
                return source;
            }
            return node;
        };

        MergeFilters mergeFilters = new MergeFilters();
        Match<Filter> match = new GroupReferencedMatcher(groupReferenceResolver).match(mergeFilters.pattern(), order, Captures.empty());
        assertThat(match.isPresent()).isFalse();
    }

    @Test
    public void test_matching_match() {
        Collect source = new Collect(tr1, Collections.emptyList(), WhereClause.MATCH_ALL, 100, 10);
        Filter sourceFilter = new Filter(source, e.asSymbol("x > 10"));
        LogicalPlan groupReference = new GroupReference(1, sourceFilter.outputs());
        Order order = new Order(groupReference, new OrderBy(List.of()));

        Capture<Filter> child = new Capture<>();
        Pattern<Filter> pattern = typeOf(Filter.class).with(source(), typeOf(Filter.class).capturedAs(child));

        GroupReferenceResolver groupReferenceResolver = node -> {
            if (node instanceof GroupReference) {
                return sourceFilter;
            }
            return node;
        };

        MergeFilters mergeFilters = new MergeFilters();
        Match<Filter> match = new GroupReferencedMatcher(groupReferenceResolver).match(mergeFilters.pattern(),
                                                                                       order,
                                                                                       Captures.empty());
        assertThat(match.isPresent()).isTrue();
    }

    @Test
    public void test_merging_union() {
        Collect source1 = new Collect(tr1, Collections.emptyList(), WhereClause.MATCH_ALL, 100, 10);
        Collect source2 = new Collect(tr1, Collections.emptyList(), WhereClause.MATCH_ALL, 100, 10);
        LogicalPlan groupReferenceSource1 = new GroupReference(1, source1.outputs());
        LogicalPlan groupReferenceSource2 = new GroupReference(2, source2.outputs());
        Union union = new Union(groupReferenceSource1, groupReferenceSource2, groupReferenceSource1.outputs());
        LogicalPlan groupReference3 = new GroupReference(3, union.outputs());
        Order order = new Order(groupReference3, new OrderBy(List.of()));

        GroupReferenceResolver groupReferenceResolver = node -> {
            if (node instanceof GroupReference g) {
                switch(g.groupId()) {
                    case 1 -> {
                        return source1;
                    }
                    case 2 -> {
                        return source2;
                    }
                    case 3 -> {
                        return union;
                    }
                }
            }
            return node;
        };
        MoveOrderBeneathUnion moveFilterBeneathUnion = new MoveOrderBeneathUnion();
        Match<Order> match = new GroupReferencedMatcher(groupReferenceResolver).match(moveFilterBeneathUnion.pattern(), order, Captures.empty());
        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isInstanceOf(Order.class);
        assertThat(match.captures().get(moveFilterBeneathUnion.unionCapture())).isInstanceOf(Union.class);
    }

    @Test
    public void test_merging_union_default() {
        Collect source1 = new Collect(tr1, Collections.emptyList(), WhereClause.MATCH_ALL, 100, 10);
        Collect source2 = new Collect(tr1, Collections.emptyList(), WhereClause.MATCH_ALL, 100, 10);
        Union union = new Union(source1, source2, source1.outputs());
        Order order = new Order(union, new OrderBy(List.of()));

        Capture<Union> unionCapture = new Capture<>();
        Pattern<Order> pattern = typeOf(Order.class).with(source(), typeOf(Union.class).capturedAs(unionCapture));

        Match<Order> match = new DefaultMatcher().match(pattern, order, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isInstanceOf(Order.class);
        assertThat(match.captures().get(unionCapture)).isInstanceOf(Union.class);
    }

    @Test
    public void test_merging_union_group_referenced() {

        Collect source1 = new Collect(tr1, Collections.emptyList(), WhereClause.MATCH_ALL, 100, 10);
        Collect source2 = new Collect(tr1, Collections.emptyList(), WhereClause.MATCH_ALL, 100, 10);
        LogicalPlan groupReferenceSource1 = new GroupReference(1, source1.outputs());
        LogicalPlan groupReferenceSource2 = new GroupReference(2, source2.outputs());
        Union union = new Union(groupReferenceSource1, groupReferenceSource2, groupReferenceSource1.outputs());
        LogicalPlan groupReference3 = new GroupReference(3, union.outputs());
        Order order = new Order(groupReference3, new OrderBy(List.of()));

        var memo = new HashMap<Integer, LogicalPlan>();
        memo.put(1, source1);
        memo.put(2, source2);
        memo.put(3, union);

        GroupReferenceResolver groupReferenceResolver = node -> {
            if (node instanceof GroupReference g) {
               return memo.get(g.groupId());
            }
            return node;
        };

        Capture<Union> unionCapture = new Capture<>();
        Pattern<Order> pattern = typeOf(Order.class).with(source(), typeOf(Union.class).capturedAs(unionCapture));

        Match<Order> match = new GroupReferencedMatcher(groupReferenceResolver).match(pattern, order, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isInstanceOf(Order.class);
        assertThat(match.captures().get(unionCapture)).isInstanceOf(Union.class);
    }

    public void test_insert() {
        var tableFunction = new TableFunction(mock(TableFunctionRelation.class), List.of(), new WhereClause(null));
        Insert insert = new Insert(tableFunction, mock(ColumnIndexWriterProjection.class), null);

        Capture<TableFunction> capture = new Capture<>();
        Pattern<Insert> pattern = typeOf(Insert.class).with(source(), typeOf(TableFunction.class).capturedAs(capture));

        Match<Insert> match = new DefaultMatcher().match(pattern, insert, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isInstanceOf(Insert.class);
        Assertions.assertThat(match.captures().get(capture)).isInstanceOf(TableFunction.class);

    }
}
