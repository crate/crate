/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.consumer;

import io.crate.analyze.Analyzer;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.projectors.TopN;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.*;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class CrossJoinConsumerTest {

    private Analyzer analyzer;
    private Planner planner;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        Injector injector = new ModulesBuilder()
                .add(PlannerTest.plannerTestModule())
                .add(new AggregationImplModule())
                .add(new ScalarFunctionModule())
                .add(new PredicateModule())
                .add(new OperatorModule())
                .createInjector();
        analyzer = injector.getInstance(Analyzer.class);
        planner = injector.getInstance(Planner.class);
    }

    private IterablePlan plan(String statement) {
        Plan plan = planner.plan(analyzer.analyze(SqlParser.createStatement(statement)));
        PlanPrinter planPrinter = new PlanPrinter();
        System.out.println(planPrinter.print(plan));
        return (IterablePlan) plan;
    }

    @Test
    public void testExplicitCrossJoinWithoutLimitOrOrderBy() throws Exception {
        IterablePlan plan = plan("select * from users cross join parted");

        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode next = iterator.next();
        assertThat(next, instanceOf(NestedLoopNode.class));

        NestedLoopNode nestedLoopNode = (NestedLoopNode) next;
        assertThat(nestedLoopNode.projections().isEmpty(), is(true));
        assertThat(nestedLoopNode.limit(), is(TopN.NO_LIMIT));
        assertThat(nestedLoopNode.offset(), is(0));
        assertThat(nestedLoopNode.outputTypes().size(), is(8));

        PlanNode left = nestedLoopNode.left();
        assertThat(left, instanceOf(QueryThenFetchNode.class));
        QueryThenFetchNode leftQtf = (QueryThenFetchNode)left;

        PlanNode right = nestedLoopNode.right();
        assertThat(right, instanceOf(QueryThenFetchNode.class));
        QueryThenFetchNode rightQtf = (QueryThenFetchNode)right;

        // what's left and right changes per test run... so just make sure the outputs are different
        assertNotEquals(leftQtf.outputs().size(), rightQtf.outputs().size());
    }

    @Test
    public void testExplicitCrossJoinWith3Tables() throws Exception {
        IterablePlan plan = plan("select * from users u1 cross join users u2 cross join users u3");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        assertThat(nl.outputTypes().size(), is(15));
    }

    @Test
    public void testCrossJoinTwoTablesWithLimit() throws Exception {
        IterablePlan plan = plan("select * from users u1, users u2 limit 2");
        PlanNode planNode = plan.iterator().next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        assertThat(nl.limit(), is(2));

        assertThat(((QueryThenFetchNode) nl.left()).limit(), is(2));
        assertThat(((QueryThenFetchNode) nl.right()).limit(), is(2));

        assertThat(nl.projections().size(), is(1));
        assertThat(nl.projections().get(0), instanceOf(TopNProjection.class));
    }

    @Test
    public void testCrossJoinWithTwoColumnsAndAddSubtractInResultColumns() throws Exception {
        IterablePlan plan = plan("select t1.id, t2.id, t1.id + cast(t2.id as integer) from users t1, characters t2");

        PlanNode planNode = plan.iterator().next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        QueryThenFetchNode left = (QueryThenFetchNode) nl.left();
        QueryThenFetchNode right = (QueryThenFetchNode) nl.right();

        TopNProjection topNProjection = (TopNProjection) nl.projections().get(0);
        InputColumn inputCol1 = (InputColumn) topNProjection.outputs().get(0);
        InputColumn inputCol2 = (InputColumn) topNProjection.outputs().get(1);
        Function add = (Function) topNProjection.outputs().get(2);

        assertThat((InputColumn) add.arguments().get(0), equalTo(inputCol1));
        assertThat(((Function) add.arguments().get(1)).info().ident().name(), equalTo("toInt"));

        List<Symbol> allOutputs = new ArrayList<>(left.outputs());
        allOutputs.addAll(right.outputs());

        Reference ref1 = (Reference) allOutputs.get(inputCol1.index());
        assertThat(ref1.ident().columnIdent().name(), is("id"));
        assertThat(ref1.ident().tableIdent().name(), is("users"));

        Reference ref2 = (Reference) allOutputs.get(inputCol2.index());
        assertThat(ref2.ident().columnIdent().name(), is("id"));
        assertThat(ref2.ident().tableIdent().name(), is("characters"));
    }
}