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

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.Analyzer;
import io.crate.analyze.WhereClause;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.operator.OrOperator;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.projectors.TopN;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.scalar.arithmetic.AddFunction;
import io.crate.planner.*;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.dql.ESGetNode;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.crate.testing.TestingHelpers.isFunction;
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
    public void testWhereWithNoMatchShouldReturnNoopPlan() throws Exception {
        Plan plan = planner.plan(analyzer.analyze(SqlParser.createStatement("select * from users u1, users u2 where 1 = 2")));
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testExplicitCrossJoinWithoutLimitOrOrderBy() throws Exception {
        IterablePlan plan = plan("select * from users cross join parted");

        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode next = iterator.next();
        assertThat(next, instanceOf(NestedLoopNode.class));

        NestedLoopNode nestedLoopNode = (NestedLoopNode) next;
        assertThat(nestedLoopNode.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
        assertThat(nestedLoopNode.offset(), is(0));
        assertThat(nestedLoopNode.outputTypes().size(), is(10));

        Plan left = nestedLoopNode.left();
        Plan right = nestedLoopNode.right();

        // what's left and right changes per test run... so just make sure the outputs are different
        assertNotEquals(left.outputTypes(), right.outputTypes());
    }

    @Test
    public void testCrossJoinWithJoinCriteriaInOrderBy() throws Exception {
        IterablePlan plan = plan("select u1.id + u2.id, u1.name from users u1, users u2 order by 1");
        NestedLoopNode nl = (NestedLoopNode) plan.iterator().next();
        TopNProjection topNProjection = (TopNProjection) nl.projections().get(0);

        assertThat(topNProjection.isOrdered(), is(true));
        Symbol orderBy = topNProjection.orderBy().get(0);
        assertThat(orderBy, isFunction(AddFunction.NAME));
        Function function = (Function) orderBy;

        for (Symbol arg : function.arguments()) {
            assertThat(arg, instanceOf(InputColumn.class));
        }
    }

    @Test
    public void testOrderOfColumnsInOutputIsCorrect() throws Exception {
        IterablePlan plan = plan("select t1.name, t2.name, t1.id from users t1 cross join characters t2");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        IterablePlan leftPlan = (IterablePlan) nl.left();
        IterablePlan rightPlan = (IterablePlan) nl.right();

        QueryThenFetchNode left = (QueryThenFetchNode)leftPlan.iterator().next();
        QueryThenFetchNode right = (QueryThenFetchNode)rightPlan.iterator().next();

        List<Symbol> leftAndRightOutputs = Lists.newArrayList(FluentIterable.from(left.outputs()).append(right.outputs()));

        TopNProjection topNProjection = (TopNProjection) nl.projections().get(0);
        InputColumn in1 = (InputColumn) topNProjection.outputs().get(0);
        InputColumn in2 = (InputColumn) topNProjection.outputs().get(1);
        InputColumn in3 = (InputColumn) topNProjection.outputs().get(2);

        Reference t1Name = (Reference) leftAndRightOutputs.get(in1.index());
        assertThat(t1Name.ident().columnIdent().name(), is("name"));
        assertThat(t1Name.ident().tableIdent().name(), is("users"));

        Reference t2Name = (Reference) leftAndRightOutputs.get(in2.index());
        assertThat(t2Name.ident().columnIdent().name(), is("name"));
        assertThat(t2Name.ident().tableIdent().name(), is("characters"));

        Reference t1Id = (Reference) leftAndRightOutputs.get(in3.index());
        assertThat(t1Id.ident().columnIdent().name(), is("id"));
        assertThat(t1Id.ident().tableIdent().name(), is("users"));
    }

    @Test
    public void testExplicitCrossJoinWith3Tables() throws Exception {
        IterablePlan plan = plan("select u1.name, u2.name, u3.name from users u1 cross join users u2 cross join users u3");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;
        assertThat(nl.limit(), is(Constants.DEFAULT_SELECT_LIMIT));

        assertThat(nl.outputTypes().size(), is(3));
        for (DataType dataType : nl.outputTypes()) {
            assertThat(dataType, equalTo((DataType) DataTypes.STRING));
        }
        assertThat(nl.left().outputTypes().size() + nl.right().outputTypes().size(), is(3));

        PlanNode left = ((IterablePlan)nl.left()).iterator().next();
        NestedLoopNode nested;
        if (left instanceof NestedLoopNode) {
            nested = (NestedLoopNode)left;
        } else {
            nested = (NestedLoopNode)((IterablePlan) nl.right()).iterator().next();
        }
        assertThat(nested.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
        assertThat(nested.offset(), is(0));
    }

    @Test
    public void testCrossJoinTwoTablesWithLimit() throws Exception {
        IterablePlan plan = plan("select * from users u1, users u2 limit 2");
        PlanNode planNode = plan.iterator().next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        assertThat(nl.limit(), is(2));

        IterablePlan leftPlan = (IterablePlan) nl.left();
        IterablePlan rightPlan = (IterablePlan) nl.right();

        QueryThenFetchNode left = (QueryThenFetchNode)leftPlan.iterator().next();
        QueryThenFetchNode right = (QueryThenFetchNode)rightPlan.iterator().next();

        assertThat(left.limit(), is(2));
        assertThat(right.limit(), is(2));

        assertThat(nl.projections().size(), is(1));
        assertThat(nl.projections().get(0), instanceOf(TopNProjection.class));
    }

    @Test
    public void testAddLiteralIsEvaluatedEarlyInQTF() throws Exception {
        IterablePlan plan = plan("select t1.id * (t2.id + 2) from users t1, users t2 limit 1");
        PlanNode planNode = plan.iterator().next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        TopNProjection topN = (TopNProjection) nl.projections().get(0);

        Function multiply = (Function) topN.outputs().get(0);
        for (Symbol symbol : multiply.arguments()) {
            assertThat(symbol, instanceOf(InputColumn.class));
        }
    }

    @Test
    public void testCrossJoinWithTwoColumnsAndAddSubtractInResultColumns() throws Exception {
        IterablePlan plan = plan("select t1.id, t2.id, t1.id + cast(t2.id as integer) from users t1, characters t2");

        PlanNode planNode = plan.iterator().next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        IterablePlan leftPlan = (IterablePlan) nl.left();
        IterablePlan rightPlan = (IterablePlan) nl.right();

        QueryThenFetchNode left = (QueryThenFetchNode)leftPlan.iterator().next();
        QueryThenFetchNode right = (QueryThenFetchNode)rightPlan.iterator().next();

        // t1 outputs: [ id ]
        // t2 outputs: [ id, cast(id as int) ]

        TopNProjection topNProjection = (TopNProjection) nl.projections().get(0);
        InputColumn inputCol1 = (InputColumn) topNProjection.outputs().get(0);
        InputColumn inputCol2 = (InputColumn) topNProjection.outputs().get(1);
        Function add = (Function) topNProjection.outputs().get(2);

        assertThat((InputColumn) add.arguments().get(0), equalTo(inputCol1));

        InputColumn inputCol3 = (InputColumn) add.arguments().get(1);

        // topN projection outputs: [ {point to t1.id}, {point to t2.id}, add( {point to t2.id}, {point to cast(t2.id) }]
        List<Symbol> allOutputs = new ArrayList<>(left.outputs());
        allOutputs.addAll(right.outputs());

        Reference ref1 = (Reference) allOutputs.get(inputCol1.index());
        assertThat(ref1.ident().columnIdent().name(), is("id"));
        assertThat(ref1.ident().tableIdent().name(), is("users"));

        Reference ref2 = (Reference) allOutputs.get(inputCol2.index());
        assertThat(ref2.ident().columnIdent().name(), is("id"));
        assertThat(ref2.ident().tableIdent().name(), is("characters"));

        Symbol castFunction = allOutputs.get(inputCol3.index());
        assertThat(castFunction, isFunction("toInt"));
    }

    @Test
    public void testCrossJoinsWithSubscript() throws Exception {
        IterablePlan plan = plan("select address['street'], details['no_such_column'] from users cross join ignored_nested");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        assertThat(nl.outputTypes().size(), is(2));
        assertThat(nl.outputTypes().get(0).id(), is(DataTypes.STRING.id()));
        assertThat(nl.outputTypes().get(1).id(), is(DataTypes.UNDEFINED.id()));
    }

    @Test
    public void testCrossJoinWithWhere() throws Exception {
        IterablePlan plan = plan("select * from users t1 cross join users t2 where (t1.id = 1 or t1.id = 2) and (t2.id = 3 or t2.id = 4)");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        ESGetNode left = (ESGetNode) ((IterablePlan) nl.left()).iterator().next();
        ESGetNode right = (ESGetNode) ((IterablePlan) nl.right()).iterator().next();
        if (left.index().equals("t1")) {
            assertThat(left.ids(), containsInAnyOrder("1", "2"));
            assertThat(right.ids(), containsInAnyOrder("3", "4"));
        } else {
            assertThat(left.ids(), containsInAnyOrder("3", "4"));
            assertThat(right.ids(), containsInAnyOrder("1", "2"));
        }
    }

    @Test
    public void testCrossJoinWhereWithJoinCondition() throws Exception {
        IterablePlan plan = plan("select * from users t1 cross join users t2 where t1.id = 1 or t2.id = 2");
        NestedLoopNode nl = (NestedLoopNode) plan.iterator().next();
        assertThat(nl.limit(), is(Constants.DEFAULT_SELECT_LIMIT));

        Projection projection = nl.projections().get(0);
        assertThat(projection, instanceOf(FilterProjection.class));
        Symbol query = ((FilterProjection) projection).query();
        assertThat(query, isFunction(OrOperator.NAME));
    }

    @Test
    public void testCrossJoinWhereSingleBooleanField() throws Exception {
        IterablePlan plan = plan("select * from users t1 cross join users t2 where t1.is_awesome");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        IterablePlan leftPlan = (IterablePlan) nl.left();
        IterablePlan rightPlan = (IterablePlan) nl.right();

        QueryThenFetchNode left = (QueryThenFetchNode)leftPlan.iterator().next();
        QueryThenFetchNode right = (QueryThenFetchNode)rightPlan.iterator().next();

        WhereClause leftWhereClause = left.whereClause();
        WhereClause rightWhereClause = right.whereClause();
        // left and right isn't deterministic... but one needs to have a query and the other shouldn't have one
        if (leftWhereClause.hasQuery()) {
            assertThat(rightWhereClause.hasQuery(), is(false));
        } else {
            assertThat(rightWhereClause.hasQuery(), is(true));
        }
    }

    @Test
    public void testCrossJoinThreeTablesWithLimitAndOffset() throws Exception {
        IterablePlan plan = plan("select * from users t1 cross join users t2 cross join users t3 limit 10 offset 2");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        assertThat(nl.limit(), is(10));
        assertThat(nl.offset(), is(2));

        IterablePlan leftPlan = (IterablePlan) nl.left();
        IterablePlan rightPlan = (IterablePlan) nl.right();

        NestedLoopNode nested;
        if (leftPlan.iterator().next() instanceof NestedLoopNode) {
            nested = (NestedLoopNode) leftPlan.iterator().next();
        } else {
            nested = (NestedLoopNode) rightPlan.iterator().next();
        }
        assertThat(nested.limit(), is(12));
        assertThat(nested.offset(), is(0));
    }

    @Test
    public void testCrossJoinThreeTablesWithJoinConditionInOrderBy() throws Exception {
        IterablePlan plan = plan("select * from users t1 cross join users t2 cross join users t3 order by t3.id + t2.id limit 10 offset 2");
        Iterator<PlanNode> iterator = plan.iterator();
        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(NestedLoopNode.class));
        NestedLoopNode nl = (NestedLoopNode) planNode;

        assertThat(nl.limit(), is(10));
        assertThat(nl.offset(), is(2));

        IterablePlan leftPlan = (IterablePlan) nl.left();
        IterablePlan rightPlan = (IterablePlan) nl.right();

        NestedLoopNode nested;
        if (leftPlan.iterator().next() instanceof NestedLoopNode) {
            nested = (NestedLoopNode) leftPlan.iterator().next();
        } else {
            nested = (NestedLoopNode) rightPlan.iterator().next();
        }
        assertThat(nested.limit(), is(TopN.NO_LIMIT));
        assertThat(nested.offset(), is(0));
    }
}