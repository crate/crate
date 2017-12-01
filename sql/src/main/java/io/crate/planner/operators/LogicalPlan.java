/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.operators;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.TableStats;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LogicalPlan is a tree of "Operators"
 * This is a representation of the logical order of operators that need to be executed to produce a correct result.
 *
 * {@link #build(PlannerContext, ProjectionBuilder, int, int, OrderBy, Integer)} is used to create the
 * actual "physical" execution plan.
 *
 * A Operator is something like Limit, OrderBy, HashAggregate, Join, Union, Collect
 * <pre>
 *     select x, y, z from t1 where x = 10 order by x limit 10:
 *
 *     Limit 10
 *        |
 *     Order By x
 *         |
 *     Collect [x, y, z]
 * </pre>
 *
 * {@link #build(PlannerContext, ProjectionBuilder, int, int, OrderBy, Integer)} is called
 * on the "root" and flows down.
 * Each time each operator may provide "hints" to the children so that they can decide to eagerly apply parts of the
 * operations
 *
 * This allows us to create execution plans as follows::
 *
 * <pre>
 *     select x, y, z from t1 where order by x limit 10;
 *
 *
 *          Merge
 *         /  limit10
 *       /         \
 *     Collect     Collect
 *     limit 10    limit 10
 *
 * </pre>
 */
public interface LogicalPlan extends Plan {

    interface Builder {

        /**
         * Create a LogicalPlan node
         *
         * @param usedBeforeNextFetch The columns the "parent" is using.
         *                    This is used to create plans which utilize query-then-fetch.
         *                    For example:
         *                    <pre>
         *                       select a, b, c from t1 order by a limit 10
         *
         *                       EvalFetch (usedColumns: [a, b, c])
         *                         outputs: [a, b, c]   (b, c resolved using _fetch)
         *                         |
         *                       Limit 10 (usedColumns: [])
         *                         |
         *                       Order (usedColumns: [a]
         *                         |
         *                       Collect (usedColumns: [a] - inherited from Order)
         *                         outputs: [_fetch, a]
         *                    </pre>
         */
        LogicalPlan build(TableStats tableStats, Set<Symbol> usedBeforeNextFetch);
    }

    /**
     * Tries to "push-down" a LogicalPlan to this plan or the children of
     * this plan. A push-down is an optimization which does not change the
     * outcome of the plan but, ideally, makes it execute more efficiently.
     *
     * For example, pushing down an *Order*:
     *
     *              *Order*                      Union
     *                 |                        /     \
     *                 |                       /       \
     *               Union                 *Order*  *Order*
     *              /     \                   |        |
     *             /       \        =>        |        |
     *          Collect   Order             Collect  Order
     *                      |                          |
     *                      |                          |
     *                   Collect                    Collect
     *
     * @param pushDown The LogicalPLan which gets "pushed down". {@code null} if currently
     *                 no plan gets pushed. Still can recurse to find other push down candidates.
     * @return A new LogicalPlan or the same plan if rewriting/optimizing is not possible.
     */
    LogicalPlan tryPushDown(@Nullable LogicalPlan pushDown);

    /**
     * Uses the current shard allocation information to create a physical execution plan.
     * <br />
     * {@code limit}, {@code offset}, {@code order} can be passed from one operator to another. Depending on the
     * operators implementation. Operator may choose to make use of this information, but can also ignore it.
     */
    ExecutionPlan build(PlannerContext plannerContext,
                        ProjectionBuilder projectionBuilder,
                        int limit,
                        int offset,
                        @Nullable OrderBy order,
                        @Nullable Integer pageSizeHint,
                        Row params,
                        Map<SelectSymbol, Object> subQueryValues);

    /**
     * Used to generate optimized operators.
     * E.g. Aggregate(count(*)) + Collect -> Count
     */
    LogicalPlan tryCollapse();

    List<Symbol> outputs();

    /**
     * Indicates if the operators which are added on top of this LogicalPlan should operate on a shard level.
     * Operating on a shard level increases parallelism.
     */
    default boolean preferShardProjections() {
        return false;
    }

    /**
     * A mapping from from symbol to symbol.
     * This is used across relation boundaries to map parent expression to source expression
     *
     * Example:
     * <pre>
     *     select tt.bb from
     *          (select t.b + t.b as bb from t) tt
     *
     * expressionMapping
     *      tt.bb -> t.b + t.b
     * </pre>
     */
    Map<Symbol, Symbol> expressionMapping();

    List<AbstractTableRelation> baseTables();

    /**
     * SubQueries that this plan depends on to be able to execute it.
     *
     * valuesBySubQuery in {@link #execute(DependencyCarrier, PlannerContext, RowConsumer, Row, Map)}
     * must receive 1 entry per selectSymbol contained in the dependencies here.
     *
     * Note that currently {@link MultiPhase} is injected into the operator-tree to declare the dependencies.
     * It's not necessary for each operator to expose it's own SelectSymbols; propagation is usually sufficient.
     */
    Map<LogicalPlan, SelectSymbol> dependencies();

    long numExpectedRows();

    @Override
    default void execute(DependencyCarrier executor,
                         PlannerContext plannerContext,
                         RowConsumer consumer,
                         Row params,
                         Map<SelectSymbol, Object> valuesBySubQuery) {
        LogicalPlanner.execute(this, executor, plannerContext, consumer, params, valuesBySubQuery);
    }
}
