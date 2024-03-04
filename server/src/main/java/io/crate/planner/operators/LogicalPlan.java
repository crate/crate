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

package io.crate.planner.operators;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;
import java.util.function.Consumer;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.optimizer.costs.PlanStats;

/**
 * LogicalPlan is a tree of "Operators"
 * This is a representation of the logical order of operators that need to be executed to produce a correct result.
 *
 * {@link #build(PlannerContext, ProjectionBuilder, int, int, OrderBy, Integer, Row, SubQueryResults)}  is used to create the
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
 * {@link #build(PlannerContext, ProjectionBuilder, int, int, OrderBy, Integer, Row, SubQueryResults)}  is called
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

    /**
     * Uses the current shard allocation information to create a physical execution plan.
     * <br />
     * {@code limit}, {@code offset}, {@code order} can be passed from one operator to another. Depending on the
     * operators implementation. Operator may choose to make use of this information, but can also ignore it.
     */
    ExecutionPlan build(DependencyCarrier dependencyCarrier,
                        PlannerContext plannerContext,
                        Set<PlanHint> planHints,
                        ProjectionBuilder projectionBuilder,
                        int limit,
                        int offset,
                        @Nullable OrderBy order,
                        @Nullable Integer pageSizeHint,
                        Row params,
                        SubQueryResults subQueryResults);

    List<Symbol> outputs();

    /**
     * Indicates if the operators which are added on top of this LogicalPlan should operate on a shard level.
     * Operating on a shard level increases parallelism.
     */
    default boolean preferShardProjections() {
        return false;
    }

    default boolean supportsDistributedReads() {
        return false;
    }


    List<LogicalPlan> sources();

    LogicalPlan replaceSources(List<LogicalPlan> sources);

    /**
     * Request an operator to return a new version of itself with all outputs removed except the ones contained in `outputsToKeep`.
     * <p>
     *  Note that `outputsToKeep` can contain scalars on top of the outputs that the "current" operator outputs.
     *  This doesn't mean that the operator has to pull-down the scalar as well, but it means it has to provide all outputs
     *  that are required by the parent. The new pruned outputs should be in the same original order, to avoid errors
     *  regarding {@link Union} (which requires the order to be kept), or introduction of unnecessary {@link Eval} operators.
     *  Using {@link io.crate.expression.symbol.SymbolVisitors#intersection(Symbol, Collection, Consumer)} is an option
     *  To find the outputs required by the parent.
     * </p>
     *
     * Example:
     *
     * <pre>
     *     A: [substr(x, 0, 3), y]
     *     └ B [x, y, z]              // Should provide `x` and `y` after the prune call.
     * </pre>
     *
     * <p>
     *   This must propagate down the tree:
     * </p>
     * <pre>
     *      root       A call to `root.pruneOutputsExcept(..)` must result in calls on all: A, B and C
     *       / \
     *     A   C
     *     |
     *     B
     * </pre>
     * <p>
     *  If there are no outputs to prune and if the source also didn't change, `this` must be returned.
     *  That allows implementations to do a cheap identity check to avoid LogicalPlan re-creations themselves.
     * </p>
     *
     * @param outputsToKeep The collection should provide those outputs in the same original order, to avoid errors
     *                      regarding {@link Union} (which requires the order to be kept), or introduction of
     *                      unnecessary {@link Eval} operators.
     */
    LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep);

    /**
     * Rewrite an operator and its children to utilize a "query-then-fetch" approach.
     * See {@link Fetch} for an explanation of query-then-fetch.
     * <pre>
     * This must propagate if possible. Example:
     *
     *     Limit[10]            // calls source.rewriteToFetch
     *      └ Order [a ASC]     // should call source.rewriteToFetch
     *        └ Collect [x, a, b]
     *
     * This results in:
     *
     *      Fetch[x, a, b]
     *       └ Limit[10]
     *         └ Order [a ASC]
     *           └ Collect [_fetchid, a]
     *
     * Note that propagation only needs to happen if all operators can forward the `_fetchid`. Consider the following:
     *
     *      Limit[10]
     *        └ HashAggregate[min(x), min(y)]
     *          └ Limit[5]
     *            └ Collect [x, y]
     *
     * In this case a call on `HashAggregate.rewriteToFetch` can return `null` to indicate that there is nothing to fetch,
     * because `HashAggregate` needs all columns to produce its result.
     * It is *NOT* responsible to insert a `Fetch` below itself.
     * </pre>
     *
     * @param usedColumns The columns that the ancestor operators use intermediately to produce their result.
     *                    For example, a `Filter (x > 10)` uses `x > 10`, a `Order [a ASC]` uses `a`.
     *                    An operator that uses *all* of its sources outputs to produce a result should return `null`
     */
    @Nullable
    default FetchRewrite rewriteToFetch(Collection<Symbol> usedColumns) {
        return null;
    }

    /**
     * SubQueries that this plan depends on to be able to execute it.
     *
     * valuesBySubQuery in {@link #execute(DependencyCarrier, PlannerContext, RowConsumer, Row, SubQueryResults)}
     * must receive 1 entry per selectSymbol contained in the dependencies here.
     *
     * Note that currently {@link MultiPhase} is injected into the operator-tree to declare the dependencies.
     * It's not necessary for each operator to expose it's own SelectSymbols; propagation is usually sufficient.
     */
    Map<LogicalPlan, SelectSymbol> dependencies();

    @Override
    default void executeOrFail(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               RowConsumer consumer,
                               Row params,
                               SubQueryResults subQueryResults) throws Exception {
        LogicalPlanner.execute(this, executor, plannerContext, consumer, params, subQueryResults, false);
    }

    <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context);

    default StatementType type() {
        return StatementType.SELECT;
    }

    /**
     *
     * @return RelationNames of the sources in order from left to right without duplicates
     */
    List<RelationName> getRelationNames();

    default void print(PrintContext printContext) {
        printContext
            .text(getClass().getSimpleName())
            .text("[")
            .text(Lists.joinOn(", ", outputs(), Symbol::toString))
            .text("]");
        printStats(printContext);
        printContext.nest(Lists.map(sources(), x -> x::print));
    }

    default void printStats(PrintContext printContext) {
        PlanStats planStats = printContext.planStats();
        if (planStats != null) {
            var stats = planStats.get(this);
            if (stats.numDocs() == -1) {
                printContext.text(" (rows=unknown)");
            } else {
                printContext.text(" (rows=" + stats.numDocs() + ")");
            }
        }
    }
}
