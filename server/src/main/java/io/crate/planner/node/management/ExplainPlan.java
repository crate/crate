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

package io.crate.planner.node.management;

import static io.crate.data.SentinelRow.SENTINEL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.BoundCopyFrom;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.data.RowN;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Plan;
import io.crate.planner.PlanPrinter;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.PrintContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.symbol.Optimizer;
import io.crate.planner.statement.CopyFromPlan;
import io.crate.types.DataTypes;

public class ExplainPlan implements Plan {

    private static final CastOptimizer CAST_OPTIMIZER = new CastOptimizer();

    public enum Phase {
        Analyze,
        Plan,
        Execute
    }

    private final Plan subPlan;
    private final boolean showCosts;
    private final boolean verbose;
    private final List<OptimizerStep> optimizerSteps;

    public ExplainPlan(Plan subExecutionPlan,
                       boolean showCosts,
                       boolean verbose,
                       List<OptimizerStep> optimizerSteps) {
        this.subPlan = subExecutionPlan;
        this.showCosts = showCosts;
        this.verbose = verbose;
        this.optimizerSteps = optimizerSteps;
    }

    public Plan subPlan() {
        return subPlan;
    }

    public boolean showCosts() {
        return showCosts;
    }

    public boolean verbose() {
        return verbose;
    }

    @Override
    public StatementType type() {
        return StatementType.MANAGEMENT;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        if (subPlan instanceof LogicalPlan logicalPlan) {
            if (verbose) {
                List<RowN> rows = optimizerSteps.stream()
                    .map(step -> {
                        if (step.isInitial()) {
                            return new RowN("Initial logical plan", step.planAsString());
                        }
                        return new RowN(step.rule().sessionSettingName(), step.planAsString());
                    })
                    .collect(Collectors.toCollection(ArrayList::new));
                rows.add(new RowN("Final logical plan", printLogicalPlan(logicalPlan, plannerContext, showCosts)));
                consumer.accept(InMemoryBatchIterator.of(rows, SENTINEL, false), null);
            } else {
                var planAsString = printLogicalPlan(logicalPlan, plannerContext, showCosts);
                consumer.accept(InMemoryBatchIterator.of(new Row1(planAsString), SENTINEL), null);
            }
        } else if (subPlan instanceof CopyFromPlan copyFromPlan && !verbose) {
            BoundCopyFrom boundCopyFrom = CopyFromPlan.bind(
                copyFromPlan.copyFrom(),
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                params,
                subQueryResults);
            ExecutionPlan executionPlan = CopyFromPlan.planCopyFromExecution(
                copyFromPlan.copyFrom(),
                boundCopyFrom,
                dependencies.clusterService().state().nodes(),
                plannerContext);
            Function<String, String> uuidToIndexName = indexUUID -> {
                RelationMetadata relation = dependencies.clusterService().state().metadata().getRelation(indexUUID);
                if (relation != null) {
                    return relation.name().sqlFqn();
                }
                throw new IllegalStateException("RelationMetadata for " + indexUUID + " is not available");
            };
            String planAsJson = DataTypes.STRING.implicitCast(PlanPrinter.objectMap(executionPlan, uuidToIndexName));
            consumer.accept(InMemoryBatchIterator.of(new Row1(planAsJson), SENTINEL), null);
        } else if (verbose) {
            consumer.accept(null,
                new UnsupportedOperationException("EXPLAIN VERBOSE not supported for " + subPlan.getClass().getSimpleName()));
        } else {
            consumer.accept(InMemoryBatchIterator.of(
                new Row1("EXPLAIN not supported for " + subPlan.getClass().getSimpleName()), SENTINEL), null);
        }
    }


    @VisibleForTesting
    public static String printLogicalPlan(LogicalPlan logicalPlan, PlannerContext plannerContext, boolean showCosts) {
        final PrintContext printContext = createPrintContext(plannerContext.planStats(), showCosts);
        var optimizedLogicalPlan = logicalPlan.accept(CAST_OPTIMIZER, plannerContext);
        optimizedLogicalPlan.print(printContext);
        return printContext.toString();
    }

    public static PrintContext createPrintContext(PlanStats planStats, boolean showCosts) {
        if (showCosts) {
            return new PrintContext(planStats);
        } else {
            return new PrintContext(null);
        }
    }


    /**
     * Optimize casts of the {@link io.crate.analyze.WhereClause} query symbol. For executions, this is done while
     * building an {@link ExecutionPlan} to take subquery and parameter symbols into account.
     * See also {@link Collect}.
     */
    private static class CastOptimizer extends LogicalPlanVisitor<PlannerContext, LogicalPlan> {

        @Override
        public LogicalPlan visitPlan(LogicalPlan logicalPlan, PlannerContext context) {
            ArrayList<LogicalPlan> newSources = new ArrayList<>(logicalPlan.sources().size());
            for (var source : logicalPlan.sources()) {
                newSources.add(source.accept(this, context));
            }
            return logicalPlan.replaceSources(newSources);
        }

        @Override
        public LogicalPlan visitCollect(Collect collect, PlannerContext context) {
            var optimizedCollect = new Collect(
                collect.relation(),
                collect.outputs(),
                collect.where().map(s -> Optimizer.optimizeCasts(s, context))
            );
            return visitPlan(optimizedCollect, context);
        }
    }

    record SubQueryResultAndExplain(Object value, Map<String, Object> explainResult) {
    }
}
