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

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

import static io.crate.planner.operators.LogicalPlanner.NO_LIMIT;

public class GroupHashAggregate extends ForwardingLogicalPlan {

    private static final String DISTRIBUTED_MERGE_PHASE_NAME = "distributed merge";
    final List<Function> aggregates;
    final List<Symbol> groupKeys;
    private final List<Symbol> outputs;
    private final long numExpectedRows;


    static long approximateDistinctValues(long numSourceRows, TableStats tableStats, List<Symbol> groupKeys) {
        long distinctValues = 1;
        int numKeysWithStats = 0;
        for (Symbol groupKey : groupKeys) {

            Stats stats = null;
            ColumnStats columnStats = null;
            if (groupKey instanceof Reference) {
                Reference ref = (Reference) groupKey;
                stats = tableStats.getStats(ref.ident().tableIdent());
                columnStats = stats.statsByColumn().get(ref.column());
                numKeysWithStats++;
            } else if (groupKey instanceof ScopedSymbol) {
                ScopedSymbol scopedSymbol = (ScopedSymbol) groupKey;
                stats = tableStats.getStats(scopedSymbol.relation());
                columnStats = stats.statsByColumn().get(scopedSymbol.column());
                numKeysWithStats++;
            }

            if (columnStats == null) {
                // Assume worst case: Every value is unique
                distinctValues *= numSourceRows;
            } else {
                // `approxDistinct` is the number of distinct values in relation to `stats.numDocs()´, not in
                // relation to `numSourceRows`, which is based on the estimates of a source operator.
                // That is why we calculate the cardinality ratio and calculate the new distinct
                // values based on `numSourceRows` to account for changes in the number of rows in source operators
                //
                // e.g. SELECT x, count(*) FROM tbl GROUP BY x
                // and  SELECT x, count(*) FROM tbl WHERE pk = 1 GROUP BY x
                //
                // have a different number of groups
                double cardinalityRatio = columnStats.approxDistinct() / stats.numDocs();
                distinctValues *= (long) (numSourceRows * cardinalityRatio);
            }
        }
        if (numKeysWithStats == groupKeys.size()) {
            return Math.min(distinctValues, numSourceRows);
        } else {
            return numSourceRows;
        }
    }

    public GroupHashAggregate(LogicalPlan source, List<Symbol> groupKeys, List<Function> aggregates, long numExpectedRows) {
        super(source);
        this.numExpectedRows = numExpectedRows;
        this.aggregates = List.copyOf(new LinkedHashSet<>(aggregates));
        this.outputs = Lists2.concat(groupKeys, this.aggregates);
        this.groupKeys = groupKeys;
    }

    @Override
    public long numExpectedRows() {
        return numExpectedRows;
    }

    public List<Function> aggregates() {
        return aggregates;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        ExecutionPlan executionPlan = source.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, null, params, subQueryResults);
        if (executionPlan.resultDescription().hasRemainingLimitOrOffset()) {
            executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);
        }
        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(params, subQueryResults);

        List<Symbol> sourceOutputs = source.outputs();
        if (shardsContainAllGroupKeyValues()) {
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                sourceOutputs,
                groupKeys,
                aggregates,
                paramBinder,
                AggregateMode.ITER_FINAL,
                source.preferShardProjections() ? RowGranularity.SHARD : RowGranularity.CLUSTER,
                plannerContext.transactionContext().sessionContext().searchPath()
            );
            executionPlan.addProjection(groupProjection, TopN.NO_LIMIT, 0, null);
            return executionPlan;
        }

        if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), executionPlan.resultDescription().nodeIds())) {
            if (source.preferShardProjections()) {
                executionPlan.addProjection(
                    projectionBuilder.groupProjection(
                        sourceOutputs,
                        groupKeys,
                        aggregates,
                        paramBinder,
                        AggregateMode.ITER_PARTIAL,
                        RowGranularity.SHARD,
                        plannerContext.transactionContext().sessionContext().searchPath()
                    )
                );
                executionPlan.addProjection(
                    projectionBuilder.groupProjection(
                        outputs,
                        groupKeys,
                        aggregates,
                        paramBinder,
                        AggregateMode.PARTIAL_FINAL,
                        RowGranularity.NODE,
                        plannerContext.transactionContext().sessionContext().searchPath()
                    ),
                    TopN.NO_LIMIT,
                    0,
                    null
                );
                return executionPlan;
            } else {
                executionPlan.addProjection(
                    projectionBuilder.groupProjection(
                        sourceOutputs,
                        groupKeys,
                        aggregates,
                        paramBinder,
                        AggregateMode.ITER_FINAL,
                        RowGranularity.NODE,
                        plannerContext.transactionContext().sessionContext().searchPath()
                    ),
                    TopN.NO_LIMIT,
                    0,
                    null
                );
                return executionPlan;
            }
        }

        GroupProjection toPartial = projectionBuilder.groupProjection(
            sourceOutputs,
            groupKeys,
            aggregates,
            paramBinder,
            AggregateMode.ITER_PARTIAL,
            source.preferShardProjections() ? RowGranularity.SHARD : RowGranularity.NODE,
            plannerContext.transactionContext().sessionContext().searchPath()
        );
        executionPlan.addProjection(toPartial);
        executionPlan.setDistributionInfo(DistributionInfo.DEFAULT_MODULO);

        GroupProjection toFinal = projectionBuilder.groupProjection(
            outputs,
            groupKeys,
            aggregates,
            paramBinder,
            AggregateMode.PARTIAL_FINAL,
            RowGranularity.CLUSTER,
            plannerContext.transactionContext().sessionContext().searchPath()
        );
        return createMerge(
            plannerContext,
            executionPlan,
            Collections.singletonList(toFinal),
            executionPlan.resultDescription().nodeIds()
        );
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(TableStats tableStats, Collection<Symbol> outputsToKeep) {
        HashSet<Symbol> toKeep = new HashSet<>();
        // We cannot prune groupKeys, even if they are not used in the outputs, because it would change the result semantically
        for (Symbol groupKey : groupKeys) {
            SymbolVisitors.intersection(groupKey, source.outputs(), toKeep::add);
        }
        ArrayList<Function> newAggregates = new ArrayList<>();
        for (Symbol outputToKeep : outputsToKeep) {
            SymbolVisitors.intersection(outputToKeep, aggregates, newAggregates::add);
        }
        for (Function newAggregate : newAggregates) {
            SymbolVisitors.intersection(newAggregate, source.outputs(), toKeep::add);
        }
        LogicalPlan newSource = source.pruneOutputsExcept(tableStats, toKeep);
        if (newSource == source && aggregates.size() == newAggregates.size()) {
            return this;
        }
        return new GroupHashAggregate(newSource, groupKeys, newAggregates, numExpectedRows);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new GroupHashAggregate(Lists2.getOnlyElement(sources), groupKeys, aggregates, numExpectedRows);
    }

    private ExecutionPlan createMerge(PlannerContext plannerContext,
                                      ExecutionPlan executionPlan,
                                      List<Projection> projections,
                                      Collection<String> nodeIds) {
        return new Merge(
            executionPlan,
            new MergePhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                DISTRIBUTED_MERGE_PHASE_NAME,
                executionPlan.resultDescription().nodeIds().size(),
                1,
                nodeIds,
                executionPlan.resultDescription().streamOutputs(),
                projections,
                DistributionInfo.DEFAULT_BROADCAST,
                null
            ),
            TopN.NO_LIMIT,
            TopN.NO_OFFSET,
            this.outputs.size(),
            TopN.NO_LIMIT,
            null
        );
    }

    /*
     * @return true if it's guaranteed that a group-key-value doesn't occur in more than 1 shard.
     *         Each shard has "group or row authority"
     */
    private boolean shardsContainAllGroupKeyValues() {
        return source instanceof Collect &&
               ((Collect) source).tableInfo instanceof DocTableInfo &&
               GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(
                   ((DocTableInfo) ((Collect) source).tableInfo),
                   ((Collect) source).where,
                   groupKeys);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitGroupHashAggregate(this, context);
    }

    @Override
    public String toString() {
        return "GroupBy{" +
               "src=" + source +
               ", keys=" + groupKeys +
               ", agg=" + aggregates +
               '}';
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("GroupHashAggregate[")
            .text(Lists2.joinOn(", ", groupKeys, Symbol::toString));
        if (!aggregates.isEmpty()) {
            printContext
                .text(" | ")
                .text(Lists2.joinOn(", ", aggregates, Symbol::toString));
        }
        printContext
            .text("]")
            .nest(source::print);
    }
}
