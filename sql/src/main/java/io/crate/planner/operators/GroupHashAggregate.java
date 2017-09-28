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
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static io.crate.planner.operators.LogicalPlanner.NO_LIMIT;
import static io.crate.planner.operators.LogicalPlanner.extractColumns;

public class GroupHashAggregate implements LogicalPlan {

    private static final String DISTRIBUTED_MERGE_PHASE_NAME = "distributed merge";
    private final LogicalPlan source;
    private final List<Function> aggregates;
    private final List<Symbol> groupKeys;
    private final List<Symbol> outputs;

    public static Builder create(Builder source, List<Symbol> groupKeys, List<Function> aggregates) {
        return parentUsedCols -> {
            HashSet<Symbol> usedCols = new HashSet<>();
            usedCols.addAll(groupKeys);
            usedCols.addAll(extractColumns(aggregates));
            return new GroupHashAggregate(source.build(usedCols), groupKeys, aggregates);
        };
    }

    private GroupHashAggregate(LogicalPlan source, List<Symbol> groupKeys, List<Function> aggregates) {
        GroupByConsumer.validateGroupBySymbols(groupKeys);
        this.source = source;
        this.groupKeys = groupKeys;
        this.aggregates = aggregates;
        this.outputs = Lists2.concat(groupKeys, aggregates);
    }

    @Override
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limit,
                      int offset,
                      @Nullable OrderBy order,
                      @Nullable Integer pageSizeHint) {

        Plan plan = source.build(plannerContext, projectionBuilder, NO_LIMIT, 0, null, null);
        if (plan.resultDescription().hasRemainingLimitOrOffset()) {
            plan = Merge.ensureOnHandler(plan, plannerContext);
        }
        List<Symbol> sourceOutputs = source.outputs();
        boolean clusteredByGroupBy = clusteredByGroupKey();
        if (clusteredByGroupBy ||
            ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), plan.resultDescription().nodeIds())) {

            GroupProjection groupProjection = projectionBuilder.groupProjection(
                sourceOutputs,
                groupKeys,
                aggregates,
                AggregateMode.ITER_FINAL,
                // if not clusteredByGroupBy we need to use CLUSTER/NODE granularity to merge across shards
                clusteredByGroupBy && source.preferShardProjections() ? RowGranularity.SHARD : RowGranularity.CLUSTER
            );
            plan.addProjection(groupProjection);
            return plan;
        }

        GroupProjection toPartial = projectionBuilder.groupProjection(
            sourceOutputs,
            groupKeys,
            aggregates,
            AggregateMode.ITER_PARTIAL,
            source.preferShardProjections() ? RowGranularity.SHARD : RowGranularity.NODE
        );
        plan.addProjection(toPartial);
        plan.setDistributionInfo(DistributionInfo.DEFAULT_MODULO);

        GroupProjection toFinal = projectionBuilder.groupProjection(
            this.outputs,
            groupKeys,
            aggregates,
            AggregateMode.PARTIAL_FINAL,
            RowGranularity.CLUSTER
        );
        return createMerge(
            plannerContext,
            plan,
            Collections.singletonList(toFinal),
            plan.resultDescription().nodeIds()
        );
    }

    private Plan createMerge(Planner.Context plannerContext,
                             Plan plan,
                             List<Projection> projections,
                             Collection<String> nodeIds) {
        return new Merge(
            plan,
            new MergePhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                DISTRIBUTED_MERGE_PHASE_NAME,
                plan.resultDescription().nodeIds().size(),
                nodeIds,
                plan.resultDescription().streamOutputs(),
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
    private boolean clusteredByGroupKey() {
        return source instanceof Collect &&
               ((Collect) source).tableInfo instanceof DocTableInfo &&
               GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(
                   ((DocTableInfo) ((Collect) source).tableInfo),
                   ((Collect) source).where,
                   groupKeys);
    }

    @Override
    public LogicalPlan tryCollapse() {
        LogicalPlan collapsed = source.tryCollapse();
        if (collapsed == source) {
            return this;
        }
        return new GroupHashAggregate(collapsed, groupKeys, aggregates);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public List<TableInfo> baseTables() {
        return source.baseTables();
    }

    @Override
    public String toString() {
        return "GroupBy{" +
               "src=" + source +
               ", keys=" + groupKeys +
               ", agg=" + aggregates +
               '}';
    }
}
