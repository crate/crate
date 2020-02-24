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
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Maps;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.UnionExecutionPlan;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.statistics.TableStats;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.planner.operators.Limit.limitAndOffset;

/**
 * A logical plan for the Union operation. Takes care of building the
 * {@link UnionExecutionPlan}.
 *
 * Note: Currently doesn't support Fetch operations. Ensures that no
 * intermediate fetches occur by passing all columns to the nested plans
 * and setting {@code FetchMode.NEVER_CLEAR}.
 */
public class Union implements LogicalPlan {

    private final List<Symbol> outputs;
    final LogicalPlan lhs;
    final LogicalPlan rhs;
    private final Map<LogicalPlan, SelectSymbol> dependencies;

    static LogicalPlan create(UnionSelect union,
                              SubqueryPlanner subqueryPlanner,
                              Functions functions,
                              CoordinatorTxnCtx txnCtx,
                              Set<PlanHint> hints,
                              TableStats tableStats,
                              Row params) {
        AnalyzedRelation left = union.left();
        AnalyzedRelation right = union.right();
        LogicalPlan lhsPlan = LogicalPlanner.plan(left, subqueryPlanner, true, functions, txnCtx, hints, tableStats, params);
        LogicalPlan rhsPlan = LogicalPlanner.plan(right, subqueryPlanner, true, functions, txnCtx, hints, tableStats, params);

        return new Union(lhsPlan, rhsPlan, union.outputs());
    }

    private Union(LogicalPlan lhs, LogicalPlan rhs, List<Symbol> outputs) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.outputs = outputs;
        this.dependencies = Maps.concat(lhs.dependencies(), rhs.dependencies());
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

        Integer childPageSizeHint = limit != TopN.NO_LIMIT
            ? limitAndOffset(limit, offset)
            : null;

        ExecutionPlan left = lhs.build(
            plannerContext, projectionBuilder, limit + offset, offset, null, childPageSizeHint, params, subQueryResults);
        ExecutionPlan right = rhs.build(
            plannerContext, projectionBuilder, limit + offset, offset, null, childPageSizeHint, params, subQueryResults);

        left = addMergeIfNeeded(left, plannerContext);
        right = addMergeIfNeeded(right, plannerContext);

        ResultDescription leftResultDesc = left.resultDescription();
        ResultDescription rightResultDesc = right.resultDescription();

        assert DataTypes.compareTypesById(leftResultDesc.streamOutputs(), rightResultDesc.streamOutputs())
            : "Left and right must output the same types, got " +
              "lhs=" + leftResultDesc.streamOutputs() + ", rhs=" + rightResultDesc.streamOutputs();

        MergePhase mergePhase = new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "union",
            leftResultDesc.nodeIds().size() + rightResultDesc.nodeIds().size(),
            2,
            Collections.singletonList(plannerContext.handlerNode()),
            leftResultDesc.streamOutputs(),
            Collections.emptyList(),
            DistributionInfo.DEFAULT_BROADCAST,
            leftResultDesc.orderBy()
        );

        return new UnionExecutionPlan(
            left,
            right,
            mergePhase,
            limit,
            offset,
            lhs.outputs().size(),
            TopN.NO_LIMIT,
            leftResultDesc.orderBy()
        );
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return Lists2.concat(lhs.baseTables(), rhs.baseTables());
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of(lhs, rhs);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Union(sources.get(0), sources.get(1), outputs);
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return dependencies;
    }

    @Override
    public long numExpectedRows() {
        return lhs.numExpectedRows() + rhs.numExpectedRows();
    }

    @Override
    public long estimatedRowSize() {
        return Math.max(lhs.estimatedRowSize(), rhs.estimatedRowSize());
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitUnion(this, context);
    }

    /**
     * Wraps the plan inside a Merge plan if limit or offset need to be applied.
     */
    private static ExecutionPlan addMergeIfNeeded(ExecutionPlan plan, PlannerContext plannerContext) {
        ResultDescription resultDescription = plan.resultDescription();
        if (resultDescription.hasRemainingLimitOrOffset()) {
            // Do a merge because we have to apply a limit/offset projection
            //
            // Note: Currently, this is performed on the handler node. It would be possible to
            // do this on another involved node instead but we don't do that for now because
            // the Merge of the union itself is always performed on the handler. So the
            // performance gain would be small.
            return Merge.ensureOnHandler(plan, plannerContext);
        }
        return plan;
    }
}
