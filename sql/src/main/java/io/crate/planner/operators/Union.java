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
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.analyze.symbol.FieldsVisitor;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Row;
import io.crate.operation.projectors.TopN;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.UnionExecutionPlan;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static io.crate.planner.operators.Limit.limitAndOffset;

/**
 * A logical plan for the Union operation. Takes care of building the
 * {@link UnionExecutionPlan}.
 *
 * Note: Currently doesn't support Fetch operations. Ensures that no
 * intermediate fetches occur by passing all columns to the nested plans
 * and setting {@code FetchMode.NEVER_CLEAR}.
 */
public class Union extends TwoInputPlan {

    static Builder create(UnionSelect ttr, SubqueryPlanner subqueryPlanner) {
        return (tableStats, usedColsByParent) -> {

            QueriedRelation left = ttr.left();
            QueriedRelation right = ttr.right();

            Set<Symbol> usedFromLeft = new HashSet<>();
            Set<Symbol> usedFromRight = new HashSet<>();

            addColumnsFrom(usedColsByParent, usedFromLeft::add, left);
            addColumnsFrom(usedColsByParent, usedFromRight::add, right);

            usedFromLeft.addAll(left.outputs());
            usedFromRight.addAll(right.outputs());

            LogicalPlan lhsPlan = LogicalPlanner
                .plan(left, FetchMode.NEVER_CLEAR, subqueryPlanner, false)
                .build(tableStats, usedFromLeft);

            LogicalPlan rhsPlan = LogicalPlanner
                .plan(right, FetchMode.NEVER_CLEAR, subqueryPlanner, false)
                .build(tableStats, usedFromRight);

            return new Union(ttr.outputs(), lhsPlan, rhsPlan);
        };
    }

    private static void addColumnsFrom(Iterable<? extends Symbol> symbols,
                                       Consumer<? super Symbol> consumer,
                                       QueriedRelation rel) {

        for (Symbol symbol : symbols) {
            addColumnsFrom(symbol, consumer, rel);
        }
    }

    private static void addColumnsFrom(@Nullable Symbol symbol, Consumer<? super Symbol> consumer, QueriedRelation rel) {
        if (symbol == null) {
            return;
        }
        FieldsVisitor.visitFields(symbol, f -> {
            if (f.relation().getQualifiedName().equals(rel.getQualifiedName())) {
                consumer.accept(rel.querySpec().outputs().get(f.index()));
            }
        });
    }

    Union(List<Symbol> outputs, LogicalPlan lhs, LogicalPlan rhs) {
        super(lhs, rhs, outputs);
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               Map<SelectSymbol, Object> subQueryValues) {

        Integer childPageSizeHint = limit != TopN.NO_LIMIT
            ? limitAndOffset(limit, offset)
            : null;

        ExecutionPlan left = lhs.build(
            plannerContext, projectionBuilder, limit + offset, offset, null, childPageSizeHint, params, subQueryValues);
        ExecutionPlan right = rhs.build(
            plannerContext, projectionBuilder, limit + offset, offset, null, childPageSizeHint, params, subQueryValues);

        left = addMergeIfNeeded(left, plannerContext);
        right = addMergeIfNeeded(right, plannerContext);

        ResultDescription leftResultDesc = left.resultDescription();
        ResultDescription rightResultDesc = right.resultDescription();

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
            null
        );

        return new UnionExecutionPlan(
            left,
            right,
            mergePhase,
            limit,
            0,
            lhs.outputs().size(),
            TopN.NO_LIMIT
        );
    }

    @Override
    protected LogicalPlan newInstance(LogicalPlan newLeftSource, LogicalPlan newRightSource) {
        return new Union(outputs, newLeftSource, newRightSource);
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        if (lhs.dependencies().isEmpty() && rhs.dependencies().isEmpty()) {
            return Collections.emptyMap();
        }
        HashMap<LogicalPlan, SelectSymbol> dependencies = new HashMap<>();
        dependencies.putAll(lhs.dependencies());
        dependencies.putAll(rhs.dependencies());
        return dependencies;
    }

    @Override
    public long numExpectedRows() {
        return lhs.numExpectedRows() + rhs.numExpectedRows();
    }

    /**
     * Wraps the plan inside a Merge plan if limit or offset need to be applied.
     */
    private static ExecutionPlan addMergeIfNeeded(ExecutionPlan plan, PlannerContext plannerContext) {
        ResultDescription resultDescription = plan.resultDescription();
        if (resultDescription.hasRemainingLimitOrOffset()) {
            // Do a merge because we have to apply a limit/offset projection
            plan = Merge.ensureOnHandler(plan, plannerContext);
        }
        return plan;
    }
}
