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
import io.crate.expression.symbol.FieldsVisitor;
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
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.distribution.DistributionInfo;

import javax.annotation.Nullable;
import java.util.Collections;
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
public class Union implements LogicalPlan {

    private final List<Symbol> outputs;
    final LogicalPlan lhs;
    final LogicalPlan rhs;
    private final Map<LogicalPlan, SelectSymbol> dependencies;

    static Builder create(UnionSelect ttr, SubqueryPlanner subqueryPlanner, Functions functions, CoordinatorTxnCtx txnCtx) {
        return (tableStats, hints, usedColsByParent) -> {

            AnalyzedRelation left = ttr.left();
            AnalyzedRelation right = ttr.right();

            Set<Symbol> usedFromLeft = new HashSet<>();
            Set<Symbol> usedFromRight = new HashSet<>();

            addColumnsFrom(usedColsByParent, usedFromLeft::add, left);
            addColumnsFrom(usedColsByParent, usedFromRight::add, right);

            usedFromLeft.addAll(left.outputs());
            usedFromRight.addAll(right.outputs());

            LogicalPlan lhsPlan = LogicalPlanner
                .plan(left, FetchMode.NEVER_CLEAR, subqueryPlanner, true, functions, txnCtx)
                .build(tableStats, hints, usedFromLeft);

            LogicalPlan rhsPlan = LogicalPlanner
                .plan(right, FetchMode.NEVER_CLEAR, subqueryPlanner, true, functions, txnCtx)
                .build(tableStats, hints, usedFromRight);

            return new Union(lhsPlan, rhsPlan, ttr.outputs());
        };
    }

    private static void addColumnsFrom(Iterable<? extends Symbol> symbols,
                                       Consumer<? super Symbol> consumer,
                                       AnalyzedRelation rel) {

        for (Symbol symbol : symbols) {
            addColumnsFrom(symbol, consumer, rel);
        }
    }

    private static void addColumnsFrom(@Nullable Symbol symbol, Consumer<? super Symbol> consumer, AnalyzedRelation rel) {
        if (symbol == null) {
            return;
        }
        FieldsVisitor.visitFields(symbol, f -> {
            if (f.relation().getQualifiedName().equals(rel.getQualifiedName())) {
                consumer.accept(f.pointer());
            }
        });
    }

    Union(LogicalPlan lhs, LogicalPlan rhs, List<Symbol> outputs) {
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
        assert leftResultDesc.streamOutputs().equals(rightResultDesc.streamOutputs())
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
