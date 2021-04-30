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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Maps;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.UnionExecutionPlan;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.statistics.TableStats;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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


    public Union(LogicalPlan lhs, LogicalPlan rhs, List<Symbol> outputs) {
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
            plannerContext, projectionBuilder, limit + offset, TopN.NO_OFFSET, null, childPageSizeHint, params, subQueryResults);
        ExecutionPlan right = rhs.build(
            plannerContext, projectionBuilder, limit + offset, TopN.NO_OFFSET, null, childPageSizeHint, params, subQueryResults);

        if (left.resultDescription().hasRemainingLimitOrOffset()) {
            left = Merge.ensureOnHandler(left, plannerContext);
        }
        if (right.resultDescription().hasRemainingLimitOrOffset()) {
            right = Merge.ensureOnHandler(right, plannerContext);
        }

        ResultDescription leftResultDesc = left.resultDescription();
        ResultDescription rightResultDesc = right.resultDescription();

        assert DataTypes.isSameType(leftResultDesc.streamOutputs(), rightResultDesc.streamOutputs())
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
    public List<AbstractTableRelation<?>> baseTables() {
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
    public LogicalPlan pruneOutputsExcept(TableStats tableStats, Collection<Symbol> outputsToKeep) {
        IntArrayList outputIndicesToKeep = new IntArrayList();
        for (Symbol outputToKeep : outputsToKeep) {
            SymbolVisitors.intersection(outputToKeep, outputs, s -> {
                int idx = outputs.indexOf(s);
                assert idx >= 0 : "outputs must contain symbol " + s + " if intersection called consumer";
                outputIndicesToKeep.add(idx);
            });
        }
        ArrayList<Symbol> toKeepFromLhs = new ArrayList<>();
        ArrayList<Symbol> toKeepFromRhs = new ArrayList<>();
        ArrayList<Symbol> newOutputs = new ArrayList<>();
        for (IntCursor cursor : outputIndicesToKeep) {
            toKeepFromLhs.add(lhs.outputs().get(cursor.value));
            toKeepFromRhs.add(rhs.outputs().get(cursor.value));
            newOutputs.add(outputs.get(cursor.value));
        }
        LogicalPlan newLhs = lhs.pruneOutputsExcept(tableStats, toKeepFromLhs);
        LogicalPlan newRhs = rhs.pruneOutputsExcept(tableStats, toKeepFromRhs);
        if (newLhs == lhs && newRhs == rhs) {
            return this;
        }
        return new Union(newLhs, newRhs, newOutputs);
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
}
