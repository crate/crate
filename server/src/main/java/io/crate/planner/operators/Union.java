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

import static io.crate.planner.operators.Limit.limitAndOffset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Maps;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.UnionExecutionPlan;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.types.DataTypes;

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

    public Union(LogicalPlan lhs, LogicalPlan rhs, List<Symbol> outputs) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.outputs = outputs;
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> hints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {

        Integer childPageSizeHint = limit != LimitAndOffset.NO_LIMIT
            ? limitAndOffset(limit, offset)
            : null;

        ExecutionPlan left = lhs.build(
            executor, plannerContext, hints, projectionBuilder, limit + offset, LimitAndOffset.NO_OFFSET, null, childPageSizeHint, params, subQueryResults);
        ExecutionPlan right = rhs.build(
            executor, plannerContext, hints, projectionBuilder, limit + offset, LimitAndOffset.NO_OFFSET, null, childPageSizeHint, params, subQueryResults);

        addCastsForIncompatibleObjects(lhs, left);
        addCastsForIncompatibleObjects(rhs, right);

        if (left.resultDescription().hasRemainingLimitOrOffset()) {
            left = Merge.ensureOnHandler(left, plannerContext);
        }
        if (right.resultDescription().hasRemainingLimitOrOffset()) {
            right = Merge.ensureOnHandler(right, plannerContext);
        }

        ResultDescription leftResultDesc = left.resultDescription();
        ResultDescription rightResultDesc = right.resultDescription();

        assert DataTypes.isCompatibleType(leftResultDesc.streamOutputs(), rightResultDesc.streamOutputs())
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
            LimitAndOffset.NO_LIMIT,
            LimitAndOffset.NO_OFFSET,
            lhs.outputs().size(),
            LimitAndOffset.NO_LIMIT,
            leftResultDesc.orderBy()
        );
    }

    /**
     * <p>
     * The Analyzer ensures that the outputs of the two relation of a UNION match,
     * but The ObjectType can be incompatible for streaming in the following
     * scenario:
     * </p>
     *
     * <pre>
     *
     * LHS: { a :: int, b :: int }
     * RHS: { a :: int, c :: int }
     * </pre>
     *
     * <p>
     * The streaming implementation uses the innerKeys for value streaming (or
     * `UndefinedType` if not)
     *
     * <ul>
     * <li>a is written and received from both sides as int</li>
     * <li>b is mixed: one side would use int, the other generic value streaming</li>
     * <li>c is mixed: one side would use int, the other generic value streaming</li>
     * </ul>
     *
     * <p>
     * This adds a EvalProjection with casts to ensure the right side is streamed
     * using the same type
     * </p>
     **/
    private void addCastsForIncompatibleObjects(LogicalPlan logicalPlan, ExecutionPlan executionPlan) {
        EvalProjection castValues = EvalProjection.castValues(Symbols.typeView(outputs), logicalPlan.outputs());
        if (castValues != null) {
            executionPlan.addProjection(castValues);
        }
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
    public List<RelationName> getRelationNames() {
        return Lists2.concatUnique(lhs.getRelationNames(), rhs.getRelationNames());
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
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        IntArrayList outputIndicesToKeep = new IntArrayList();
        for (Symbol outputToKeep : outputsToKeep) {
            SymbolVisitors.intersection(outputToKeep, outputs, s -> {
                // Union can contain identically looking ScopedSymbols due to aliased relations. E.g.:
                //
                // SELECT * FROM
                //  (SELECT
                //      t1.a,
                //      t2.a
                //   FROM t AS t1,
                //        t AS t2
                //   ) t3
                // UNION
                // SELECT 1, 1;
                //
                // Has [a, a] as outputs where the two `a` are not the same
                //
                // To account for that, we keep all indices that match:
                for (int i = 0; i < outputs.size(); i++) {
                    Symbol output = outputs.get(i);
                    if (output.equals(s) && !outputIndicesToKeep.contains(i)) {
                        outputIndicesToKeep.add(i);
                    }
                }
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
        LogicalPlan newLhs = lhs.pruneOutputsExcept(toKeepFromLhs);
        LogicalPlan newRhs = rhs.pruneOutputsExcept(toKeepFromRhs);
        if (newLhs == lhs && newRhs == rhs) {
            return this;
        }
        return new Union(newLhs, newRhs, newOutputs);
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Maps.concat(lhs.dependencies(), rhs.dependencies());
    }

    public LogicalPlan lhs() {
        return lhs;
    }

    public LogicalPlan rhs() {
        return rhs;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitUnion(this, context);
    }
}
