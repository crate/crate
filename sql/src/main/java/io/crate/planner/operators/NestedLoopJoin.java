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

import com.google.common.collect.ImmutableSet;
import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.join.JoinOperations;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.join.JoinType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static io.crate.planner.operators.Limit.limitAndOffset;
import static io.crate.planner.operators.LogicalPlanner.NO_LIMIT;

class NestedLoopJoin extends TwoInputPlan {

    @Nullable
    private final Symbol joinCondition;
    private final AnalyzedRelation topMostLeftRelation;
    private final JoinType joinType;
    private final boolean hasOuterJoins;

    private final boolean isFiltered;

    NestedLoopJoin(LogicalPlan lhs,
                   LogicalPlan rhs,
                   JoinType joinType,
                   @Nullable Symbol joinCondition,
                   boolean isFiltered,
                   boolean hasOuterJoins,
                   AnalyzedRelation topMostLeftRelation) {
        super(lhs, rhs, new ArrayList<>());
        this.joinType = joinType;
        this.isFiltered = isFiltered;
        if (joinType == JoinType.SEMI) {
            this.outputs.addAll(lhs.outputs());
        } else {
            this.outputs.addAll(lhs.outputs());
            this.outputs.addAll(rhs.outputs());
        }
        this.topMostLeftRelation = topMostLeftRelation;
        this.joinCondition = joinCondition;
        this.hasOuterJoins = hasOuterJoins;
    }

    public JoinType joinType() {
        return joinType;
    }

    @Nullable
    public Symbol joinCondition() {
        return joinCondition;
    }

    public Map<LogicalPlan, SelectSymbol> dependencies() {
        HashMap<LogicalPlan, SelectSymbol> deps = new HashMap<>(lhs.dependencies().size() + rhs.dependencies().size());
        deps.putAll(lhs.dependencies());
        deps.putAll(rhs.dependencies());
        return deps;
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
        /*
         * isDistributed/filterNeeded doesn't consider the joinCondition.
         * This means joins with implicit syntax result in a different plan than joins using explicit syntax.
         * This was unintentional but we'll keep it that way (for now) as a distributed plan can be significantly slower
         * (depending on the number of rows that are filtered)
         * and we don't want to introduce a performance regression.
         *
         * We may at some point add some kind of session-settings to override this behaviour or otherwise
         * come up with a better heuristic.
         */
        Integer childPageSizeHint = !isFiltered && joinCondition == null && limit != TopN.NO_LIMIT
            ? limitAndOffset(limit, offset)
            : null;

        ExecutionPlan left = lhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, childPageSizeHint, params, subQueryValues);
        ExecutionPlan right = rhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, childPageSizeHint, params, subQueryValues);


        boolean hasDocTables = baseTables.stream().anyMatch(r -> r instanceof DocTableRelation);
        JoinType joinType = this.joinType;
        boolean isDistributed = hasDocTables && isFiltered && !joinType.isOuter();

        ResultDescription leftResultDesc = left.resultDescription();
        ResultDescription rightResultDesc = right.resultDescription();
        isDistributed = isDistributed &&
                        (!leftResultDesc.nodeIds().isEmpty() && !rightResultDesc.nodeIds().isEmpty());
        boolean switchTables = false;
        if (isDistributed && joinType.supportsInversion() && lhs.numExpectedRows() < rhs.numExpectedRows()) {
            // temporarily switch plans and relations to apply broadcasting logic
            // to smaller side (which is always the right side).
            switchTables = true;
            ExecutionPlan tmpExecutionPlan = left;
            left = right;
            right = tmpExecutionPlan;

            leftResultDesc = left.resultDescription();
            rightResultDesc = right.resultDescription();
        }
        Collection<String> nlExecutionNodes = ImmutableSet.of(plannerContext.handlerNode());

        MergePhase leftMerge = null;
        MergePhase rightMerge = null;
        if (isDistributed && !leftResultDesc.hasRemainingLimitOrOffset()) {
            left.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            nlExecutionNodes = leftResultDesc.nodeIds();
        } else {
            left.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            if (JoinOperations.isMergePhaseNeeded(nlExecutionNodes, leftResultDesc, false)) {
                leftMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, leftResultDesc, nlExecutionNodes);
            }
        }
        if (nlExecutionNodes.size() == 1
            && nlExecutionNodes.equals(rightResultDesc.nodeIds())
            && !rightResultDesc.hasRemainingLimitOrOffset()) {
            // if the left and the right plan are executed on the same single node the mergePhase
            // should be omitted. This is the case if the left and right table have only one shards which
            // are on the same node
            right.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
        } else {
            if (JoinOperations.isMergePhaseNeeded(nlExecutionNodes, rightResultDesc, isDistributed)) {
                rightMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, rightResultDesc, nlExecutionNodes);
            }
            right.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
        }

        if (switchTables) {
            ExecutionPlan tmpExecutionPlan = left;
            left = right;
            right = tmpExecutionPlan;
            MergePhase tmp = leftMerge;
            leftMerge = rightMerge;
            rightMerge = tmp;
        }
        Symbol joinInput = null;
        if (joinCondition != null) {
            joinInput = InputColumns.create(joinCondition, Lists2.concat(lhs.outputs(), rhs.outputs()));
        }

        NestedLoopPhase nlPhase = new NestedLoopPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            isDistributed ? "distributed-nested-loop" : "nested-loop",
            // JoinPhase ctor wants at least one projection
            Collections.singletonList(new EvalProjection(InputColumn.fromSymbols(outputs))),
            leftMerge,
            rightMerge,
            lhs.outputs().size(),
            rhs.outputs().size(),
            nlExecutionNodes,
            joinType,
            joinInput
        );
        return new io.crate.planner.node.dql.join.Join(
            nlPhase,
            left,
            right,
            TopN.NO_LIMIT,
            0,
            TopN.NO_LIMIT,
            outputs.size(),
            null
        );
    }

    @Override
    protected LogicalPlan updateSources(LogicalPlan newLeftSource, LogicalPlan newRightSource) {
        return new NestedLoopJoin(
            newLeftSource,
            newRightSource,
            joinType,
            joinCondition,
            isFiltered,
            hasOuterJoins,
            topMostLeftRelation);
    }

    @Override
    public long numExpectedRows() {
        if (joinType == JoinType.CROSS) {
            return lhs.numExpectedRows() * rhs.numExpectedRows();
        } else {
            // We don't have any cardinality estimates, so just take the bigger table
            return Math.max(lhs.numExpectedRows(), rhs.numExpectedRows());
        }
    }

    @Override
    public long estimatedRowSize() {
        return lhs.estimatedRowSize() + rhs.estimatedRowSize();
    }

    @Override
    public LogicalPlan tryOptimize(@Nullable LogicalPlan pushDown, SymbolMapper mapper) {
        if (pushDown instanceof Order) {
            /* Move the orderBy expression to the sub-relation if possible.
             *
             * This is possible because a nested loop preserves the ordering of the input-relation
             * IF:
             *   - the order by expressions only operate using fields from a single relation
             *   - that relation happens to be on the left-side of the join
             *   - there is no outer join involved in the whole join (outer joins may create null rows - breaking the ordering)
             */
            if (hasOuterJoins == false) {
                Set<AnalyzedRelation> relationsInOrderBy =
                    Collections.newSetFromMap(new IdentityHashMap<AnalyzedRelation, Boolean>());
                Consumer<Field> gatherRelations = f -> relationsInOrderBy.add(f.relation());

                OrderBy orderBy = ((Order) pushDown).orderBy;
                for (Symbol orderExpr : orderBy.orderBySymbols()) {
                    FieldsVisitor.visitFields(orderExpr, gatherRelations);
                }
                if (relationsInOrderBy.size() == 1) {
                    AnalyzedRelation relationInOrderBy = relationsInOrderBy.iterator().next();
                    if (relationInOrderBy == topMostLeftRelation) {
                        LogicalPlan newLhs = lhs.tryOptimize(pushDown, SymbolMapper.fromMap(expressionMapping));
                        if (newLhs != null) {
                            return updateSources(newLhs, rhs);
                        }
                    }
                }
            }
        }
        return super.tryOptimize(pushDown, mapper);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoopJoin(this, context);
    }
}
