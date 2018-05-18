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
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.join.JoinOperations;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.join.Join;
import io.crate.planner.node.dql.join.JoinType;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
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
        this.isFiltered = isFiltered || joinCondition != null;
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

    JoinType joinType() {
        return joinType;
    }

    @Nullable
    Symbol joinCondition() {
        return joinCondition;
    }

    @Override
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
         * Benchmarks reveal that if rows are filtered out distributed execution gives better performance.
         * Therefore if `filterNeeded` is true (there is joinCondition or a filtering after the join operation)
         * then it's a good indication that distributed execution will be faster.
         *
         * We may at some point add some kind of session-settings to override this behaviour
         * or otherwise come up with a better heuristic.
         */
        Integer childPageSizeHint = !isFiltered && limit != TopN.NO_LIMIT
            ? limitAndOffset(limit, offset)
            : null;

        ExecutionPlan left = lhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, childPageSizeHint, params, subQueryValues);
        ExecutionPlan right = rhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, childPageSizeHint, params, subQueryValues);

        PositionalOrderBy orderByFromLeft = left.resultDescription().orderBy();
        boolean hasDocTables = baseTables.stream().anyMatch(r -> r instanceof DocTableRelation);
        boolean isDistributed = hasDocTables && isFiltered && !joinType.isOuter();

        LogicalPlan leftLogicalPlan = lhs;
        LogicalPlan rightLogicalPlan = rhs;
        isDistributed = isDistributed &&
                        (!left.resultDescription().nodeIds().isEmpty() && !right.resultDescription().nodeIds().isEmpty());
        if (isDistributed && joinType.supportsInversion() &&
            lhs.numExpectedRows() < rhs.numExpectedRows() && orderByFromLeft == null) {
            // The right side is always broadcast-ed, so for performance reasons we switch the tables so that
            // the right table is the smaller (numOfRows). If left relation has a pushed-down OrderBy that needs
            // to be preserved, then the switch is not possible.
            ExecutionPlan tmpExecutionPlan = left;
            left = right;
            right = tmpExecutionPlan;
            leftLogicalPlan = rhs;
            rightLogicalPlan = lhs;
        }
        Tuple<Collection<String>, List<MergePhase>> joinExecutionNodesAndMergePhases =
            configureExecution(left, right, plannerContext, isDistributed);

        List<Symbol> joinOutputs = Lists2.concat(leftLogicalPlan.outputs(), rightLogicalPlan.outputs());

        Symbol joinInput = null;
        if (joinCondition != null) {
            joinInput = InputColumns.create(joinCondition, joinOutputs);
        }

        NestedLoopPhase nlPhase = new NestedLoopPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            isDistributed,
            Collections.singletonList(JoinOperations.createJoinProjection(outputs, joinOutputs)),
            joinExecutionNodesAndMergePhases.v2().get(0),
            joinExecutionNodesAndMergePhases.v2().get(1),
            leftLogicalPlan.outputs().size(),
            rightLogicalPlan.outputs().size(),
            joinExecutionNodesAndMergePhases.v1(),
            joinType,
            joinInput
        );
        return new Join(
            nlPhase,
            left,
            right,
            TopN.NO_LIMIT,
            0,
            TopN.NO_LIMIT,
            outputs.size(),
            orderByFromLeft
        );
    }

    private Tuple<Collection<String>, List<MergePhase>> configureExecution(ExecutionPlan left,
                                                                           ExecutionPlan right,
                                                                           PlannerContext plannerContext,
                                                                           boolean isDistributed) {
        Collection<String> nlExecutionNodes = ImmutableSet.of(plannerContext.handlerNode());
        ResultDescription leftResultDesc = left.resultDescription();
        ResultDescription rightResultDesc = right.resultDescription();
        MergePhase leftMerge = null;
        MergePhase rightMerge = null;

        if (leftResultDesc.nodeIds().size() == 1
            && leftResultDesc.nodeIds().equals(rightResultDesc.nodeIds())
            && !rightResultDesc.hasRemainingLimitOrOffset()) {
            // if the left and the right plan are executed on the same single node the mergePhase
            // should be omitted. This is the case if the left and right table have only one shards which
            // are on the same node
            nlExecutionNodes = leftResultDesc.nodeIds();
            left.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            right.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
        } else if (isDistributed && !leftResultDesc.hasRemainingLimitOrOffset()) {
            // run join phase distributed on all nodes of the left relation
            nlExecutionNodes = leftResultDesc.nodeIds();
            left.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            right.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            rightMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, rightResultDesc, nlExecutionNodes);
        } else {
            // run join phase non-distributed on the handler
            left.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            right.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            if (JoinOperations.isMergePhaseNeeded(nlExecutionNodes, leftResultDesc, false)) {
                leftMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, leftResultDesc, nlExecutionNodes);
            }
            if (JoinOperations.isMergePhaseNeeded(nlExecutionNodes, rightResultDesc, false)) {
                rightMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, rightResultDesc, nlExecutionNodes);
            }
        }
        return new Tuple<>(nlExecutionNodes, Arrays.asList(leftMerge, rightMerge));
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
                    Collections.newSetFromMap(new IdentityHashMap<>());
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
