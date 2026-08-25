/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.List;
import java.util.SequencedCollection;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import io.crate.common.annotations.VisibleForTesting;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.sql.tree.JoinType;

public class JoinPlan extends AbstractJoinPlan {

    private final boolean isFiltered;
    private final boolean rewriteFilterOnOuterJoinToInnerJoinDone;
    private final boolean lookUpJoinRuleApplied;
    private final boolean moveConstantJoinConditionRuleApplied;
    private final boolean eliminateCrossJoinRuleIsApplied;

    public JoinPlan(LogicalPlan lhs,
                    LogicalPlan rhs,
                    JoinType joinType,
                    @Nullable Symbol joinCondition,
                    boolean isFiltered,
                    boolean rewriteFilterOnOuterJoinToInnerJoinDone,
                    boolean lookUpJoinRuleApplied,
                    LookUpJoin lookUpJoin) {
        this(lhs, rhs, joinType, joinCondition, isFiltered, rewriteFilterOnOuterJoinToInnerJoinDone, lookUpJoinRuleApplied, false, false, lookUpJoin);
    }

    @VisibleForTesting
    public JoinPlan(LogicalPlan lhs,
                    LogicalPlan rhs,
                    JoinType joinType,
                    @Nullable Symbol joinCondition) {
        this(lhs, rhs, joinType, joinCondition, false, false, false, false, false, LookUpJoin.NONE);
    }

    public JoinPlan(LogicalPlan lhs,
                     LogicalPlan rhs,
                     JoinType joinType,
                     @Nullable Symbol joinCondition,
                     boolean isFiltered,
                     boolean rewriteFilterOnOuterJoinToInnerJoinDone,
                     boolean lookUpJoinRuleApplied,
                     boolean moveConstantJoinConditionRuleApplied,
                     boolean eliminateCrossJoinRuleIsApplied,
                     LookUpJoin lookUpJoin) {
        super(lhs, rhs, joinCondition, joinType, lookUpJoin);
        this.isFiltered = isFiltered;
        this.rewriteFilterOnOuterJoinToInnerJoinDone = rewriteFilterOnOuterJoinToInnerJoinDone;
        this.lookUpJoinRuleApplied = lookUpJoinRuleApplied;
        this.moveConstantJoinConditionRuleApplied = moveConstantJoinConditionRuleApplied;
        this.eliminateCrossJoinRuleIsApplied = eliminateCrossJoinRuleIsApplied;
    }

    public boolean isLookUpJoinRuleApplied() {
        return lookUpJoinRuleApplied;
    }

    public boolean isFiltered() {
        return isFiltered;
    }

    public boolean isRewriteFilterOnOuterJoinToInnerJoinDone() {
        return rewriteFilterOnOuterJoinToInnerJoinDone;
    }

    public boolean moveConstantJoinConditionRuleApplied() {
        return moveConstantJoinConditionRuleApplied;
    }

    public boolean eliminateCrossJoinRuleIsApplied() {
        return eliminateCrossJoinRuleIsApplied;
    }

    public JoinPlan withMoveConstantJoinConditionRuleApplied(boolean moveConstantJoinConditionRuleApplied) {
        return new JoinPlan(
            lhs,
            rhs,
            joinType,
            joinCondition,
            isFiltered,
            rewriteFilterOnOuterJoinToInnerJoinDone,
            lookUpJoinRuleApplied,
            moveConstantJoinConditionRuleApplied,
            eliminateCrossJoinRuleIsApplied,
            lookupJoin
        );
    }

    @Override
    public ExecutionPlan build(DependencyCarrier dependencyCarrier,
                               PlannerContext plannerContext,
                               Set<PlanHint> planHints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        throw new UnsupportedOperationException(
            "JoinPlan cannot be build, it needs to be converted to a NestedLoop/HashJoin");
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitJoinPlan(this, context);
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        throw new UnsupportedOperationException(
            "JoinPlan cannot have outputs pruned directly, it needs to be converted to a NestedLoop/HashJoin");
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("Join[")
            .text(joinType.toString());
        if (joinCondition != null) {
            printContext
                .text(" | ")
                .text(joinCondition.toString());
        }
        printContext.text("]");
        printStats(printContext);
        printContext.nest(Lists.map(sources(), x -> x::print));
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new JoinPlan(
            sources.get(0),
            sources.get(1),
            joinType,
            joinCondition,
            isFiltered,
            rewriteFilterOnOuterJoinToInnerJoinDone,
            lookUpJoinRuleApplied,
            moveConstantJoinConditionRuleApplied,
            eliminateCrossJoinRuleIsApplied,
            lookupJoin
        );
    }
}
