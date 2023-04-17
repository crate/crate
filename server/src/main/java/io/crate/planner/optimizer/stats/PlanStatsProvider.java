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

package io.crate.planner.optimizer.stats;

import javax.annotation.Nullable;

import io.crate.expression.symbol.Literal;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Count;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.operators.Union;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.iterative.Memo;
import io.crate.sql.tree.JoinType;
import io.crate.statistics.TableStats;
import io.crate.types.DataTypes;

public class PlanStatsProvider {

    private final TableStats tableStats;
    // Memo can be null when there are no Group References in the Logical Plans involved
    @Nullable
    private final Memo memo;

    public PlanStatsProvider(TableStats tableStats, @Nullable Memo memo) {
        this.tableStats = tableStats;
        this.memo = memo;
    }

    public PlanStats apply(LogicalPlan logicalPlan) {
        PlanStatsVisitor visitor = new PlanStatsVisitor(tableStats, memo);
        return logicalPlan.accept(visitor, null);
    }

    private static class PlanStatsVisitor extends LogicalPlanVisitor<Void, PlanStats> {

        private final TableStats tableStats;
        @Nullable
        private final Memo memo;

        public PlanStatsVisitor(TableStats tableStats, @Nullable  Memo memo) {
            this.tableStats = tableStats;
            this.memo = memo;
        }

        @Override
        public PlanStats visitGroupReference(GroupReference group, Void context) {
            if (memo == null) {
                throw new UnsupportedOperationException("Stats cannot be provided for GroupReference without a Memo");
            }
            var groupId = group.groupId();
            var stats = memo.stats(groupId);
            if (stats == null) {
                // No stats for this group yet.
                // Let's get the logical plan, calculate the stats
                // and update the stats for this group
                var logicalPlan = memo.resolve(groupId);
                stats = logicalPlan.accept(this, context);
                memo.addStats(groupId, stats);
            }
            return stats;
        }

        @Override
        public PlanStats visitLimit(Limit limit, Void context) {
            if (limit.limit() instanceof Literal) {
                var numberOfRows = DataTypes.LONG.sanitizeValue(((Literal<?>) limit.limit()).value());
                return new PlanStats(numberOfRows);
            }
            return limit.source().accept(this, context);
        }

        @Override
        public PlanStats visitUnion(Union union, Void context) {
            var lhsStats = union.lhs().accept(this, context);
            var rhsStats = union.rhs().accept(this, context);
            if (lhsStats.outputRowCount() == -1 || rhsStats.outputRowCount() == -1) {
                return PlanStats.EMPTY;
            }
            return new PlanStats(lhsStats.outputRowCount() + rhsStats.outputRowCount());
        }

        @Override
        public PlanStats visitNestedLoopJoin(NestedLoopJoin join, Void context) {
            var lhsStats = join.lhs().accept(this, context);
            var rhsStats = join.rhs().accept(this, context);
            if (lhsStats.outputRowCount() == -1 || rhsStats.outputRowCount() == -1) {
                return PlanStats.EMPTY;
            }
            if (join.joinType() == JoinType.CROSS) {
                return new PlanStats(lhsStats.outputRowCount() * rhsStats.outputRowCount());
            }
            // We don't have any cardinality estimates, so just take the bigger table
            return new PlanStats(Math.max(lhsStats.outputRowCount(), rhsStats.outputRowCount()));
        }

        @Override
        public PlanStats visitHashJoin(HashJoin join, Void context) {
            var lhsStats = join.lhs().accept(this, context);
            var rhsStats = join.rhs().accept(this, context);
            if (lhsStats.outputRowCount() == -1 || rhsStats.outputRowCount() == -1) {
                return PlanStats.EMPTY;
            }
            // We don't have any cardinality estimates, so just take the bigger table
            return new PlanStats(Math.max(lhsStats.outputRowCount(), rhsStats.outputRowCount()));
        }

        @Override
        public PlanStats visitCollect(Collect collect, Void context) {
            var stats = tableStats.getStats(collect.relation().relationName());
            return new PlanStats(stats.numDocs());
        }

        @Override
        public PlanStats visitCount(Count logicalPlan, Void context) {
            return new PlanStats(1);
        }

        @Override
        public PlanStats visitPlan(LogicalPlan logicalPlan, Void context) {
            if(logicalPlan.sources().size() == 1) {
                return logicalPlan.sources().get(0).accept(this, context);
            }
            throw new UnsupportedOperationException("Plan stats not available");
        }
    }
}
