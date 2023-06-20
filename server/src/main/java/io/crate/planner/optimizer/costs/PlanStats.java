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

package io.crate.planner.optimizer.costs;

import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Maps;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.CorrelatedJoin;
import io.crate.planner.operators.Count;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.Get;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.HashAggregate;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.Insert;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.operators.TableFunction;
import io.crate.planner.operators.Union;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.iterative.Memo;
import io.crate.planner.selectivity.SelectivityFunctions;
import io.crate.sql.tree.JoinType;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.types.DataTypes;

public class PlanStats {

    private final TableStats tableStats;
    private final StatsVisitor visitor;
    private final NodeContext nodeContext;

    public PlanStats(NodeContext nodeContext, TableStats tableStats) {
        this(nodeContext, tableStats, null);
    }

    public PlanStats(NodeContext nodeContext, TableStats tableStats, @Nullable Memo memo) {
        this.nodeContext = nodeContext;
        this.tableStats = tableStats;
        this.visitor = new StatsVisitor(nodeContext, tableStats, memo);
    }

    public PlanStats withMemo(Memo memo) {
        return new PlanStats(nodeContext, tableStats, memo);
    }

    public Stats get(TransactionContext txnCtx, LogicalPlan logicalPlan) {
        return logicalPlan.accept(visitor, txnCtx);
    }

    private static class StatsVisitor extends LogicalPlanVisitor<TransactionContext, Stats> {

        private final NodeContext nodeContext;
        private final TableStats tableStats;

        @Nullable
        private final Memo memo;

        public StatsVisitor(NodeContext nodeContext, TableStats tableStats, @Nullable Memo memo) {
            this.nodeContext = nodeContext;
            this.tableStats = tableStats;
            this.memo = memo;
        }

        @Override
        public Stats visitGroupReference(GroupReference group, TransactionContext txnCtx) {
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
                stats = logicalPlan.accept(this, txnCtx);
                memo.addStats(groupId, stats);
            }
            return stats;
        }

        @Override
        public Stats visitLimit(Limit limit, TransactionContext txnCtx) {
            var stats = limit.source().accept(this, txnCtx);
            if (limit.limit() instanceof Literal<?> literal) {
                var numberOfRows = DataTypes.LONG.sanitizeType(literal.value());
                return stats.withNumDocs(numberOfRows);
            }
            return stats;
        }

        @Override
        public Stats visitUnion(Union union, TransactionContext txnCtx) {
            var lhsStats = union.lhs().accept(this, txnCtx);
            var rhsStats = union.rhs().accept(this, txnCtx);
            return lhsStats.add(rhsStats);
        }

        @Override
        public Stats visitNestedLoopJoin(NestedLoopJoin join, TransactionContext txnCtx) {
            var lhsStats = join.lhs().accept(this, txnCtx);
            var rhsStats = join.rhs().accept(this, txnCtx);
            Map<ColumnIdent, ColumnStats<?>> statsByColumn = Maps.concat(lhsStats.statsByColumn(), rhsStats.statsByColumn());
            if (lhsStats.numDocs() == -1
                || lhsStats.sizeInBytes() == -1
                || rhsStats.numDocs() == -1
                || rhsStats.sizeInBytes() == -1) {
                return new Stats(-1, -1, statsByColumn);
            }
            long numRows = join.joinType() == JoinType.CROSS
                ? lhsStats.numDocs() * rhsStats.numDocs()
                // We don't have any cardinality estimates, so just take the bigger table
                : Math.max(lhsStats.numDocs(), rhsStats.numDocs());
            Stats joinStats = new Stats(
                numRows,
                (lhsStats.averageSizePerRowInBytes() * numRows) + (rhsStats.averageSizePerRowInBytes() * numRows),
                statsByColumn
            );
            Symbol joinCondition = join.joinCondition();
            if (joinCondition == null) {
                return joinStats;
            }
            long estimatedNumRows = SelectivityFunctions.estimateNumRows(
                nodeContext,
                txnCtx,
                joinStats,
                joinCondition,
                null
            );
            return joinStats.withNumDocs(estimatedNumRows);
        }

        @Override
        public Stats visitHashJoin(HashJoin join, TransactionContext txnCtx) {
            var lhsStats = join.lhs().accept(this, txnCtx);
            var rhsStats = join.rhs().accept(this, txnCtx);
            Map<ColumnIdent, ColumnStats<?>> statsByColumn = Maps.concat(lhsStats.statsByColumn(), rhsStats.statsByColumn());
            if (lhsStats.numDocs() == -1
                || lhsStats.sizeInBytes() == -1
                || rhsStats.numDocs() == -1
                || rhsStats.sizeInBytes() == -1) {
                return new Stats(-1, -1, statsByColumn);
            }
            long numRows = Math.max(lhsStats.numDocs(), rhsStats.numDocs());
            long sizeInBytes =
                (numRows * lhsStats.averageSizePerRowInBytes())
                + (numRows * rhsStats.averageSizePerRowInBytes());

            Stats joinStats = new Stats(numRows, sizeInBytes, statsByColumn);
            long estimatedNumRows = SelectivityFunctions.estimateNumRows(
                nodeContext,
                txnCtx,
                joinStats,
                join.joinCondition(),
                null
            );
            return joinStats.withNumDocs(estimatedNumRows);
        }

        @Override
        public Stats visitCollect(Collect collect, TransactionContext txnCtx) {
            var stats = tableStats.getStats(collect.relation().tableInfo().ident());
            if (stats.equals(Stats.EMPTY)) {
                return stats;
            } else {
                var query = collect.where().queryOrFallback();
                var numberOfRows = SelectivityFunctions.estimateNumRows(nodeContext, txnCtx, stats, query, null);
                return stats.withNumDocs(numberOfRows);
            }
        }

        @Override
        public Stats visitFilter(Filter filter, TransactionContext txnCtx) {
            Stats sourceStats = filter.source().accept(this, txnCtx);
            Symbol query = filter.query();
            long numRows = SelectivityFunctions.estimateNumRows(nodeContext, txnCtx, sourceStats, query, null);
            return sourceStats.withNumDocs(numRows);
        }

        @Override
        public Stats visitCount(Count count, TransactionContext txnCtx) {
            return new Stats(1, Long.BYTES, Map.of());
        }

        @Override
        public Stats visitGet(Get get, TransactionContext txnCtx) {
            Stats stats = tableStats.getStats(get.table().relationName());
            return stats.withNumDocs(get.numExpectedRows());
        }

        @Override
        public Stats visitGroupHashAggregate(GroupHashAggregate groupHashAggregate, TransactionContext txnCtx) {
            var stats = groupHashAggregate.source().accept(this, txnCtx);
            return stats.withNumDocs(GroupHashAggregate.approximateDistinctValues(stats, groupHashAggregate.groupKeys()));
        }

        @Override
        public Stats visitHashAggregate(HashAggregate hashAggregate, TransactionContext txnCtx) {
            var stats = hashAggregate.source().accept(this, txnCtx);
            return stats.withNumDocs(1);
        }

        @Override
        public Stats visitInsert(Insert insert, TransactionContext txnCtx) {
            var stats = insert.sources().get(0).accept(this, txnCtx);
            return stats.withNumDocs(1);
        }

        @Override
        public Stats visitCorrelatedJoin(CorrelatedJoin join, TransactionContext txnCtx) {
            return join.sources().get(0).accept(this, txnCtx);
        }

        @Override
        public Stats visitTableFunction(TableFunction tableFunction, TransactionContext txnCtx) {
            // We don't have any estimates for table functions, but could go through the types of `outputs` to make a guess
            return Stats.EMPTY;
        }

        @Override
        public Stats visitPlan(LogicalPlan logicalPlan, TransactionContext txnCtx) {
            // This covers all sub-classes of LogicalForwardPlan
            List<LogicalPlan> sources = logicalPlan.sources();
            if (sources.size() == 1) {
                return sources.get(0).accept(this, txnCtx);
            }
            throw new UnsupportedOperationException("Plan stats not available for " + logicalPlan.getClass().getSimpleName());
        }
    }
}
