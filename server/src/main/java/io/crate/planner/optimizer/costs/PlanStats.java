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

import javax.annotation.Nullable;

import io.crate.common.collections.Maps;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
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
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.types.DataTypes;

public class PlanStats {

    private final TableStats tableStats;
    private final StatsVisitor visitor;

    public PlanStats(TableStats tableStats) {
        this(tableStats, null);
    }

    public PlanStats(TableStats tableStats, @Nullable Memo memo) {
        this.tableStats = tableStats;
        this.visitor = new StatsVisitor(tableStats, memo);
    }

    public TableStats tableStats() {
        return tableStats;
    }

    public Stats get(LogicalPlan logicalPlan) {
        return logicalPlan.accept(visitor, null);
    }

    private static class StatsVisitor extends LogicalPlanVisitor<Void, Stats> {

        private final TableStats tableStats;
        @Nullable
        private final Memo memo;

        public StatsVisitor(TableStats tableStats, @Nullable Memo memo) {
            this.tableStats = tableStats;
            this.memo = memo;
        }

        @Override
        public Stats visitGroupReference(GroupReference group, Void context) {
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
        public Stats visitLimit(Limit limit, Void context) {
            var stats = limit.source().accept(this, context);
            if (limit.limit() instanceof Literal) {
                var numberOfRows = DataTypes.LONG.sanitizeValue(((Literal<?>) limit.limit()).value());
                return new Stats(numberOfRows, stats.sizeInBytes(), stats.statsByColumn());
            }
            return stats;
        }

        @Override
        public Stats visitUnion(Union union, Void context) {
            var numberOfRows = -1L;
            var sizeInBytes = -1L;
            var lhsStats = union.lhs().accept(this, context);
            var rhsStats = union.rhs().accept(this, context);
            if (lhsStats.numDocs() != -1 && rhsStats.numDocs() != -1) {
                numberOfRows = lhsStats.numDocs() + rhsStats.numDocs();
            }
            if (lhsStats.sizeInBytes() != -1 && rhsStats.sizeInBytes() != -1) {
                sizeInBytes = Math.max(lhsStats.sizeInBytes(), rhsStats.sizeInBytes());
            }
            return new Stats(numberOfRows, sizeInBytes, Maps.concat(lhsStats.statsByColumn(), rhsStats.statsByColumn()));
        }

        @Override
        public Stats visitNestedLoopJoin(NestedLoopJoin join, Void context) {
            var numberOfRows = -1L;
            var sizeInBytes = -1L;
            var lhsStats = join.lhs().accept(this, context);
            var rhsStats = join.rhs().accept(this, context);
            if (lhsStats.numDocs() != -1 && rhsStats.numDocs() != -1) {
                if (join.joinType() == JoinType.CROSS) {
                    numberOfRows = lhsStats.numDocs() * rhsStats.numDocs();
                } else {
                    // We don't have any cardinality estimates, so just take the bigger table
                    numberOfRows = Math.max(lhsStats.numDocs(), rhsStats.numDocs());
                }
            }
            if (lhsStats.sizeInBytes() != -1 && rhsStats.sizeInBytes() != -1) {
                sizeInBytes = lhsStats.sizeInBytes() + rhsStats.sizeInBytes();
            }
            return new Stats(numberOfRows, sizeInBytes, Maps.concat(lhsStats.statsByColumn(), rhsStats.statsByColumn()));
        }

        @Override
        public Stats visitHashJoin(HashJoin join, Void context) {
            var numberOfRows = -1L;
            var sizeInBytes = -1L;
            var lhsStats = join.lhs().accept(this, context);
            var rhsStats = join.rhs().accept(this, context);
            if (lhsStats.numDocs() != -1 && rhsStats.numDocs() != -1) {
                // We don't have any cardinality estimates, so just take the bigger table
                numberOfRows = Math.max(lhsStats.numDocs(), rhsStats.numDocs());
            }
            if (lhsStats.sizeInBytes() != -1 && rhsStats.sizeInBytes() != -1) {
                sizeInBytes = lhsStats.sizeInBytes() + rhsStats.sizeInBytes();
            }
            return new Stats(numberOfRows, sizeInBytes, Maps.concat(lhsStats.statsByColumn(), rhsStats.statsByColumn()));
        }

        @Override
        public Stats visitCollect(Collect collect, Void context) {
            var stats = tableStats.getStats(collect.relation().tableInfo().ident());
            if (stats.equals(Stats.EMPTY)) {
                return stats;
            } else {
                var query = collect.where().queryOrFallback();
                var numberOfRows = SelectivityFunctions.estimateNumRows(stats, query, null);
                return new Stats(numberOfRows, stats.sizeInBytes(), stats.statsByColumn());
            }
        }

        @Override
        public Stats visitFilter(Filter filter, Void context) {
            Stats sourceStats = filter.source().accept(this, context);
            Symbol query = filter.query();
            long numRows = SelectivityFunctions.estimateNumRows(sourceStats, query, null);
            return new Stats(numRows, sourceStats.sizeInBytes(), sourceStats.statsByColumn());
        }

        @Override
        public Stats visitCount(Count count, Void context) {
            return new Stats(1, Long.BYTES, Map.of());
        }

        @Override
        public Stats visitGet(Get get, Void context) {
            return new Stats(get.numExpectedRows(), get.estimatedRowSize(), Map.of());
        }

        @Override
        public Stats visitGroupHashAggregate(GroupHashAggregate groupHashAggregate, Void context) {
            var stats = groupHashAggregate.source().accept(this, context);
            return new Stats(groupHashAggregate.numExpectedRows(), stats.sizeInBytes(), stats.statsByColumn());
        }

        @Override
        public Stats visitHashAggregate(HashAggregate hashAggregate, Void context) {
            var stats = hashAggregate.source().accept(this, context);
            return new Stats(1L, stats.sizeInBytes(), stats.statsByColumn());
        }

        @Override
        public Stats visitInsert(Insert insert, Void context) {
            var stats = insert.sources().get(0).accept(this, context);
            return new Stats(1L, stats.sizeInBytes(), stats.statsByColumn());
        }

        @Override
        public Stats visitCorrelatedJoin(CorrelatedJoin join, Void context) {
            return join.sources().get(0).accept(this, context);
        }

        @Override
        public Stats visitTableFunction(TableFunction tableFunction, Void context) {
            // We don't have any estimates for table functions, but could go through the types of `outputs` to make a guess
            return Stats.EMPTY;
        }

        @Override
        public Stats visitPlan(LogicalPlan logicalPlan, Void context) {
            // This covers all sub-classes of LogicalForwardPlan
            List<LogicalPlan> sources = logicalPlan.sources();
            if (sources.size() == 1) {
                return sources.get(0).accept(this, context);
            }
            throw new UnsupportedOperationException("Plan stats not available for " + logicalPlan.getClass().getSimpleName());
        }
    }
}
