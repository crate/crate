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

import java.util.ArrayList;
import java.util.List;

import io.crate.expression.symbol.Literal;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Count;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.Union;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.iterative.Memo;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.types.DataTypes;

public class PlanStatsProvider {

    private final Memo memo;
    private final TableStats tableStats;

    public PlanStatsProvider(Memo memo, TableStats tableStats) {
        this.memo = memo;
        this.tableStats = tableStats;
    }

    public PlanStats apply(LogicalPlan logicalPlan) {

        if (logicalPlan instanceof GroupReference g) {
           return getAndMaybeUpdateGroupReferenceStats(g);
        }

        StatsVisitor visitor = new StatsVisitor(this, tableStats);
        var context = new ArrayList<PlanStats>();
        logicalPlan.accept(visitor, context);
        if (context.isEmpty()) {
            return new PlanStats(-1);
        }
        return context.get(context.size() -1);
    }

    PlanStats getAndMaybeUpdateGroupReferenceStats(GroupReference group) {
        var groupId = group.groupId();
        var stats = memo.stats(groupId);
        if (stats == null) {
            var logicalPlan = memo.resolve(groupId);
            stats = apply(logicalPlan);
            memo.addStats(groupId, stats);
        }
        return stats;
    }

    private static class StatsVisitor extends LogicalPlanVisitor<List<PlanStats>, Void> {

        private final PlanStatsProvider statsProvider;
        private final TableStats tableStats;

        public StatsVisitor(PlanStatsProvider statsCalculator, TableStats tableStats) {
            this.statsProvider = statsCalculator;
            this.tableStats = tableStats;
        }

        @Override
        public Void visitGroupReference(GroupReference group, List<PlanStats> context) {
            var stats  = statsProvider.apply(group);
            context.add(stats);
            return null;
        }

        @Override
        public Void visitLimit(Limit limit, List<PlanStats> context) {
            if (limit.limit() instanceof Literal) {
                long numberOfRows = DataTypes.LONG.sanitizeValue(((Literal<?>) limit.limit()).value());
                context.add(new PlanStats(numberOfRows));
            } else {
                limit.source().accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitUnion(Union union, List<PlanStats> context) {
            union.lhs().accept(this, context);
            var lhsStats = context.get(context.size() - 1);
            union.rhs().accept(this, context);
            var rhsStats = context.get(context.size() - 1);
            if (lhsStats.outputRowCount() == -1 || rhsStats.outputRowCount() == -1) {
                context.add(new PlanStats(-1));
            } else {
                context.add(new PlanStats(lhsStats.outputRowCount() + rhsStats.outputRowCount()));
            }
            return null;
        }

        @Override
        public Void visitCollect(Collect collect, List<PlanStats> context) {
            Stats stats = tableStats.getStats(collect.relation().relationName());
            context.add(new PlanStats(stats.numDocs()));
            return null;
        }

        @Override
        public Void visitCount(Count logicalPlan,List<PlanStats> context) {
            context.add(new PlanStats(1));
            return null;
        }

        @Override
        public Void visitPlan(LogicalPlan logicalPlan, List<PlanStats> context) {
            for (LogicalPlan source : logicalPlan.sources()) {
                source.accept(this, context);
            }
            return null;
        }
    }
}
