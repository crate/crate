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

import java.util.function.Function;

import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.Union;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.iterative.Memo;
import io.crate.statistics.TableStats;

public class StatsCalculator {

    private final Function<LogicalPlan, LogicalPlan> resolvePlan;
    private final TableStats tableStats;
    private final Memo memo;

    public StatsCalculator(TableStats tableStats, Function<LogicalPlan, LogicalPlan> resolvePlan, Memo memo ) {
        this.tableStats = tableStats;
        this.resolvePlan = resolvePlan;
        this.memo = memo;
    }

    public PlanStats calculate(LogicalPlan logicalPlan) {
        StatsPlanVisitor visitor = new StatsPlanVisitor(tableStats, memo);
        logicalPlan.accept(visitor)
    }

    private PlanStats groupStats(GroupReference group) {
        var groupId = group.groupId();
        var stats = memo.stats(groupId);
        if (stats == null) {
            stats = calculate(memo.resolve(groupId));
            memo.addStats(groupId, stats);
        }
        return stats;
    }

    static class StatsPlanVisitor extends LogicalPlanVisitor<StatsCalculator.Context, Void> {

        private final TableStats tableStats;
        private final Memo memo;

        public StatsPlanVisitor(TableStats tableStats, Memo memo) {
            this.tableStats = tableStats;
            this.memo = memo;
        }

        @Override
        public Void visitGroupReference(GroupReference group, Void context) {
                groupStats(group);
        }

        @Override
        public Void visitCollect(Collect logicalPlan, StatsCalculator.Context context) {
            var stats = tableStats.getStats(logicalPlan.relation().relationName());
            context.outputRowCount = stats.numDocs();
            return null;
        }

        @Override
        public Void visitLimit(Limit logicalPlan, StatsCalculator.Context context) {
            // filter by limit
        }

        @Override
        public Void visitUnion(Union logicalPlan, Context context) {
            logicalPlan.
        }

        @Override
        public Void visitPlan(LogicalPlan logicalPlan, StatsCalculator.Context context) {
            for (LogicalPlan source : logicalPlan.sources()) {
                source.accept(this, context);
            }
            return null;
        }
    }

    static class Context {
        long outputRowCount;
    }
}
