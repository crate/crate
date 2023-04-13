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

import java.util.HashMap;
import java.util.Map;

import io.crate.expression.symbol.Literal;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.CorrelatedJoin;
import io.crate.planner.operators.Count;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.iterative.Memo;
import io.crate.statistics.Stats;
import io.crate.types.DataTypes;

public class StatsProvider {

    private final Memo memo;

    public StatsProvider(Memo memo) {
        this.memo = memo;
    }

    public PlanStats apply(LogicalPlan logicalPlan) {
        StatsVisitor visitor = new StatsVisitor(memo, this);
        var context = new HashMap<LogicalPlan, PlanStats>();
        logicalPlan.accept(visitor, context);
        return context.get(logicalPlan);
    }

    static class StatsVisitor extends LogicalPlanVisitor<Map<LogicalPlan, PlanStats>, Void> {

        private final Memo memo;
        private final StatsProvider statsProvider;

        public StatsVisitor(Memo memo, StatsProvider statsCalculator) {
            this.memo = memo;
            this.statsProvider = statsCalculator;
        }

        @Override
        public Void visitGroupReference(GroupReference group, Map<LogicalPlan, PlanStats> context) {
            var groupId = group.groupId();
            var stats = memo.stats(groupId);
            if (stats == null) {
                var logicalPlan = memo.resolve(groupId);
                stats = statsProvider.apply(logicalPlan);
                memo.addStats(groupId, stats);
            }
            context.put(group, stats);
            return null;
        }

        @Override
        public Void visitLimit(Limit limit, Map<LogicalPlan, PlanStats> context) {
            if (limit.limit() instanceof Literal) {
                long numberOfRows = DataTypes.LONG.sanitizeValue(((Literal<?>) limit.limit()).value());
                context.put(limit, new PlanStats(numberOfRows));
            } else {
                limit.source().accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitCollect(Collect collect, Map<LogicalPlan, PlanStats> context) {
            var stats = new PlanStats(collect.numExpectedRows());
            context.put(collect, stats);
            return null;
        }

        @Override
        public Void visitCount(Count logicalPlan, Map<LogicalPlan, PlanStats> context) {
            context.put(logicalPlan, new PlanStats(1));
            return null;
        }

        @Override
        public Void visitPlan(LogicalPlan logicalPlan, Map<LogicalPlan, PlanStats> context) {
            long numberOfDocs = 0L;
            for (LogicalPlan source : logicalPlan.sources()) {
                source.accept(this, context);
                var stats = context.get(source);
                numberOfDocs = numberOfDocs + stats.outputRowCount();
            }
            context.put(logicalPlan, new PlanStats(numberOfDocs));
            return null;
        }
    }
}
