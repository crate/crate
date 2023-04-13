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

import io.crate.planner.operators.Collect;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.iterative.Memo;
import io.crate.statistics.Stats;

public class StatsCalculator {

    private final Memo memo;

    public StatsCalculator(Memo memo) {
        this.memo = memo;
    }

    public Map<LogicalPlan, Stats> calculate(LogicalPlan logicalPlan) {
        StatsVisitor visitor = new StatsVisitor(memo, this);
        var context = new HashMap<LogicalPlan, Stats>();
        logicalPlan.accept(visitor, context);
        return context;
    }

    static class StatsVisitor extends LogicalPlanVisitor<Map<LogicalPlan, Stats>, Void> {

        private final Memo memo;
        private final StatsCalculator statsCalculator;

        public StatsVisitor(Memo memo, StatsCalculator statsCalculator) {
            this.memo = memo;
            this.statsCalculator = statsCalculator;
        }

        @Override
        public Void visitGroupReference(GroupReference group, Map<LogicalPlan, Stats> context) {
            var groupId = group.groupId();
            var stats = memo.stats(groupId);
            if (stats == null) {
                var logicalPlan = memo.resolve(groupId);
                var result = statsCalculator.calculate(logicalPlan);
                stats = result.get(logicalPlan);
                memo.addStats(groupId, stats);
            }
            context.put(group, stats);
            return null;
        }

        @Override
        public Void visitCollect(Collect collect, Map<LogicalPlan, Stats> context) {
            var stats = new Stats(collect.numExpectedRows(), collect.estimatedRowSize());
            context.put(collect, stats);
            return null;
        }

        @Override
        public Void visitPlan(LogicalPlan logicalPlan, Map<LogicalPlan, Stats> context) {
            long numberOfDocs = 0L;
            for (LogicalPlan source : logicalPlan.sources()) {
                source.accept(this, context);
                var stats = context.get(source);
                numberOfDocs = numberOfDocs + stats.numDocs();
            }
            context.put(logicalPlan, new Stats(numberOfDocs, 0));
            return null;
        }
    }
}
