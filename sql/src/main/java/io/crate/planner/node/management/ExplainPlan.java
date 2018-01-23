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

package io.crate.planner.node.management;

import io.crate.analyze.symbol.SelectSymbol;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Plan;
import io.crate.planner.PlanPrinter;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.ExplainLogicalPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.statement.CopyStatementPlanner;

import java.util.Map;

import static io.crate.data.SentinelRow.SENTINEL;

public class ExplainPlan implements Plan {

    private final Plan subPlan;

    public ExplainPlan(Plan subExecutionPlan) {
        this.subPlan = subExecutionPlan;
    }

    public Plan subPlan() {
        return subPlan;
    }

    @Override
    public void execute(DependencyCarrier executor,
                        PlannerContext plannerContext,
                        RowConsumer consumer,
                        Row params,
                        Map<SelectSymbol, Object> valuesBySubQuery) {
        Map<String, Object> map;
        try {
            if (subPlan instanceof LogicalPlan) {
                map = ExplainLogicalPlan.explainMap((LogicalPlan) subPlan, plannerContext, executor.projectionBuilder());
            } else if (subPlan instanceof CopyStatementPlanner.CopyFrom) {
                ExecutionPlan executionPlan = CopyStatementPlanner.planCopyFromExecution(
                    executor.clusterService().state().nodes(),
                    ((CopyStatementPlanner.CopyFrom) subPlan).copyFrom,
                    plannerContext
                );
                map = PlanPrinter.objectMap(executionPlan);
            } else {
                consumer.accept(null, new UnsupportedOperationException("EXPLAIN not supported for " + subPlan));
                return;
            }
        } catch (Throwable t) {
            consumer.accept(null, t);
            return;
        }
        consumer.accept(InMemoryBatchIterator.of(new Row1(map), SENTINEL), null);
    }
}
