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

package io.crate.executor.task;

import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowsBatchIterator;
import io.crate.executor.Task;
import io.crate.planner.PlanPrinter;
import io.crate.planner.node.management.ExplainPlan;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ExplainTask implements Task {

    private final ExplainPlan explainPlan;

    public ExplainTask(ExplainPlan explainPlan) {
        this.explainPlan = explainPlan;
    }

    @Override
    public void execute(BatchConsumer consumer, Row parameters) {
        Map<String, Object> map;
        try {
            map = PlanPrinter.objectMap(explainPlan.subPlan());
        } catch (Throwable t) {
            consumer.accept(null, t);
            return;
        }

        consumer.accept(RowsBatchIterator.newInstance(new Row1(map)), null);
    }

    @Override
    public List<CompletableFuture<Long>> executeBulk() {
        throw new UnsupportedOperationException("ExplainTask cannot be executed as bulk operation");
    }
}
