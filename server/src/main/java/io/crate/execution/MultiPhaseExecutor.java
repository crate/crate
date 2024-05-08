/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution;

import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.breaker.TypedRowAccounting;
import io.crate.data.CollectingRowConsumer;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.FirstColumnConsumers;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SubQueryResults;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;

public final class MultiPhaseExecutor {

    private MultiPhaseExecutor() {
    }

    public static CompletableFuture<SubQueryResults> execute(Map<LogicalPlan, SelectSymbol> dependencies,
                                                             DependencyCarrier executor,
                                                             PlannerContext plannerContext,
                                                             Row params) {
        var ramAccounting = ConcurrentRamAccounting.forCircuitBreaker(
            "multi-phase",
            executor.circuitBreaker(HierarchyCircuitBreakerService.QUERY),
            plannerContext.transactionContext().sessionSettings().memoryLimitInBytes()
        );

        List<CompletableFuture<?>> dependencyFutures = new ArrayList<>(dependencies.size());
        IdentityHashMap<SelectSymbol, Object> valueBySubQuery = new IdentityHashMap<>();

        for (Map.Entry<LogicalPlan, SelectSymbol> entry : dependencies.entrySet()) {
            LogicalPlan depPlan = entry.getKey();
            depPlan = plannerContext.optimize().apply(depPlan, plannerContext);
            SelectSymbol selectSymbol = entry.getValue();

            CollectingRowConsumer<?, ?> rowConsumer = getConsumer(selectSymbol, ramAccounting);
            depPlan.execute(
                executor, PlannerContext.forSubPlan(plannerContext), rowConsumer, params, SubQueryResults.EMPTY);

            dependencyFutures.add(rowConsumer.completionFuture().thenAccept(val -> {
                synchronized (valueBySubQuery) {
                    valueBySubQuery.put(selectSymbol, val);
                }
            }));
        }
        return CompletableFuture
            .allOf(dependencyFutures.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> {
                ramAccounting.release();
                return new SubQueryResults(valueBySubQuery);
            });
    }

    private static CollectingRowConsumer<?, ?> getConsumer(SelectSymbol selectSymbol, RamAccounting ramAccounting) {
        return new CollectingRowConsumer<>(FirstColumnConsumers.getCollector(selectSymbol.getResultType(), selectSymbol.innerType(), ramAccounting));
    }
}
