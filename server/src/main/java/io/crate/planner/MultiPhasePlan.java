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

package io.crate.planner;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.MultiPhaseExecutor;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SubQueryResults;

import java.util.Map;

/**
 * Plan which depends on other plans to be executed first.
 *
 * E.g. for sub selects:
 *
 * <pre>
 * select * from t where x = (select 1) or x = (select 2)
 * </pre>
 *
 * The two subselects would be the dependencies.
 *
 * The dependencies themselves may also be MultiPhasePlans.
 */
public class MultiPhasePlan implements Plan {

    @VisibleForTesting
    protected final Plan rootPlan;

    @VisibleForTesting
    protected final Map<LogicalPlan, SelectSymbol> dependencies;

    public static Plan createIfNeeded(Plan rootPlan, Map<LogicalPlan, SelectSymbol> dependencies) {
        if (dependencies.isEmpty()) {
            return rootPlan;
        }
        return new MultiPhasePlan(rootPlan, dependencies);
    }

    private MultiPhasePlan(Plan rootPlan, Map<LogicalPlan, SelectSymbol> dependencies) {
        this.rootPlan = rootPlan;
        this.dependencies = dependencies;
    }

    @Override
    public StatementType type() {
        return rootPlan.type();
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencyCarrier,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        MultiPhaseExecutor.execute(dependencies, dependencyCarrier, plannerContext, params)
            .whenComplete((subQueryValues, failure) -> {
                if (failure == null) {
                    rootPlan.execute(dependencyCarrier, plannerContext, consumer, params, subQueryValues);
                } else {
                    consumer.accept(null, failure);
                }
            });
    }
}
