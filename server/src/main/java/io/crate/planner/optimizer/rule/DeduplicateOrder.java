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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

import java.util.function.UnaryOperator;

import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.Order;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

/**
 * Transforms
 * <pre>
 * OrderA
 *  - OrderB
 * </pre>
 * into
 * <pre>
 * OrderA
 * </pre>
 *
 * The OrderB is redundant and would get re-sorted into OrderA
 */
public final class DeduplicateOrder implements Rule<Order> {

    private final Capture<Order> childOrder;
    private final Pattern<Order> pattern;

    public DeduplicateOrder() {
        this.childOrder = new Capture<>();
        this.pattern = typeOf(Order.class)
            .with(source(), typeOf(Order.class).capturedAs(childOrder));
    }

    @Override
    public Pattern<Order> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Order plan,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {
        Order childOrder = captures.get(this.childOrder);
        return plan.replaceSources(childOrder.sources());
    }
}
