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
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.ForeignCollect;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

/**
 * Like {@link MergeFilterAndCollect}, changes `Filter -> ForeignCollect` to
 * `ForeignCollect` with the filter merged into {@link ForeignCollect#where()}.
 *
 * Separate rule to have a separate session setting to disable/enable it.
 * (And in the future it could be restricted to specific FDW implementations)
 */
public class MergeFilterAndForeignCollect implements Rule<Filter> {

    private final Capture<ForeignCollect> collectCapture;
    private final Pattern<Filter> pattern;


    public MergeFilterAndForeignCollect() {
        this.collectCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(), typeOf(ForeignCollect.class).capturedAs(collectCapture));
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {
        ForeignCollect collect = captures.get(collectCapture);
        if (collect.fdw().supportsQueryPushdown(filter.query())) {
            return new ForeignCollect(
                collect.fdw(),
                collect.relation(),
                collect.outputs(),
                collect.where().add(filter.query()),
                collect.executeAs()
            );
        }
        return null;
    }
}
