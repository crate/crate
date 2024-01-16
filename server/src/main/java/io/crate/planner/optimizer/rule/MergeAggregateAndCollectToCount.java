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

import java.util.function.Function;

import io.crate.common.collections.Lists;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Count;
import io.crate.planner.operators.HashAggregate;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public final class MergeAggregateAndCollectToCount implements Rule<HashAggregate> {

    private final Capture<Collect> collectCapture;
    private final Pattern<HashAggregate> pattern;

    public MergeAggregateAndCollectToCount() {
        this.collectCapture = new Capture<>();
        this.pattern = typeOf(HashAggregate.class)
            .with(source(), typeOf(Collect.class).capturedAs(collectCapture)
                .with(collect -> collect.relation().tableInfo() instanceof DocTableInfo))
            .with(aggregate ->
                aggregate.aggregates().size() == 1
                && aggregate.aggregates().get(0).signature().equals(CountAggregation.COUNT_STAR_SIGNATURE));
    }

    @Override
    public Pattern<HashAggregate> pattern() {
        return pattern;
    }

    @Override
    public Count apply(HashAggregate aggregate,
                       Captures captures,
                       PlanStats planStats,
                       TransactionContext txnCtx,
                       NodeContext nodeCtx,
                       Function<LogicalPlan, LogicalPlan> resolvePlan) {
        Collect collect = captures.get(collectCapture);
        var countAggregate = Lists.getOnlyElement(aggregate.aggregates());
        if (countAggregate.filter() != null) {
            return new Count(
                countAggregate,
                collect.relation(),
                collect.where().add(countAggregate.filter()));
        } else {
            return new Count(countAggregate, collect.relation(), collect.where());
        }
    }
}
