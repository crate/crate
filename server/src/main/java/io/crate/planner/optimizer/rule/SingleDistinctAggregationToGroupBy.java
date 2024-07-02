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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.iterative.GroupReferenceResolver.resolveFully;
import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

import java.util.List;

import io.crate.execution.engine.aggregation.impl.CollectSetAggregation;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.expression.symbol.Function;
import io.crate.metadata.Reference;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.HashAggregate;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.types.DataTypes;

public class SingleDistinctAggregationToGroupBy implements Rule<Eval> {

    private final Pattern<Eval> pattern;

    public SingleDistinctAggregationToGroupBy() {
        this.pattern = typeOf(Eval.class).with(source(), typeOf(HashAggregate.class).with(this::hasSingleDistinct));
    }

    boolean hasSingleDistinct(HashAggregate agg) {
        Function function = agg.aggregates().get(0);
        return function.name().equals(CollectSetAggregation.NAME) &&
            function.arguments().size() == 1 &&
            function.arguments().get(0) instanceof Reference;
    }

    @Override
    public Pattern<Eval> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Eval plan, Captures captures, Context context) {
        Eval eval = (Eval) resolveFully(context.resolvePlan(), plan);
        HashAggregate agg = (HashAggregate) eval.source();
        Function function = agg.aggregates().get(0);
        Reference ref = (Reference) function.arguments().get(0);

        var countFunctionImpl = context.nodeCtx().functions().get(
            null,
            CountAggregation.NAME,
            List.of(ref),
            context.txnCtx().sessionSettings().searchPath()
        );
        var countFunction = new Function(
            countFunctionImpl.signature(),
            List.of(ref),
            DataTypes.LONG
        );

        return new HashAggregate(new GroupHashAggregate(agg.source(), List.of(ref), List.of()), List.of(countFunction));
    }
}
