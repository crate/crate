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

import io.crate.expression.symbol.Literal;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.Union;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.types.DataTypes;

public class MoveLimitBeneathUnion implements Rule<Limit> {

    private final Capture<Union> unionCapture;
    private final Pattern<Limit> pattern;

    public MoveLimitBeneathUnion() {
        this.unionCapture = new Capture<>();
        this.pattern = typeOf(Limit.class)
            .with(l -> l.isPushedDown() == false)
            .with(source(), typeOf(Union.class).capturedAs(unionCapture));
    }

    @Override
    public Pattern<Limit> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Limit limit,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             Function<LogicalPlan, LogicalPlan> resolvePlan) {
        Union union = captures.get(unionCapture);
        // We cannot push the offset down to the sources, otherwise
        // we may loose rows, therefore we create a new limit where
        // the offset is added to the limit.
        if (limit.limit() instanceof Literal<?> l &&
            limit.offset() instanceof Literal<?> o) {

            int limitVal = DataTypes.INTEGER.sanitizeValue(l.value());
            int offSetVal = DataTypes.INTEGER.sanitizeValue(o.value());
            int newLimit = offSetVal + limitVal;

            return new Limit(
                new Union(
                    new Limit(union.lhs(), Literal.of(newLimit), Literal.of(0), false),
                    new Limit(union.rhs(), Literal.of(newLimit), Literal.of(0), false),
                    union.outputs()
                ),
                limit.limit(),
                limit.offset(),
                true
            );
        }
        return null;
    }
}
