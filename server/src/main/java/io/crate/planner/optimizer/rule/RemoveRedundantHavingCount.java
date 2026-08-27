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

import java.util.ArrayList;
import java.util.List;

import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public final class RemoveRedundantHavingCount implements Rule<Filter> {

    private final Capture<GroupHashAggregate> groupByCapture;
    private final Pattern<Filter> pattern;

    public RemoveRedundantHavingCount() {
        this.groupByCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(), typeOf(GroupHashAggregate.class).capturedAs(groupByCapture));
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter,
                             Captures captures,
                             Rule.Context ruleContext) {
        GroupHashAggregate groupBy = captures.get(groupByCapture);

        if (groupBy.groupKeys().isEmpty()) {
            return null;
        }

        Symbol query = filter.query();
        List<Symbol> parts = AndOperator.split(query);
        List<Symbol> toKeep = new ArrayList<>();
        boolean changed = false;

        for (Symbol part : parts) {
            if (isRedundantCountStarGreaterThanZero(part)) {
                changed = true;
            } else {
                toKeep.add(part);
            }
        }

        if (!changed) {
            return null;
        }

        if (toKeep.isEmpty()) {
            return filter.source();
        } else {
            return new Filter(filter.source(), AndOperator.join(toKeep));
        }
    }

    private static boolean isRedundantCountStarGreaterThanZero(Symbol symbol) {
        if (symbol instanceof Function function && function.name().equals("op_>")) {
            List<Symbol> args = function.arguments();
            if (args.size() == 2) {
                Symbol left = args.get(0);
                Symbol right = args.get(1);

                if (left instanceof Function leftFunc && right instanceof Literal<?> rightLiteral) {
                    if (leftFunc.signature().equals(CountAggregation.COUNT_STAR_SIGNATURE) && leftFunc.filter() == null) {
                        Object val = rightLiteral.value();
                        return val instanceof Number num && num.longValue() == 0L;
                    }
                }
            }
        }
        return false;
    }
}
