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
import static io.crate.planner.optimizer.rule.Util.transpose;

import java.util.ArrayList;
import java.util.List;

import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionType;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

/**
 * Transforms queries like
 *
 * <pre>
 *     SELECT x, count(*) FROM t GROUP BY x HAVING x > 10
 * </pre>
 *
 * into
 *
 * <pre>
 *     SELECT x, count(*) FROM t WHERE x > 10 GROUP BY x
 * </pre>
 */
public final class MoveFilterBeneathGroupBy implements Rule<Filter> {

    private final Pattern<Filter> pattern;
    private final Capture<GroupHashAggregate> groupByCapture;

    public MoveFilterBeneathGroupBy() {
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
        // Since something like `SELECT x, sum(y) FROM t GROUP BY x HAVING y > 10` is not valid
        // (y would have to be declared as group key) any parts of a HAVING that is not an aggregation can be moved.
        Symbol predicate = filter.query();
        List<Symbol> parts = AndOperator.split(predicate);
        ArrayList<Symbol> withAggregates = new ArrayList<>();
        ArrayList<Symbol> withoutAggregates = new ArrayList<>();
        for (Symbol part : parts) {
            if (part.hasFunctionType(FunctionType.AGGREGATE)) {
                withAggregates.add(part);
            } else {
                withoutAggregates.add(part);
            }
        }
        if (withoutAggregates.isEmpty()) {
            return null;
        }
        GroupHashAggregate groupBy = captures.get(groupByCapture);
        if (withoutAggregates.size() == parts.size()) {
            return transpose(filter, groupBy);
        }

        /* HAVING `count(*) > 1 AND x = 10`
         * withAggregates:    [count(*) > 1]
         * withoutAggregates: [x = 10]
         *
         * Filter
         *  |
         * GroupBy
         *
         * transforms to
         *
         * Filter (count(*) > 1)
         *  |
         * GroupBy
         *  |
         * Filter (x = 10)
         */
        LogicalPlan newGroupBy = groupBy.replaceSources(
            List.of(new Filter(groupBy.source(), AndOperator.join(withoutAggregates))));

        return new Filter(newGroupBy, AndOperator.join(withAggregates));
    }

}
