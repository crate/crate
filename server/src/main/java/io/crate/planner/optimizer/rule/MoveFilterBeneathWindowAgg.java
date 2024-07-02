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

import static io.crate.planner.operators.LogicalPlanner.extractColumns;
import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;
import static io.crate.planner.optimizer.rule.Util.transpose;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import io.crate.analyze.WindowDefinition;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.WindowAgg;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public final class MoveFilterBeneathWindowAgg implements Rule<Filter> {

    private final Capture<WindowAgg> windowAggCapture;
    private final Pattern<Filter> pattern;

    public MoveFilterBeneathWindowAgg() {
        this.windowAggCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(), typeOf(WindowAgg.class).capturedAs(windowAggCapture));
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter,
                             Captures captures,
                             Rule.Context context) {
        WindowAgg windowAgg = captures.get(windowAggCapture);
        WindowDefinition windowDefinition = windowAgg.windowDefinition();
        List<WindowFunction> windowFunctions = windowAgg.windowFunctions();

        Predicate<Symbol> containsWindowFunction =
            symbol -> symbol instanceof WindowFunction && windowFunctions.contains(symbol);

        Symbol predicate = filter.query();
        List<Symbol> filterParts = AndOperator.split(predicate);

        ArrayList<Symbol> remainingFilterSymbols = new ArrayList<>();
        ArrayList<Symbol> windowPartitionedBasedFilters = new ArrayList<>();

        for (Symbol part : filterParts) {
            if (part.any(containsWindowFunction) == false
                && windowDefinition.partitions().containsAll(extractColumns(part))) {
                windowPartitionedBasedFilters.add(part);
            } else {
                remainingFilterSymbols.add(part);
            }
        }
        assert remainingFilterSymbols.size() > 0 || windowPartitionedBasedFilters.size() > 0 :
            "Splitting the filter symbol must result in at least one group";

        /* SELECT ROW_NUMBER() OVER(PARTITION BY id)
         * WHERE `x = 1`
         *
         * `x` is not the window partition column.
         * We cannot push down the filter as it would change the window aggregation value
         *
         */
        if (windowPartitionedBasedFilters.isEmpty()) {
            return null;
        }

        /* SELECT ROW_NUMBER() OVER(PARTITION BY id)
         * WHERE `id = 1`
         * remainingFilterSymbols:                  []
         * windowPartitionedBasedFilters:           [id = 1]
         *
         * Filter
         *  |
         * WindowsAgg
         *
         * transforms to
         *
         * WindowAgg
         *  |
         * Filter (id = 1)
         */
        if (remainingFilterSymbols.isEmpty()) {
            return transpose(filter, windowAgg);
        }

        /* WHERE `ROW_NUMBER() OVER(PARTITION BY id) = 2 AND id = 1`
         * remainingFilterSymbols:                  [ROW_NUMBER() OVER(PARTITION BY id) = 2]
         * windowPartitionedBasedFilters:           [id = 1]
         *
         * Filter
         *  |
         * WindowsAgg
         *
         * transforms to
         *
         * Filter (ROW_NUMBER() OVER(PARTITION BY id) = 2)
         *  |
         * WindowAgg
         *  |
         * Filter (id = 1)
         */
        LogicalPlan newWindowAgg = windowAgg.replaceSources(
            List.of(new Filter(windowAgg.source(), AndOperator.join(windowPartitionedBasedFilters))));

        return new Filter(newWindowAgg, AndOperator.join(remainingFilterSymbols));
    }
}
