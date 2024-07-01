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

import java.util.List;

import io.crate.expression.symbol.FieldReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.Union;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public final class MoveFilterBeneathUnion implements Rule<Filter> {

    private final Capture<Union> unionCapture;
    private final Pattern<Filter> pattern;

    public MoveFilterBeneathUnion() {
        this.unionCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(), typeOf(Union.class).capturedAs(unionCapture));
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter,
                             Captures captures,
                             Rule.Context ruleContext) {
        Union union = captures.get(unionCapture);
        LogicalPlan lhs = union.sources().get(0);
        LogicalPlan rhs = union.sources().get(1);
        return union.replaceSources(List.of(
            createNewFilter(filter, lhs),
            createNewFilter(filter, rhs)
        ));
    }

    private static Filter createNewFilter(Filter filter, LogicalPlan newSource) {
        Symbol newQuery = FieldReplacer.replaceFields(filter.query(), f -> {
            int idx = filter.source().outputs().indexOf(f);
            if (idx < 0) {
                throw new IllegalArgumentException(
                    "Field used in filter must be present in its source outputs." +
                    f + " missing in " + filter.source().outputs());
            }
            return newSource.outputs().get(idx);
        });
        return new Filter(newSource, newQuery);
    }
}
