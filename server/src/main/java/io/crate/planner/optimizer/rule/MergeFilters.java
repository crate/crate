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

import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.Filter;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

/**
 * Transforms
 * <pre>
 * - Filter
 *   - Filter
 *     - Source
 * </pre>
 * into
 * <pre>
 * - Filter
 *   - Source
 * </pre>
 *
 * By merging the conditions of both filters
 */
public class MergeFilters implements Rule<Filter> {

    private final Capture<Filter> child;
    private final Pattern<Filter> pattern;

    public MergeFilters() {
        child = new Capture<>();
        pattern = typeOf(Filter.class)
            .with(source(), typeOf(Filter.class).capturedAs(child));
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public Filter apply(Filter plan,
                        Captures captures,
                        Rule.Context context) {
        Filter childFilter = captures.get(child);
        Symbol parentQuery = plan.query();
        Symbol childQuery = childFilter.query();
        return new Filter(childFilter.source(), AndOperator.of(parentQuery, childQuery));
    }
}
