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

import java.util.List;
import java.util.Set;

import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.execution.engine.aggregation.impl.average.AverageAggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.LogicalPlan;

public final class Util {

    private Util() {}

    /// Aggregate functions for which a `distinct` rewrite to `GROUP BY` is supported.
    ///
    /// The default implementation of distinct functions is limited to the ones below
    /// (because it needs a matching `collection_*` scalar function), hence rules
    /// rewriting `distinct` aggregates are limited to those as well. Otherwise, we'd
    /// have a situation where an optimization enables more functions.
    static final Set<String> DISTINCT_REWRITE_SUPPORTED_AGGREGATES = Set.of(
        CountAggregation.NAME,
        AverageAggregation.NAMES[0],
        AverageAggregation.NAMES[1]
    );

    static boolean hasNoFilter(Function aggregate) {
        Symbol filter = aggregate.filter();
        return filter == null || filter.equals(Literal.BOOLEAN_TRUE);
    }

    /**
     * @return a new Plan where parent-child (A-B-C) are exchanged to child-parent (B-A-C)
     */
    static LogicalPlan transpose(LogicalPlan parent, LogicalPlan child) {
        return child.replaceSources(List.of(
            parent.replaceSources(child.sources())
        ));
    }
}
