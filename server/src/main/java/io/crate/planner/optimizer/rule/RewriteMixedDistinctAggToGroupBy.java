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

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

import java.util.List;
import java.util.Set;

import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.execution.engine.aggregation.impl.MaximumAggregation;
import io.crate.execution.engine.aggregation.impl.MinimumAggregation;
import io.crate.execution.engine.aggregation.impl.average.AverageAggregation;
import io.crate.execution.engine.aggregation.sum.SumAggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.HashAggregate;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SplitDistinctAggregate;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

/// Rewrites a global aggregate that mixes 1 or more distinct aggregation with 1 or more non-distinct
/// aggregate over different.
///
/// The rewrite is done with a [GroupHashAggregate] that deduplicates the distinct column and pre-aggregates
/// the non-distinct ones. The execution plan for `HashAggregate` calculates the final value from the partial values.
///
/// The result is:
///
/// ```
/// HashAggregate[sum(x), count(DISTINCT y)]           HashAggregate[sum(x), count(DISTINCT y)]
///   └ Collect[doc.t | [x, y] | true]           ->       └ GroupHashAggregate[y | sum(x)]
///                                                          └ Collect[doc.t | [x, y] | true]
/// ```
///
/// The effect is the same as if we would rewrite the query:
///  ```
/// SELECT SUM(x), COUNT(DISTINCT y) FROM t;
/// ```
/// to
/// ```
/// SELECT SUM(partial_sum), COUNT(y)
/// FROM (SELECT y, SUM(x) AS partial_sum FROM t GROUP BY y) tmp;
/// ```
/// See [RewriteDistinctAggToGroupBy] for the case where *every* aggregate is distinct over the
/// same column, which this rule doesn't handle.
public final class RewriteMixedDistinctAggToGroupBy implements Rule<HashAggregate> {
    /// Non-distinct aggregates that can be calculated from per-distinct-key partials.
    private static final Set<String> SUPPORTED_NON_DISTINCT = Set.of(
        CountAggregation.NAME,
        SumAggregation.NAME,
        AverageAggregation.NAMES[0],
        AverageAggregation.NAMES[1],
        MinimumAggregation.NAME,
        MaximumAggregation.NAME
    );

    private final Pattern<HashAggregate> pattern;

    public RewriteMixedDistinctAggToGroupBy() {
        this.pattern = typeOf(HashAggregate.class)
            .with(RewriteMixedDistinctAggToGroupBy::matches);
    }

    /// True if `agg` has a mix of
    /// * `DISTINCT` aggregates that all share one column, and
    /// * one or more non-distinct aggregates.
    ///
    /// Returns `false` if:
    /// * there is more than one distinct column
    /// * there is a filter
    /// * there are and scalar-expression aggregate arguments (only plain columns, or `count(*)`).
    private static boolean matches(HashAggregate agg) {
        if (agg.distinctMode() != HashAggregate.DistinctMode.COLLECT_SET) {
            return false;
        }

        Reference distinctColumn = null;
        boolean foundNonDistinct = false;

        for (Function fn : agg.aggregates()) {
            if (hasFilter(fn)) {
                return false;
            }
            if (fn.distinct()) {
                if (!RewriteDistinctAggToGroupBy.SUPPORTED_AGGREGATES.contains(fn.name())) {
                    return false;
                }
                if (fn.arguments().isEmpty() || !(fn.arguments().get(0) instanceof Reference ref)) {
                    return false;
                }
                if (distinctColumn != null && !distinctColumn.equals(ref)) {
                    return false;
                }
                distinctColumn = ref;
            } else {
                if (!SUPPORTED_NON_DISTINCT.contains(fn.name())) {
                    return false;
                }
                if (!fn.arguments().isEmpty() && !(fn.arguments().get(0) instanceof Reference)) {
                    return false;
                }
                foundNonDistinct = true;
            }
        }

        return distinctColumn != null && foundNonDistinct;
    }

    private static boolean hasFilter(Function aggregate) {
        Symbol filter = aggregate.filter();
        return filter != null && !filter.equals(Literal.BOOLEAN_TRUE);
    }

    @Override
    public Pattern<HashAggregate> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(HashAggregate aggregate, Captures captures, Rule.Context context) {
        // List of partial aggregates that go into the GroupHashAggregate.
        // For example, `AVG(x)` needs two partial aggregates, `SUM(X)` and `COUNT(x)`,
        // that are then used to calculate the global `AVG(x)`.
        List<Function> partials = SplitDistinctAggregate.partials(aggregate.aggregates(), context.txnCtx(), context.nodeCtx());
        if (partials == null) {
            return null;
        }

        Symbol distinctColumn = distinctColumn(aggregate.aggregates());
        GroupHashAggregate inner = new GroupHashAggregate(
            aggregate.sources().getFirst(),
            List.of(distinctColumn),
            partials
        );
        return new HashAggregate(inner, aggregate.aggregates(), HashAggregate.DistinctMode.SPLIT_AND_MERGE);
    }

    private static Symbol distinctColumn(List<Function> aggregates) {
        for (Function fn : aggregates) {
            if (fn.distinct()) {
                return fn.arguments().get(0);
            }
        }
        throw new IllegalStateException("matches() guarantees a distinct aggregate is present");
    }
}
