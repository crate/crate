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
import io.crate.execution.engine.aggregation.impl.average.AverageAggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.HashAggregate;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

/// Deduplicates the argument of a global `agg(DISTINCT col)` with a keys-only `GROUP BY`, so the
/// aggregate runs over the distinct values instead of over every row. Same result as
/// `select agg(col) from (select col from t group by col) tmp`.
///
/// ```
/// HashAggregate[count(DISTINCT b)]          HashAggregate[count(DISTINCT b)]
///   └ Collect[doc.t | [b] | true]     ->      └ GroupHashAggregate[b]
///                                               └ Collect[doc.t | [b] | true]
/// ```
///
/// The rule applies if all aggregation functions have the same argument.
///
/// The `distinct` flag stays in the function, because an optimization mustn't change the outputs of an operator.
///
/// However, we set a flag in the `HashAggregate`, so that it doesn't use the default implementation of `distinct`
/// (the replacement with `collection_agg(collect_set(x))`).
public final class RewriteDistinctAggToGroupBy implements Rule<HashAggregate> {
    /// Generally, the rule doesn't care about which aggregate functions are present.
    /// However, the default implementation of distinct functions is limited to the ones below
    /// (because it needs a matching `collection_*` scalar function), hence the rule is limited
    /// to those as well. Otherwise, we'd have a situation where an optimization enables more functions.
    private static final Set<String> SUPPORTED_AGGREGATES = Set.of(
        CountAggregation.NAME,
        AverageAggregation.NAMES[0],
        AverageAggregation.NAMES[1]
    );

    private final Pattern<HashAggregate> pattern;

    public RewriteDistinctAggToGroupBy() {
        this.pattern = typeOf(HashAggregate.class)
            .with(this::allAggregatesDistinctOnSameSingleColumn);
    }

    /// Currently, we only optimize aggregate operators where every function is over the same column,
    /// so we can use one sub-query with "group by".
    private boolean allAggregatesDistinctOnSameSingleColumn(HashAggregate agg) {
        List<Function> aggregates = agg.aggregates();
        if (aggregates.get(0).arguments().isEmpty()) {
            return false;
        }

        if (!(aggregates.get(0).arguments().get(0) instanceof Reference column)) {
            return false;
        }

        return aggregates.stream().allMatch(fn ->
            fn.distinct() &&
                !hasFilter(fn) &&
                SUPPORTED_AGGREGATES.contains(fn.name()) &&
                // NB: distinct functions can have one and only one argument
                fn.arguments().get(0).equals(column)
        );
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
        Function distinctAggregate = aggregate.aggregates().getFirst();
        Symbol groupKey = distinctAggregate.arguments().getFirst();
        LogicalPlan source = aggregate.sources().getFirst();

        LogicalPlan resolvedSource = context.resolvePlan().apply(source);
        if (groupByPresent(resolvedSource, groupKey)) {
            return null;
        }

        GroupHashAggregate dedup = new GroupHashAggregate(source, List.of(groupKey), List.of());
        return new HashAggregate(dedup, aggregate.aggregates(), false);
    }

    /// True if `plan` already groups by `groupKey`, i.e., a `GROUP BY` isn't needed.
    ///
    /// Happens for a query written as `select count(distinct col) from (select col from t group by col) tmp`.
    ///
    /// This is also what terminates the rule. This rule keeps the `distinct` flag in functions, so its result
    /// matches the pattern again. This method recognizes the `GROUP BY` that was inserted.
    private static boolean groupByPresent(LogicalPlan plan, Symbol groupKey) {
        return plan instanceof GroupHashAggregate groupBy
            && groupBy.aggregates().isEmpty()
            && groupBy.groupKeys().equals(List.of(groupKey));
    }
}
