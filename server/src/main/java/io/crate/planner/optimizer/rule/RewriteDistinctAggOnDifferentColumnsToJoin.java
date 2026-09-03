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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.HashAggregate;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

/// Splits a global aggregate with `DISTINCT` functions over *different* columns into one
/// branch per column, combined with a cross join, so [[RewriteDistinctAggToGroupBy]] can
/// deduplicate each branch on its own `GROUP BY`.
///
/// e.g. for
/// ```
/// SELECT count(DISTINCT a), count(DISTINCT b) FROM t
/// ```
///
/// ```
/// HashAggregate[count(DISTINCT a), count(DISTINCT b)]
///   └ Collect[doc.t | [a, b] | true]
/// ```
///
/// becomes
///
/// ```
/// Eval[count(DISTINCT a), count(DISTINCT b)]
///   └ Join[CROSS]
///       ├ HashAggregate[count(DISTINCT a)]
///       │    └ Collect[doc.t | [a, b] | true]
///       └ HashAggregate[count(DISTINCT b)]
///            └ Collect[doc.t | [a, b] | true]
/// ```
///
/// Each branch reuses the same source, so `WHERE` (baked into that source) applies to every
/// branch unchanged. A parent `HAVING` filter references the original `Function` outputs, so
/// the `Eval` on top reprojects to `aggregate.aggregates()` verbatim, same instances, same
/// order, to keep the operator's outputs unchanged.
///
/// The rule only splits functions on different columns; a single shared column is left to
/// [[RewriteDistinctAggToGroupBy]].
public final class RewriteDistinctAggOnDifferentColumnsToJoin implements Rule<HashAggregate> {

    private final Pattern<HashAggregate> pattern;

    public RewriteDistinctAggOnDifferentColumnsToJoin() {
        this.pattern = typeOf(HashAggregate.class)
            .with(this::isGlobalDistinctAggOnMultipleColumns);
    }

    private boolean isGlobalDistinctAggOnMultipleColumns(HashAggregate agg) {
        List<Function> aggregates = agg.aggregates();
        boolean allSupportedDistinct = aggregates.stream().allMatch(fn ->
            fn.distinct() &&
                Util.hasNoFilter(fn) &&
                Util.DISTINCT_REWRITE_SUPPORTED_AGGREGATES.contains(fn.name()) &&
                // NB: distinct functions can have one and only one argument
                !fn.arguments().isEmpty() &&
                fn.arguments().getFirst() instanceof Reference
        );
        if (!allSupportedDistinct) {
            return false;
        }
        return groupByColumn(aggregates).size() > 1;
    }

    private static Map<Symbol, List<Function>> groupByColumn(List<Function> aggregates) {
        Map<Symbol, List<Function>> byColumn = new LinkedHashMap<>();
        for (Function fn : aggregates) {
            byColumn.computeIfAbsent(fn.arguments().getFirst(), k -> new ArrayList<>()).add(fn);
        }
        return byColumn;
    }

    @Override
    public Pattern<HashAggregate> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(HashAggregate aggregate, Captures captures, Rule.Context context) {
        LogicalPlan source = aggregate.sources().getFirst();
        Map<Symbol, List<Function>> byColumn = groupByColumn(aggregate.aggregates());

        LogicalPlan joined = null;
        for (List<Function> columnAggregates : byColumn.values()) {
            LogicalPlan branch = new HashAggregate(source, columnAggregates);
            joined = joined == null ? branch : new JoinPlan(joined, branch, JoinType.CROSS, null);
        }

        return Eval.create(joined, List.copyOf(aggregate.aggregates()));
    }
}
