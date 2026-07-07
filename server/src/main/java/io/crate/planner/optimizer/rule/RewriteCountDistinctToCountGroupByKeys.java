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
import static io.crate.planner.optimizer.matcher.Patterns.source;

import java.util.List;

import io.crate.execution.engine.aggregation.impl.CollectSetAggregation;
import io.crate.expression.scalar.CollectionCountFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.HashAggregate;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

/// Speeds up a global `count(DISTINCT col)` by deduplicating `col` first, so it counts the
/// distinct values instead of every row. The result is the same as
/// `select count(col) from (select col from t group by col) tmp`.
///
/// `count(DISTINCT col)` is planned as `collection_count(collect_set(col))`:
/// `collect_set` folds every one of the source rows into a `HashSet`,
/// `collection_count` returns its size. Here's how a non-optimized plan looks like:
///
/// ```
/// Eval[collection_count(collect_set(col))]
///   └ HashAggregate[collect_set(col)]
///     └ Collect[... | [col] | ...]
/// ```
///
/// This rule inserts a keys-only `GROUP BY` beneath the unchanged `collect_set`, so it only sees
/// the distinct values:
///
/// ```
/// Eval[collection_count(collect_set(col))]
///   └ HashAggregate[collect_set(col)]
///     └ GroupHashAggregate[col | ]
///       └ Collect[... | [col] | ...]
/// ```
///
/// Important: the scope of the rule is a single `collect_set` with no FILTER,
/// wrapped by exactly one `collection_count` that is the Eval's only output.
public final class RewriteCountDistinctToCountGroupByKeys implements Rule<Eval> {

    private final Capture<HashAggregate> aggCapture;
    private final Pattern<Eval> pattern;

    public RewriteCountDistinctToCountGroupByKeys() {
        this.aggCapture = new Capture<>();
        this.pattern = typeOf(Eval.class)
            .with(this::hasCollectionCountOnly)
            .with(
                source(),
                typeOf(HashAggregate.class)
                    .capturedAs(aggCapture)
                    .with(agg -> getSingleCollectSet(agg) != null)
            );
    }

    /// If `hashAgg` contains exactly one `collect_set` without any filters,
    /// then this method returns that. Otherwise, returns `null`.
    private Function getSingleCollectSet(HashAggregate hashAgg) {
        List<Function> aggs = hashAgg.aggregates();
        if (aggs.size() != 1) {
            return null;
        }
        Function collectSet = aggs.get(0);
        if (!collectSet.name().equals(CollectSetAggregation.NAME)) {
            return null;
        }
        Symbol filter = collectSet.filter();
        if (filter != null && !filter.equals(Literal.BOOLEAN_TRUE)) {
            // A FILTER would have to be pushed into the inner GROUP BY's WHERE
            return null;
        }
        return collectSet;
    }

    /// Returns true if the `Eval` has a single output: `collection_count(collect_set(col))`.
    private boolean hasCollectionCountOnly(Eval eval) {
        List<Symbol> outputs = eval.outputs();
        if (outputs.size() != 1) {
            return false;
        }
        return outputs.get(0) instanceof Function fn
            && fn.name().equals(CollectionCountFunction.NAME)
            && fn.arguments().size() == 1
            && fn.arguments().get(0) instanceof Function inner
            && inner.name().equals(CollectSetAggregation.NAME);
    }

    @Override
    public Pattern<Eval> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Eval eval, Captures captures, Rule.Context context) {
        HashAggregate aggregate = captures.get(aggCapture);
        Function collectSet = getSingleCollectSet(aggregate);
        Symbol col = collectSet.arguments().get(0);
        LogicalPlan collectSource = aggregate.sources().get(0);

        // `GROUP BY columnName` already present, so skip this rule.
        LogicalPlan resolvedSource = context.resolvePlan().apply(collectSource);
        if (resolvedSource instanceof GroupHashAggregate groupBy
                && groupBy.aggregates().isEmpty()
                && groupBy.groupKeys().equals(List.of(col))) {
            return null;
        }

        GroupHashAggregate dedup = new GroupHashAggregate(collectSource, List.of(col), List.of());
        HashAggregate newAggregate = new HashAggregate(dedup, aggregate.aggregates());
        return eval.replaceSources(List.of(newAggregate));
    }
}
