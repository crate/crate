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

import java.util.List;
import java.util.Set;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.statistics.TableStats;

public final class MoveFilterBeneathFetchOrEval implements Rule<Filter> {

    private final Capture<Eval> fetchOrEvalCapture;
    private final Pattern<Filter> pattern;

    public MoveFilterBeneathFetchOrEval() {
        this.fetchOrEvalCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(), typeOf(Eval.class).capturedAs(fetchOrEvalCapture));
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter plan, Captures captures, TableStats tableStats, TransactionContext txnCtx, NodeContext nodeCtx) {
        Eval eval = captures.get(fetchOrEvalCapture);
        List<Symbol> sourceOutputs = eval.source().outputs();
        Set<Symbol> filterColumns = extractColumns(plan.query());
        boolean[] intersects = new boolean[] { false };
        for (Symbol filterColumn : filterColumns) {
            intersects[0] = false;
            // Move also for cases like `o['x']` where the source only provides `o`.
            // This makes `MergeFilterAndCollect` application more likely.
            // Even if `MergeFilterAndCollect` doesn't apply, the `Filter` operator can fallback
            // to use a `subscript(o, 'x')` function if `o['x']` as ref is not accessible.
            SymbolVisitors.intersection(filterColumn, sourceOutputs, s -> {
                intersects[0] = true;
            });
            if (!intersects[0]) {
                return null;
            }
        }
        return transpose(plan, eval);
    }
}
