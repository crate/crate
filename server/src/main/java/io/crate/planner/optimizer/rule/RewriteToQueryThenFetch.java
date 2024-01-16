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
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.Fetch;
import io.crate.planner.operators.FetchRewrite;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.Order;
import io.crate.planner.operators.Rename;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.planner.optimizer.matcher.Pattern;

public final class RewriteToQueryThenFetch implements Rule<Limit> {

    private static final Pattern<Order> ORDER_COLLECT = typeOf(Order.class)
        .with(source(), typeOf(Collect.class));

    private static final Pattern<Rename> RENAME_ORDER_COLLECT = typeOf(Rename.class)
        .with(
            source(),
            typeOf(Order.class)
                .with(source(), typeOf(Collect.class))
        );

    private final Pattern<Limit> pattern;

    public RewriteToQueryThenFetch() {
        this.pattern = typeOf(Limit.class);
    }

    @Override
    public Pattern<Limit> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Limit limit,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {
        if (Symbols.containsColumn(limit.outputs(), DocSysColumns.FETCHID)) {
            return null;
        }
        FetchRewrite fetchRewrite = limit.source().rewriteToFetch(Set.of());
        if (fetchRewrite == null) {
            return null;
        }
        List<Reference> fetchRefs = fetchRewrite.extractFetchRefs();
        Map<RelationName, FetchSource> fetchSourceByRelation = fetchRewrite.createFetchSources();
        return new Fetch(
            fetchRewrite.replacedOutputs(),
            fetchRefs,
            fetchSourceByRelation,
            limit.replaceSources(List.of(fetchRewrite.newPlan()))
        );
    }


    public static LogicalPlan tryRewrite(AnalyzedRelation relation, LogicalPlan plan) {
        Match<?> match = ORDER_COLLECT.accept(plan, Captures.empty());
        if (match.isPresent()) {
            return doRewrite(relation, plan);
        }
        match = RENAME_ORDER_COLLECT.accept(plan, Captures.empty());
        if (match.isPresent()) {
            return doRewrite(relation, plan);
        }
        return plan;
    }

    private static LogicalPlan doRewrite(AnalyzedRelation relation, LogicalPlan plan) {
        FetchRewrite fetchRewrite = plan.rewriteToFetch(List.of());
        if (fetchRewrite == null) {
            return plan;
        }
        List<Reference> fetchRefs = fetchRewrite.extractFetchRefs();
        Map<RelationName, FetchSource> fetchSourceByRelation = fetchRewrite.createFetchSources();
        Fetch fetch = new Fetch(
            fetchRewrite.replacedOutputs(),
            fetchRefs,
            fetchSourceByRelation,
            fetchRewrite.newPlan()

        );
        return Eval.create(fetch, relation.outputs());
    }
}
