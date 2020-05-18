/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.optimizer.rule;

import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.operators.Fetch;
import io.crate.planner.operators.FetchRewrite;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.statistics.TableStats;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

public final class RewriteToQueryThenFetch implements Rule<Limit> {

    private final Pattern<Limit> pattern;
    private volatile boolean enabled = true;

    public RewriteToQueryThenFetch() {
        this.pattern = typeOf(Limit.class);
    }

    @Override
    public Pattern<Limit> pattern() {
        return pattern;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public LogicalPlan apply(Limit limit,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx,
                             Functions functions) {
        if (Symbols.containsColumn(limit.outputs(), DocSysColumns.FETCHID)) {
            return null;
        }
        FetchRewrite fetchRewrite = limit.source().rewriteToFetch(tableStats, Set.of());
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
}
