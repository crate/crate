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

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Fetch;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.statistics.TableStats;
import io.crate.types.DataTypes;

public final class RewriteToQueryThenFetch implements Rule<Limit> {

    private final Capture<Collect> collectCapture;
    private final Pattern<Limit> pattern;

    public RewriteToQueryThenFetch() {
        this.collectCapture = new Capture<>();
        this.pattern = typeOf(Limit.class)
            .with(
                source(),
                typeOf(Collect.class)
                    .with(c -> c.relation() instanceof DocTableRelation)
                    .capturedAs(collectCapture)
            );
    }

    @Override
    public Pattern<Limit> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Limit limit,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx) {
        Collect collect = captures.get(collectCapture);
        DocTableRelation relation = (DocTableRelation) collect.relation();
        Reference fetchRef = DocSysColumns.forTable(relation.relationName(), DocSysColumns.FETCHID);
        if (collect.outputs().contains(fetchRef)) {
            return null;
        }
        ArrayList<Symbol> newOutputs = new ArrayList<>();
        FetchSource fetchSource = new FetchSource();
        ArrayList<Reference> allFetchRefs = new ArrayList<>();
        for (Symbol output : collect.outputs()) {
            if (Symbols.containsColumn(output, DocSysColumns.SCORE)) {
                newOutputs.add(output);
            } else if (!SymbolVisitors.any(Symbols.IS_COLUMN, output)) {
                newOutputs.add(output);
            } else {
                RefVisitor.visitRefs(output, ref -> {
                    fetchSource.addRefToFetch(ref);
                    allFetchRefs.add(ref);
                });
            }
        }
        if (newOutputs.size() == collect.outputs().size()) {
            return null;
        }
        newOutputs.add(0, fetchRef);
        fetchSource.addFetchIdColumn(new InputColumn(0, DataTypes.LONG));
        Collect newCollect = new Collect(
            collect.preferSourceLookup(),
            relation,
            newOutputs,
            collect.where(),
            collect.numExpectedRows(),
            collect.estimatedRowSize()
        );
        return new Fetch(
            collect.outputs(),
            allFetchRefs,
            Map.of(relation.relationName(), fetchSource),
            limit.replaceSources(List.of(newCollect))
        );
    }
}
