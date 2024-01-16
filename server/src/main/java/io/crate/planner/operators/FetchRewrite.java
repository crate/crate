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

package io.crate.planner.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import io.crate.expression.symbol.FetchMarker;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.planner.node.fetch.FetchSource;

public final class FetchRewrite {

    private final Map<Symbol, Symbol> replacedOutputs;
    private final LogicalPlan plan;

    /**
     * @param replacedOutputs See {@link #replacedOutputs()}
     */
    public FetchRewrite(Map<Symbol, Symbol> replacedOutputs, LogicalPlan plan) {
        this.replacedOutputs = replacedOutputs;
        this.plan = plan;
    }

    public LogicalPlan newPlan() {
        return plan;
    }

    public List<Reference> extractFetchRefs() {
        ArrayList<Reference> allFetchReferences = new ArrayList<>();
        for (Symbol output : plan.outputs()) {
            if (output instanceof FetchMarker) {
                allFetchReferences.addAll(((FetchMarker) output).fetchRefs());
            }
        }
        return allFetchReferences;
    }

    public Map<RelationName, FetchSource> createFetchSources() {
        HashMap<RelationName, FetchSource> fetchSources = new HashMap<>();
        List<Symbol> outputs = plan.outputs();
        for (int i = 0; i < outputs.size(); i++) {
            Symbol output = outputs.get(i);
            if (output instanceof FetchMarker) {
                FetchMarker fetchMarker = (FetchMarker) output;
                RelationName tableName = fetchMarker.fetchId().ident().tableIdent();
                FetchSource fetchSource = fetchSources.get(tableName);
                if (fetchSource == null) {
                    fetchSource = new FetchSource();
                    fetchSources.put(tableName, fetchSource);
                }
                fetchSource.addFetchIdColumn(new InputColumn(i, fetchMarker.valueType()));
                for (Reference fetchRef : fetchMarker.fetchRefs()) {
                    fetchSource.addRefToFetch(fetchRef);
                }
            }
        }
        return fetchSources;
    }

    /**
     * This contains a map from "previous" output, to "new" output.
     * For example:
     * <pre>
     *     Limit [5]
     *      └ Eval [x + x]
     *        └ Collect [x]
     *
     *  Could be rewritten to:
     *
     *      Fetch [x + x]
     *       └ Limit [5]
     *         └ Eval [_fetchId]  with replacedOutputs `x + x` → `FetchStub(_fetchId, x) + FetchStub(_fetchId, x)`
     *           └ Collect [_fetchId]  with replacedOutputs `x` → `FetchStub(_fetchId, x)`
     * </pre>
     *
     * This Map must contain 1 entry for each symbol an operator previously contained in its outputs.
     */
    public Map<Symbol, Symbol> replacedOutputs() {
        return replacedOutputs;
    }

    /**
     * @return A function that converts any symbol within a symbol-tree that is present in `replacedOutputs` from the key to the value.
     */
    public UnaryOperator<Symbol> mapToFetchStubs() {
        return s -> MapBackedSymbolReplacer.convert(s, replacedOutputs);
    }
}
