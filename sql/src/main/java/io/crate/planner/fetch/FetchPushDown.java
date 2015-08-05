/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.fetch;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import io.crate.Constants;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.metadata.ScoreReferenceDetector;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.symbol.*;

import javax.annotation.Nullable;
import java.util.*;

public class FetchPushDown {

    private static final LinkedHashMap<Symbol, InputColumn> EMPTY_INPUTS = new LinkedHashMap<>(0);
    private static final FetchRequiredVisitor fetchRequiredVisitor = new FetchRequiredVisitor();
    private static final ScoreReferenceDetector SCORE_REFERENCE_DETECTOR = new ScoreReferenceDetector();

    private static class Context {

        private final LinkedHashMap<Symbol, InputColumn> requiredInSub;

        private Context(LinkedHashMap<Symbol, InputColumn> sub) {
            requiredInSub = sub;
        }

        public Set<Symbol> requiredSymbols() {
            if (requiredInSub != null) {
                return requiredInSub.keySet();
            }
            return ImmutableSet.of();
        }
    }

    @Nullable
    public static QuerySpec pushDown(QuerySpec querySpec, TableIdent tableIdent) {
        assert querySpec.groupBy() == null && querySpec.having() == null && !querySpec.hasAggregates();

        LinkedHashMap<Symbol, InputColumn> requiredInQuery;
        OrderBy orderBy = querySpec.orderBy();

        if (orderBy != null) {
            requiredInQuery = new LinkedHashMap<>(orderBy.orderBySymbols().size());
            int i = 1;
            for (Symbol symbol : orderBy.orderBySymbols()) {
                if (!requiredInQuery.containsKey(symbol)) {
                    requiredInQuery.put(symbol, new InputColumn(i++, symbol.valueType()));
                }
            }
        } else {
            requiredInQuery = EMPTY_INPUTS;
        }
        Context context = new Context(requiredInQuery);

        boolean fetchRequired = fetchRequiredVisitor.process(querySpec.outputs(), context.requiredSymbols());

        if (!fetchRequired) return null;

        // build the subquery
        QuerySpec sub = new QuerySpec();
        Reference docIdReference = new Reference(DocSysColumns.forTable(tableIdent, DocSysColumns.DOCID));

        List<Symbol> outputs = new ArrayList<>();
        if (orderBy != null) {
            sub.orderBy(querySpec.orderBy());
            querySpec.orderBy(new OrderBy(new ArrayList<Symbol>(context.requiredInSub.values()),
                    orderBy.reverseFlags(), orderBy.nullsFirst()));

            outputs.add(docIdReference);
            outputs.addAll(context.requiredInSub.keySet());
        } else {
            outputs.add(docIdReference);
        }
        for (Symbol symbol : querySpec.outputs()) {
            if (SCORE_REFERENCE_DETECTOR.detect(symbol) && !outputs.contains(symbol)) {
                outputs.add(symbol);
            }
        }
        sub.outputs(outputs);

        // push down the where clause
        sub.where(querySpec.where());
        querySpec.where(null);

        sub.limit(MoreObjects.firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT) + querySpec.offset());
        return sub;
    }

    private static class FetchRequiredVisitor extends SymbolVisitor<Collection<Symbol>, Boolean> {

        public boolean process(List<Symbol> symbols, Collection<Symbol> requiredInQuery) {
            for (Symbol symbol : symbols) {
                if (process(symbol, requiredInQuery)) return true;
            }
            return false;
        }

        @Override
        public Boolean visitReference(Reference symbol, Collection<Symbol> requiredInQuery) {
            return !requiredInQuery.contains(symbol);
        }

        @Override
        public Boolean visitDynamicReference(DynamicReference symbol, Collection<Symbol> requiredInQuery) {
            return visitReference(symbol, requiredInQuery);
        }

        @Override
        protected Boolean visitSymbol(Symbol symbol, Collection<Symbol> requiredInQuery) {
            return false;
        }

        @Override
        public Boolean visitAggregation(Aggregation symbol, Collection<Symbol> requiredInQuery) {
            return !requiredInQuery.contains(symbol) && process(symbol.inputs(), requiredInQuery);
        }

        @Override
        public Boolean visitFunction(Function symbol, Collection<Symbol> requiredInQuery) {
            return !requiredInQuery.contains(symbol) && process(symbol.arguments(), requiredInQuery);
        }
    }
}
