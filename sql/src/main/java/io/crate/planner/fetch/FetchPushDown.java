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
import com.google.common.collect.ImmutableList;
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

        private final OrderBy orderBy;
        private LinkedHashSet<Symbol> querySymbols;
        private LinkedHashMap<Symbol, InputColumn> inputColumns;

        private Context(@Nullable OrderBy orderBy) {
            this.orderBy = orderBy;
            if (orderBy != null) {
                querySymbols = new LinkedHashSet<>(orderBy.orderBySymbols().size() + 1);
                querySymbols.addAll(orderBy.orderBySymbols());
            }
        }

        boolean isQuerySymbol(Symbol symbol) {
            return querySymbols != null && querySymbols.contains(symbol);
        }

        void allocateQuerySymbol(Symbol symbol) {
            if (querySymbols == null) {
                querySymbols = new LinkedHashSet<>(1);
            }
            querySymbols.add(symbol);
        }

        List<Symbol> orderByInputs() {
            if (orderBy == null) {
                return ImmutableList.of();
            }
            ArrayList<Symbol> result = new ArrayList<>(orderBy.orderBySymbols().size());
            int i = 0;
            Iterator<InputColumn> iter = inputColumns().values().iterator();
            while (i < orderBy.orderBySymbols().size()) {
                result.add(iter.next());
                i++;
            }
            return result;
        }

        LinkedHashMap<Symbol, InputColumn> inputColumns() {
            if (inputColumns == null) {
                if (querySymbols == null || querySymbols.isEmpty()) {
                    inputColumns = EMPTY_INPUTS;
                } else {
                    inputColumns = new LinkedHashMap<>(querySymbols.size());
                    int i = 1;
                    for (Symbol querySymbol : querySymbols) {
                        inputColumns.put(querySymbol, new InputColumn(i++, querySymbol.valueType()));
                    }
                }
            }
            return inputColumns;
        }
    }

    @Nullable
    public static QuerySpec pushDown(QuerySpec querySpec, TableIdent tableIdent) {
        assert querySpec.groupBy() == null && querySpec.having() == null && !querySpec.hasAggregates();

        OrderBy orderBy = querySpec.orderBy();

        Context context = new Context(orderBy);

        boolean fetchRequired = fetchRequiredVisitor.process(querySpec.outputs(), context);
        if (!fetchRequired) return null;

        // build the subquery
        QuerySpec sub = new QuerySpec();
        Reference docIdReference = new Reference(DocSysColumns.forTable(tableIdent, DocSysColumns.DOCID));

        LinkedHashMap<Symbol, InputColumn> inputColumns = context.inputColumns();
        List<Symbol> outputs = new ArrayList<>();
        if (orderBy != null) {
            sub.orderBy(querySpec.orderBy());
            querySpec.orderBy(new OrderBy(context.orderByInputs(), orderBy.reverseFlags(), orderBy.nullsFirst()));
            outputs.add(docIdReference);
            outputs.addAll(context.inputColumns().keySet());
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

        // replace the columns with input columns;
        for (int i = 0; i < querySpec.outputs().size(); i++) {
            InputColumn col = inputColumns.get(querySpec.outputs().get(i));
            if (col != null) {
                querySpec.outputs().set(i, col);
            }
        }
        sub.limit(MoreObjects.firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT) + querySpec.offset());
        return sub;
    }

    private static class FetchRequiredVisitor extends SymbolVisitor<Context, Boolean> {

        public boolean process(List<Symbol> symbols, Context context) {
            boolean result = false;
            for (Symbol symbol : symbols) {
                result = process(symbol, context) || result;
            }
            return result;
        }

        @Override
        public Boolean visitReference(Reference symbol, Context context) {
            if (context.isQuerySymbol(symbol)) {
                return false;
            } else if (symbol.ident().columnIdent().equals(DocSysColumns.SCORE)) {
                context.allocateQuerySymbol(symbol);
                return false;
            }
            return true;
        }

        @Override
        public Boolean visitDynamicReference(DynamicReference symbol, Context context) {
            return visitReference(symbol, context);
        }

        @Override
        protected Boolean visitSymbol(Symbol symbol, Context context) {
            return false;
        }

        @Override
        public Boolean visitAggregation(Aggregation symbol, Context context) {
            return !context.isQuerySymbol(symbol) && process(symbol.inputs(), context);
        }

        @Override
        public Boolean visitFunction(Function symbol, Context context) {
            return !context.isQuerySymbol(symbol) && process(symbol.arguments(), context);
        }
    }
}
