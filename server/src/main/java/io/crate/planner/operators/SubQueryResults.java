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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.crate.data.Row;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.OuterColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;

public class SubQueryResults {

    public static final SubQueryResults EMPTY = new SubQueryResults(Collections.emptyMap());

    private final Map<SelectSymbol, Object> valuesBySubQuery;
    private final Map<OuterColumn, Object> boundOuterColumns;

    public SubQueryResults(Map<SelectSymbol, Object> valuesBySubQuery) {
        this.valuesBySubQuery = valuesBySubQuery;
        this.boundOuterColumns = Map.of();
    }

    public SubQueryResults(Map<SelectSymbol, Object> valuesBySubQuery,
                           Map<OuterColumn, Object> boundOuterColumns) {
        this.valuesBySubQuery = valuesBySubQuery;
        this.boundOuterColumns = boundOuterColumns;
    }

    public Object getSafe(SelectSymbol key) {
        Object value = valuesBySubQuery.get(key);
        if (value == null && !valuesBySubQuery.containsKey(key)) {
            throw new IllegalArgumentException("Couldn't resolve value for subQuery: " + key);
        }
        return value;
    }

    public Object get(OuterColumn key) {
        Object value = boundOuterColumns.get(key);
        if (value == null && !boundOuterColumns.containsKey(key)) {
            throw new IllegalArgumentException("Couldn't resolve value for OuterColumn: " + key);
        }
        return value;
    }

    public SubQueryResults merge(SelectSymbol selectSymbol, List<Symbol> subQueryOutputs, Row row) {
        HashMap<OuterColumn, Object> boundOuterColumns = new HashMap<>();
        selectSymbol.relation().visitSymbols(symbol -> {
            symbol.accept(new DefaultTraversalSymbolVisitor<Void, Void>() {

                @Override
                public Void visitOuterColumn(OuterColumn outerColumn, Void context) {
                    int index = subQueryOutputs.indexOf(outerColumn.symbol());
                    if (index < 0) {
                        throw new IllegalStateException("OuterColumn must appear in input of CorrelatedJoin");
                    }
                    boundOuterColumns.put(outerColumn, row.get(index));
                    return null;
                }
            }, null);
        });
        return new SubQueryResults(this.valuesBySubQuery, boundOuterColumns);
    }
}
