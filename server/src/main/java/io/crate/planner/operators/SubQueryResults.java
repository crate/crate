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
import java.util.List;
import java.util.Map;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;

import io.crate.data.Row;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.OuterColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.Except;

public class SubQueryResults {

    public static final SubQueryResults EMPTY = new SubQueryResults(Collections.emptyMap());
    private static final ObjectIntHashMap<OuterColumn> EMPTY_OUTER_COLUMNS = new ObjectIntHashMap<>();

    private final Map<SelectSymbol, Object> valuesBySubQuery;
    private final ObjectIntMap<OuterColumn> boundOuterColumns;

    private Row inputRow = Row.EMPTY;

    public SubQueryResults(Map<SelectSymbol, Object> valuesBySubQuery) {
        this.valuesBySubQuery = valuesBySubQuery;
        this.boundOuterColumns = new ObjectIntHashMap<>();
    }

    public SubQueryResults(Map<SelectSymbol, Object> valuesBySubQuery,
                           ObjectIntMap<OuterColumn> boundOuterColumns) {
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
        int index = boundOuterColumns.getOrDefault(key, -1);
        if (index == -1) {
            throw new IllegalArgumentException("Couldn't resolve value for OuterColumn: " + key);
        }
        try {
            return inputRow.get(index);
        } catch (Exception e) {
            return null;
        }
    }

    public SubQueryResults forCorrelation(SelectSymbol correlatedSubQuery, List<Symbol> subQueryOutputs) {
        ObjectIntHashMap<OuterColumn> outerColumnPositions = new ObjectIntHashMap<>();
        var visitor = new DefaultTraversalSymbolVisitor<Void, Void>() {

            @Override
            public Void visitOuterColumn(OuterColumn outerColumn, Void context) {
                int index = subQueryOutputs.indexOf(outerColumn);
                if (index < 0) {
                    throw new IllegalStateException(
                        "OuterColumn `" + outerColumn + "` must appear in input of CorrelatedJoin");
                }
                outerColumnPositions.put(outerColumn, index);
                return null;
            }
        };
        correlatedSubQuery.relation().visitSymbols(symbol -> symbol.accept(visitor, null));
        return new SubQueryResults(this.valuesBySubQuery, outerColumnPositions);
    }

    public void bindOuterColumnInputRow(Row inputRow) {
        this.inputRow = inputRow;
    }
}
