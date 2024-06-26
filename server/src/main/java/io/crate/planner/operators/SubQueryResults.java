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
import io.crate.expression.symbol.OuterColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;

public class SubQueryResults {
    // Static fields on the jvm are initialized in their order.
    // EMPTY_OUTER_COLUMNS must be created before EMPTY to be referenced successfully insides EMPTY.
    private static final ObjectIntHashMap<OuterColumn> EMPTY_OUTER_COLUMNS = new ObjectIntHashMap<>();
    public static final SubQueryResults EMPTY = new SubQueryResults(Collections.emptyMap());


    private final Map<SelectSymbol, Object> valuesBySubQuery;
    private final ObjectIntMap<OuterColumn> boundOuterColumns;

    private Row inputRow = Row.EMPTY;

    public SubQueryResults(Map<SelectSymbol, Object> valuesBySubQuery) {
        this.valuesBySubQuery = valuesBySubQuery;
        this.boundOuterColumns = EMPTY_OUTER_COLUMNS;
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
        return inputRow.get(index);
    }

    public SubQueryResults forCorrelation(SelectSymbol correlatedSubQuery, List<Symbol> subQueryOutputs) {
        ObjectIntHashMap<OuterColumn> outerColumnPositions = new ObjectIntHashMap<>();
        correlatedSubQuery.relation().visitSymbols(tree -> tree.visit(OuterColumn.class, outerColumn -> {
            int index = subQueryOutputs.indexOf(outerColumn.symbol());
            if (index < 0) {
                throw new IllegalStateException(
                    "OuterColumn `" + outerColumn + "` must appear in input of CorrelatedJoin");
            }
            outerColumnPositions.put(outerColumn, index);
        }));
        return new SubQueryResults(this.valuesBySubQuery, outerColumnPositions);
    }

    public void bindOuterColumnInputRow(Row inputRow) {
        this.inputRow = inputRow;
    }

    public static SubQueryResults merge(SubQueryResults subQueryResults1, SubQueryResults subQueryResults2) {
        // Correlated subquery related fields.
        Row mergedRow = subQueryResults1.inputRow;
        ObjectIntMap<OuterColumn> mergedBoundOuterColumns = subQueryResults1.boundOuterColumns;
        if (mergedRow.equals(Row.EMPTY)) {
            mergedRow = subQueryResults2.inputRow;
        }
        if (mergedBoundOuterColumns.equals(EMPTY_OUTER_COLUMNS)) {
            mergedBoundOuterColumns = subQueryResults2.boundOuterColumns;
        }

        // Regular sub-select field.
        Map<SelectSymbol, Object> mergedValuesBySubQuery = subQueryResults1.valuesBySubQuery;
        if (mergedValuesBySubQuery.isEmpty()) {
            mergedValuesBySubQuery = subQueryResults2.valuesBySubQuery;
        }

        SubQueryResults combinedSubQueryResults = new SubQueryResults(mergedValuesBySubQuery, mergedBoundOuterColumns);
        combinedSubQueryResults.bindOuterColumnInputRow(mergedRow);
        return combinedSubQueryResults;
    }
}
