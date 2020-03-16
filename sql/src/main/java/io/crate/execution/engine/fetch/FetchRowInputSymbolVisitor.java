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

package io.crate.execution.engine.fetch;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.UnsafeArrayRow;
import io.crate.expression.BaseImplementationSymbolVisitor;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.InputColumn;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.node.fetch.FetchSource;

import java.util.Map;

public class FetchRowInputSymbolVisitor extends BaseImplementationSymbolVisitor<FetchRowInputSymbolVisitor.Context> {

    public static class Context {

        private final UnsafeArrayRow[] fetchRows;
        private final Map<RelationName, FetchSource> fetchSources;
        private final UnsafeArrayRow inputRow = new UnsafeArrayRow();
        private final int[] fetchIdPositions;
        private final Object[][] nullCells;


        public Context(Map<RelationName, FetchSource> fetchSources) {
            assert !fetchSources.isEmpty() : "fetchSources must not be empty";
            this.fetchSources = fetchSources;

            int numFetchIds = 0;
            for (FetchSource fetchSource : fetchSources.values()) {
                numFetchIds += fetchSource.fetchIdCols().size();
            }

            this.fetchRows = new UnsafeArrayRow[numFetchIds];
            this.fetchIdPositions = new int[numFetchIds];
            nullCells = new Object[numFetchIds][];

            int idx = 0;
            for (FetchSource fetchSource : fetchSources.values()) {
                for (InputColumn col : fetchSource.fetchIdCols()) {
                    fetchRows[idx] = new UnsafeArrayRow();
                    fetchIdPositions[idx] = col.index();
                    nullCells[idx] = new Object[fetchSource.references().size()];
                    idx++;
                }
            }
        }

        /**
         * @return an array with the positions of the fetchIds in the input
         */
        public int[] fetchIdPositions() {
            return fetchIdPositions;
        }

        public UnsafeArrayRow[] fetchRows() {
            return fetchRows;
        }

        public UnsafeArrayRow inputRow() {
            return inputRow;
        }

        public Object[][] nullCells() {
            return nullCells;
        }

        public Input<?> allocateInput(int index) {
            return new RowInput(inputRow, index);
        }

        public Input<?> allocateInput(FetchReference fetchReference) {
            FetchSource fs = null;
            int fetchIdx = 0;
            for (Map.Entry<RelationName, FetchSource> entry : fetchSources.entrySet()) {
                if (entry.getKey().equals(fetchReference.ref().ident().tableIdent())) {
                    fs = entry.getValue();
                    for (InputColumn col : fs.fetchIdCols()) {
                        if (col.equals(fetchReference.fetchId())) {
                            break;
                        }
                        fetchIdx++;
                    }
                    break;
                } else {
                    fetchIdx += entry.getValue().fetchIdCols().size();
                }
            }
            assert fs != null : "fs must not be null";
            Row row = fetchRows[fetchIdx];
            int idx = 0;
            RowInput input = null;
            for (Reference reference : fs.references()) {
                if (reference.equals(fetchReference.ref())) {
                    input = new RowInput(row, idx);
                    break;
                }
                idx++;
            }
            assert input != null : "FetchReference " + fetchReference.ref() + " must be present in fetchRefs: "
                    + fs.references();
            return input;
        }

    }

    static class RowInput implements Input<Object> {

        private final Row row;
        private final int index;

        public RowInput(Row row, int index) {
            this.row = row;
            this.index = index;
        }

        @Override
        public Object value() {
            return row.get(index);
        }

        @Override
        public String toString() {
            return "RowInput{" + index + ", row=" + row + '}';
        }
    }

    public FetchRowInputSymbolVisitor(TransactionContext txnCtx, Functions functions) {
        super(txnCtx, functions);
    }

    @Override
    public Input<?> visitInputColumn(InputColumn inputColumn, Context context) {
        return context.allocateInput(inputColumn.index());
    }

    @Override
    public Input<?> visitField(ScopedSymbol field, Context context) {
        throw new UnsupportedOperationException(
            "Encountered " + field + " but Fields should have been changed to InputColumns already");
    }

    @Override
    public Input<?> visitFetchReference(FetchReference fetchReference, Context context) {
        return context.allocateInput(fetchReference);
    }
}
