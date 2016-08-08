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

package io.crate.operation.fetch;

import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.InputColumn;
import io.crate.core.collections.Row;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.operation.BaseImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.projectors.fetch.FetchProjector;
import io.crate.planner.node.fetch.FetchSource;

import java.util.Map;

public class FetchRowInputSymbolVisitor extends BaseImplementationSymbolVisitor<FetchRowInputSymbolVisitor.Context> {

    public static class Context {

        private final FetchProjector.ArrayBackedRow[] fetchRows;
        private FetchProjector.ArrayBackedRow[] partitionRows;

        private Map<TableIdent, FetchSource> fetchSources;
        private final FetchProjector.ArrayBackedRow inputRow = new FetchProjector.ArrayBackedRow();
        private final int[] docIdPositions;
        private final Object[][] nullCells;

        public Context(Map<TableIdent, FetchSource> fetchSources) {
            assert !fetchSources.isEmpty();
            this.fetchSources = fetchSources;

            int numDocIds = 0;
            for (FetchSource fetchSource : fetchSources.values()) {
                numDocIds += fetchSource.docIdCols().size();
            }

            this.fetchRows = new FetchProjector.ArrayBackedRow[numDocIds];
            this.docIdPositions = new int[numDocIds];
            this.partitionRows = new FetchProjector.ArrayBackedRow[numDocIds];
            nullCells = new Object[numDocIds][];

            int idx = 0;
            for (FetchSource fetchSource : fetchSources.values()) {
                for (InputColumn col : fetchSource.docIdCols()) {
                    fetchRows[idx] = new FetchProjector.ArrayBackedRow();
                    docIdPositions[idx] = col.index();
                    if (!fetchSource.partitionedByColumns().isEmpty()) {
                        partitionRows[idx] = new FetchProjector.ArrayBackedRow();
                    }
                    nullCells[idx] = new Object[fetchSource.references().size()];
                    idx++;
                }
            }
        }

        /**
         * @return an array with the positions of the docIds in the input
         */
        public int[] docIdPositions() {
            return docIdPositions;
        }

        public FetchProjector.ArrayBackedRow[] fetchRows() {
            return fetchRows;
        }

        public FetchProjector.ArrayBackedRow[] partitionRows() {
            return partitionRows;
        }

        public FetchProjector.ArrayBackedRow inputRow() {
            return inputRow;
        }

        public Object[][] nullCells() {
            return nullCells;
        }

        public Input<?> allocateInput(int index) {
            return new RowInput(inputRow, index);
        }

        public Input<?> allocatePartitionedInput(FetchReference fetchReference) {
            int idx = -1;
            int fetchIdx = 0;
            FetchSource fs = null;
            for (FetchSource fetchSource : fetchSources.values()) {
                idx = fetchSource.partitionedByColumns().indexOf(fetchReference.ref());
                if (idx >= 0) {
                    for (InputColumn col : fetchSource.docIdCols()) {
                        if (col.equals(fetchReference.docId())){
                            fs = fetchSource;
                            break;
                        }
                    }
                    if (fs != null) {
                        break;
                    }
                }
                fetchIdx++;
            }
            assert fs != null;
            if (partitionRows == null) {
                partitionRows = new FetchProjector.ArrayBackedRow[fetchSources.size()];
            }
            FetchProjector.ArrayBackedRow row = partitionRows[fetchIdx];
            if (row == null){
                row = new FetchProjector.ArrayBackedRow();
                partitionRows[fetchIdx] = row;
            }
            return new RowInput(row, idx);
        }

        public Input<?> allocateInput(FetchReference fetchReference) {
            FetchSource fs = null;
            int fetchIdx = 0;
            for (Map.Entry<TableIdent, FetchSource> entry : fetchSources.entrySet()) {
                if (entry.getKey().equals(fetchReference.ref().ident().tableIdent())){
                    fs = entry.getValue();
                    for (InputColumn col : fs.docIdCols()) {
                        if (col.equals(fetchReference.docId())){
                            break;
                        }
                        fetchIdx++;
                    }
                    break;
                } else {
                    fetchIdx += entry.getValue().docIdCols().size();
                }
            }
            assert fs != null;
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
            assert input != null;
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
    }

    public FetchRowInputSymbolVisitor(Functions functions) {
        super(functions);
    }

    @Override
    public Input<?> visitInputColumn(InputColumn inputColumn, Context context) {
        return context.allocateInput(inputColumn.index());
    }

    @Override
    public Input<?> visitField(Field field, Context context) {
        return context.allocateInput(field.index());
    }

    @Override
    public Input<?> visitFetchReference(FetchReference fetchReference, Context context) {
        if (fetchReference.ref().granularity() == RowGranularity.DOC){
            return context.allocateInput(fetchReference);
        }
        assert fetchReference.ref().granularity() == RowGranularity.PARTITION;
        return context.allocatePartitionedInput(fetchReference);

    }
}
