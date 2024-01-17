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

package io.crate.execution.engine.fetch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;

import io.crate.common.collections.Lists;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.UnsafeArrayRow;
import io.crate.expression.BaseImplementationSymbolVisitor;
import io.crate.expression.InputRow;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.node.fetch.FetchSource;

/**
 * Maps a incoming row to a fetch operation to the expected outgoing row.
 *
 * <pre>
 *  updatedOutputRow(incomingCells, readerBuckets) will return a Row with the requested output.
 *
 *  Example:
 *
 *  Input cells format:
 *
 *         ┌ rel r1    ┌ rel r2
 *         │           │
 *      _fetchId | _fetchId | x | y
 *
 *
 * Expected output (described via outputSymbols):
 *
 *                                           ┌ points to pos 2 in incomingCells
 *                                           │
 *      FetchRef(0, a) | FetchRef(0, b) | IC(2) | FetchRef(1, a) | IC(3)
 *               │  │
 *               │  └ value is retrieved via ReaderBuckets. See below.
 *               │
 *               │
 *               └ points to _fetchId at pos 0 in incomingCells
 *
 *
 * _fetchId → readerId → readerBuckets
 *          │               └ readerBucket \
 *          │                                 → fetched row
 *          → _docId --------------------- /
 *
 * Each readerBucket contains a fetchedRow per _docId
 * The position of `a` within the fetched row is based on the index of `a` within `fetchSource.references()`
 * </pre>
 */
public final class FetchRows {

    public static FetchRows create(TransactionContext txnCtx,
                                   NodeContext nodeCtx,
                                   Map<RelationName, FetchSource> fetchSourceByTable,
                                   List<Symbol> outputSymbols) {
        IntArrayList fetchIdPositions = new IntArrayList();
        ArrayList<Object[]> nullRows = new ArrayList<>();
        IntObjectHashMap<UnsafeArrayRow> fetchedRows = new IntObjectHashMap<>();
        for (var fetchSource : fetchSourceByTable.values()) {
            Object[] nullRow = new Object[fetchSource.references().size()];
            for (InputColumn ic : fetchSource.fetchIdCols()) {
                fetchIdPositions.add(ic.index());
                nullRows.add(nullRow);
                fetchedRows.put(ic.index(), new UnsafeArrayRow());
            }
        }
        final UnsafeArrayRow inputRow = new UnsafeArrayRow();
        var visitor = new BaseImplementationSymbolVisitor<Void>(txnCtx, nodeCtx) {

            @Override
            public Input<?> visitInputColumn(final InputColumn inputColumn, final Void context) {
                final int idx = inputColumn.index();
                return () -> inputRow.get(idx);
            }

            @Override
            public Input<?> visitFetchReference(final FetchReference fetchReference, final Void context) {
                var ref = fetchReference.ref();
                UnsafeArrayRow row = fetchedRows.get(fetchReference.fetchId().index());
                int posInFetchedRow = fetchSourceByTable.get(ref.ident().tableIdent()).references().indexOf(ref);
                return () -> row.get(posInFetchedRow);
            }
        };
        List<Input<?>> outputExpressions = Lists.map(outputSymbols, x -> x.accept(visitor, null));
        return new FetchRows(fetchIdPositions, outputExpressions, inputRow, fetchedRows, nullRows);
    }

    /**
     * The indices in the incoming cells where there is a _fetchId value
     */
    private final int[] fetchIdPositions;

    /**
     * Row that is always mutated to contain the values of the current incoming row
     **/
    private final UnsafeArrayRow inputRow;

    /**
     * Outgoing row. Encapsulates the value retrieval from incoming row and readerBuckets.
     **/
    private final Row output;

    private final IntObjectHashMap<UnsafeArrayRow> fetchedRows;


    /**
     * Contains an entry per fetchIdPosition.
     * Each entry contains only null values; the length matches `fetchSource.references().size`.
     *
     * This is used as substitute fetchedRow if the _fetchId in the incoming row is null.
     */
    private final ArrayList<Object[]> nullRows;


    public FetchRows(IntArrayList fetchIdPositions,
                     List<Input<?>> outputExpressions,
                     UnsafeArrayRow inputRow,
                     IntObjectHashMap<UnsafeArrayRow> fetchedRows,
                     ArrayList<Object[]> nullRows) {
        this.fetchedRows = fetchedRows;
        this.nullRows = nullRows;
        this.fetchIdPositions = fetchIdPositions.toArray();
        this.output = new InputRow(outputExpressions);
        this.inputRow = inputRow;
    }

    public Row updatedOutputRow(Object[] incomingCells, IntFunction<ReaderBucket> getReaderBucket) {
        for (int i = 0; i < fetchIdPositions.length; i++) {
            int fetchIdPos = fetchIdPositions[i];
            Long fetchId = (Long) incomingCells[fetchIdPos];
            UnsafeArrayRow fetchedRow = fetchedRows.get(fetchIdPos);
            if (fetchId == null) {
                fetchedRow.cells(nullRows.get(i));
            } else {
                int readerId = FetchId.decodeReaderId(fetchId);
                int docId = FetchId.decodeDocId(fetchId);
                ReaderBucket readerBucket = getReaderBucket.apply(readerId);
                Object[] cells = readerBucket.get(docId);
                assert cells != null : "Must have cells in readerBucket docId=" + docId + ", readerId=" + readerId;
                fetchedRow.cells(cells);
            }
        }
        inputRow.cells(incomingCells);
        return output;
    }

    public int[] fetchIdPositions() {
        return fetchIdPositions;
    }
}
