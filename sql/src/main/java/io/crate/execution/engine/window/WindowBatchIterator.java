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

package io.crate.execution.engine.window;

import io.crate.analyze.WindowDefinition;
import io.crate.data.BatchIterator;
import io.crate.data.MappedForwardingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;

/**
 * BatchIterator that computes an aggregate or window function against a window over the source batch iterator.
 * Executing a function over a window means having to range over a set of rows from the source iterator. By default (ie.
 * empty window) the range is between unbounded preceding and current row. This means the function will need to be
 * computed against all the rows from the beginning of the partition and through the current row's _last peer_. The
 * function result will be emitted for every row in the window.
 * <p>
 *      For eg.
 * <p>
 *      source : [ 1, 2, 3, 4 ]
 * <p>
 *      function : sum
 * <p>
 *      window definition : empty (equivalent to an empty OVER() clause)
 * <p>
 *      window range is all the rows in the source as all rows are peers if there is no ORDER BY in the OVER clause
 * <p>
 *      Execution steps :
 * <p>
 *          1. compute the function while the rows are peers
 * <p>
 *                  1 + 2 + 3 + 4 = 10
 * <p>
 *          2. for each row in the window emit the function result
 * <p>
 *                  [ 10, 10, 10, 10 ]
 */
public class WindowBatchIterator extends MappedForwardingBatchIterator<Row, Row> {

    private final BatchIterator<Row> source;
    private final Collector<Row, ?, Iterable<Row>> collector;
    private final WindowDefinition windowDefinition;

    private Row currentElement;

    private int sourceRowsConsumed;
    private int windowRowPosition;
    private final List<Row> peerRows = new ArrayList<>();
    private boolean peersCollected = false;
    private Row prevRow = null;

    WindowBatchIterator(WindowDefinition windowDefinition,
                        BatchIterator<Row> source,
                        Collector<Row, ?, Iterable<Row>> collector) {
        assert windowDefinition.partitions().size() == 0 : "Only empty OVER() is supported now";
        assert windowDefinition.orderBy() == null : "Only empty OVER() is supported now";
        assert windowDefinition.windowFrameDefinition() == null : "Only empty OVER() is supported now";

        this.windowDefinition = windowDefinition;
        this.source = source;
        this.collector = collector;
    }

    @SuppressWarnings("unused")
    private boolean arePeers(@Nullable Row prevRow, Row nextRow) {
        // all rows are peers when orderBy is missing
        return windowDefinition.orderBy() == null;
    }

    @Override
    protected BatchIterator<Row> delegate() {
        return source;
    }

    @Override
    public Row currentElement() {
        return currentElement;
    }

    @Override
    public void moveToStart() {
        super.moveToStart();
        sourceRowsConsumed = 0;
        windowRowPosition = 0;
        peerRows.clear();
        prevRow = null;
        currentElement = null;
    }

    @Override
    public boolean moveNext() {
        if (peersCollected && windowRowPosition < sourceRowsConsumed) {
            // emit the result of the window function as we computed the result and not yet emitted it for every row
            // in the window
            windowRowPosition++;
            return true;
        }

        while (source.moveNext()) {
            sourceRowsConsumed++;
            Row sourceRow = source.currentElement();
            if (arePeers(prevRow, sourceRow)) {
                peersCollected = false;
                RowN materializedRow = new RowN(sourceRow.materialize());
                peerRows.add(materializedRow);
                prevRow = materializedRow;
            } else {
                // rows are not peers anymore so compute the window function and emit the result
                collectPeers();
                windowRowPosition++;
                return true;
            }
        }

        if (source.allLoaded()) {
            if (!peersCollected) {
                collectPeers();
            }

            if (windowRowPosition < sourceRowsConsumed) {
                // we still need to emit rows
                windowRowPosition++;
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    private void collectPeers() {
        Iterable<Row> rows = peerRows.stream().collect(collector);
        peerRows.clear();
        peersCollected = true;
        currentElement = rows.iterator().next();
        prevRow = null;
    }
}
