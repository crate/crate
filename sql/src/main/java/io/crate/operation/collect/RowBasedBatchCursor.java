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

package io.crate.operation.collect;

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.Row;
import io.crate.operation.data.BatchCursor;

import java.util.Iterator;

/**
 * This class provides a cursor over a given row iterable.
 *
 * NOTE: this is mostly used for migration purposes from the old Row based execution.
 * In most cases it would be better to implement a special cursor without having an intermediate iterable.
 */
public class RowBasedBatchCursor implements BatchCursor {

    private volatile Iterator<Row> iter;
    private volatile Row currentRow;
    private final Iterable<Row> rows;

    public RowBasedBatchCursor(Iterable<Row> rows) {
        this.rows = rows;
        moveFirst();
    }

    @Override
    public boolean moveFirst() {
        System.err.println("RowsCollector: moveFirst");
        iter = rows.iterator();
        return moveNext();
    }

    @Override
    public boolean moveNext() {
        assert iter != null : "iterator must not be null";
        if (iter.hasNext()) {
            currentRow = iter.next();
            System.err.println("RowsCollector: moveNext onRow=" + true);
            return true;
        } else {
            currentRow = null;
            System.err.println("RowsCollector: moveNext onRow=" + false);
            return false;
        }
    }

    @Override
    public void close() {
        System.err.println("RowsCollector: close -------------------------------------");
//            Thread.dumpStack();
//            System.err.println("RowsCollector: close -------------------------------------");
        iter = null;
        currentRow = null;
    }

    @Override
    public Status status() {
        if (currentRow != null) {
            return Status.ON_ROW;
        } else if (iter == null) {
            return Status.CLOSED;
        }
        return Status.OFF_ROW;
    }

    @Override
    public ListenableFuture<?> loadNextBatch() {
        throw new IllegalStateException("No more batches");
    }

    @Override
    public boolean allLoaded() {
        return true;
    }

    private void ensureOnRow() {
        if (currentRow == null) {
            throw new IllegalStateException("Not on a row");
        }
    }

    @Override
    public int size() {
        ensureOnRow();
        return currentRow.size();
    }

    @Override
    public Object get(int index) {
        ensureOnRow();
        return currentRow.get(index);
    }

    @Override
    public Object[] materialize() {
        ensureOnRow();
        return currentRow.materialize();
    }
}
