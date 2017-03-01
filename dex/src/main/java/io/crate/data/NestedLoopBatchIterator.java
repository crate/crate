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

package io.crate.data;

import java.util.concurrent.CompletionStage;

public class NestedLoopBatchIterator implements BatchIterator {

    private final BatchIterator left;
    private final BatchIterator right;
    private final CombinedColumn rowData;

    private BatchIterator activeIt;

    public NestedLoopBatchIterator(BatchIterator left, BatchIterator right) {
        this.left = left;
        this.right = right;
        this.activeIt = left;
        this.rowData = new CombinedColumn(left.rowData(), right.rowData());
    }

    @Override
    public Columns rowData() {
        return rowData;
    }

    @Override
    public void moveToStart() {
        left.moveToStart();
        right.moveToStart();
    }

    @Override
    public boolean moveNext() {
        if (activeIt == left) {
            return tryAdvanceLeftAndRight();
        } else {
            return tryAdvanceRight();
        }
    }

    private boolean tryAdvanceRight() {
        if (right.moveNext()) {
            return true;
        }
        if (right.allLoaded() == false) {
            return false;
        }

        // new outer loop iteration
        right.moveToStart();
        if (left.moveNext()) {
            return right.moveNext();
        }
        activeIt = left; // left is either out of rows or needs to load more data - whatever the case,
                         // next moveNext must operate on left
        return false;
    }

    private boolean tryAdvanceLeftAndRight() {
        while (left.moveNext()) {
            activeIt = right;
            if (right.moveNext()) {
                return true;
            }
            if (right.allLoaded() == false) {
                return false;
            }
            right.moveToStart();
        }
        return false;
    }

    @Override
    public void close() {
        left.close();
        right.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return activeIt.loadNextBatch();
    }

    @Override
    public boolean allLoaded() {
        return left.allLoaded() && right.allLoaded();
    }

    private static class CombinedColumn implements Columns {
        private final int numColumns;
        private final Columns left;
        private final Columns right;

        CombinedColumn(Columns left, Columns right) {
            this.left = left;
            this.right = right;
            numColumns = left.size() + right.size();
        }

        @Override
        public Input<?> get(int index) {
            if (index >= left.size()) {
                return right.get(index - left.size());
            }
            return left.get(index);
        }

        @Override
        public int size() {
            return numColumns;
        }
    }
}
