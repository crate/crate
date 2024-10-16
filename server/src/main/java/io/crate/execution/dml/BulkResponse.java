/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.dml;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import com.carrotsearch.hppc.IntCollection;
import com.carrotsearch.hppc.cursors.IntCursor;

import io.crate.data.Row1;

public final class BulkResponse {

    private final int size;
    private final long[] rowCounts;
    @Nullable
    private final Throwable[] failures;

    public BulkResponse(int size) {
        this.size = size;
        rowCounts = new long[size];
        failures = new Throwable[size];
    }

    /**
     * Create bulk-response depending on number of bulk responses
     * <pre>
     *     compressedResult
     *          success: [1, 1, 1, 1]
     *          failure: []
     *
     *     insert into t (x) values (?), (?)   -- bulkParams: [[1, 2], [3, 4]]
     *     Response:
     *      [2, 2]
     *
     *     insert into t (x) values (?)        -- bulkParams: [[1], [2], [3], [4]]
     *     Response:
     *      BulkResponse (rowCounts=[1, 1, 1, 1], failures=[null, null, null, null])
     * </pre>
     */
    public BulkResponse update(ShardResponse.CompressedResult result, IntCollection items) {
        for (IntCursor c : items) {
            int itemLocation = c.index;
            int resultIdx = c.value;
            if (result.successfulWrites(itemLocation)) {
                incrementRowCount(resultIdx);
            } else if (result.failed(itemLocation)) {
                setFailure(resultIdx, result.failure(itemLocation));
            }
        }
        return this;
    }

    /**
     * Update the bulk response with the row count and failure for the given bulk parameter index.
     *
     * @param idx       the index of the bulk parameter
     * @param rowCount  The row count for the bulk parameter, expect null if failure is not null
     * @param failure   The optional failure for the bulk parameter, expect null if rowCount is not null
     * @return this
     */
    public BulkResponse update(int idx, @Nullable Long rowCount, @Nullable Throwable failure) {
        if (failure == null) {
            assert rowCount != null : "rowCount must not be null if failure is null";
            rowCounts[idx] = rowCount;
        } else {
            setFailure(idx, failure);
        }
        return this;
    }

    public void incrementRowCount(int idx) {
        rowCounts[idx]++;
    }

    public void setFailure(int idx, Throwable throwable) {
        rowCounts[idx] = Row1.ERROR;
        failures[idx] = throwable;
    }

    public long rowCount(int idx) {
        return rowCounts[idx];
    }

    @Nullable
    public Throwable failure(int idx) {
        return failures[idx];
    }

    public int size() {
        return size;
    }

    @VisibleForTesting
    public long[] rowCounts() {
        return rowCounts;
    }
}
