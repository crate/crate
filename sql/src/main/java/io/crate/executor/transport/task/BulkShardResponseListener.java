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

package io.crate.executor.transport.task;

import com.carrotsearch.hppc.IntCollection;
import com.carrotsearch.hppc.cursors.IntCursor;
import io.crate.data.Row1;
import io.crate.exceptions.SQLExceptions;
import io.crate.executor.MultiActionListener;
import io.crate.executor.transport.ShardResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Listener to aggregate the responses of multiple (bulk-operation-mode) ShardResponses
 */
final class BulkShardResponseListener implements ActionListener<ShardResponse> {

    private final BitSet allResponses;
    private final ArrayList<CompletableFuture<Long>> results;
    private final MultiActionListener<ShardResponse, ?, long[]> listener;

    /**
     * @param resultIndices a list containing one element per shardRequest-item across all shardRequests being made.
     *                      the values must contain the resultIdx;
     *                      (See {@link #toRowCounts(BitSet, IntCollection, int)})
     */
    BulkShardResponseListener(int numCallbacks,
                              int numBulkParams,
                              IntCollection resultIndices) {
        this.results = new ArrayList<>();
        for (int i = 0; i < numBulkParams; i++) {
            results.add(new CompletableFuture<>());
        }
        this.allResponses = new BitSet();
        listener = new MultiActionListener<>(
            numCallbacks,
            () -> allResponses,
            BulkShardResponseListener::onResponse,
            responses -> toRowCounts(responses, resultIndices, numBulkParams),
            new SetResultFutures(results)
        );
    }

    private static void onResponse(BitSet responses, ShardResponse response) {
        Exception failure = response.failure();
        if (failure == null) {
            ShardResponse.markResponseItemsAndFailures(response, responses);
        } else {
            Throwable t = SQLExceptions.unwrap(failure, e -> e instanceof RuntimeException);
            if (!(t instanceof DocumentMissingException) && !(t instanceof VersionConflictEngineException)) {
                throw new RuntimeException(t);
            }
        }
    }

    public List<CompletableFuture<Long>> rowCountFutures() {
        return results;
    }

    @Override
    public void onResponse(ShardResponse shardResponse) {
        listener.onResponse(shardResponse);
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    /**
     * This calculates & returns the correct rowCount specific to the bulk individual bulk-params.
     *
     * Example:
     *
     * <pre>
     * where id = ? or id = ?
     * bulkParams: [ [1, 2], [3, 4] ]
     *
     * ->
     *  3 requests (per shard) (they can span across *all* parameters)
     *
     *   shard0: [0, 2]      // item locations
     *   shard1: [1]
     *   shard2: [3]
     *
     *  numBulkParams: 2 -> 2 items in the result
     *
     *  items:
     *     idx  value
     *      0 -> 0
     *      1 -> 0
     *      2 -> 1
     *      3 -> 1
     *
     *   result:
     *      long[] {2, 2}
     * </pre>
     */
    private static long[] toRowCounts(BitSet responses, IntCollection items, int numBulkParams) {
        long[] rowCounts = new long[numBulkParams];
        Arrays.fill(rowCounts, 0L);
        for (IntCursor c : items) {
            int itemLocation = c.index;
            int resultIdx = c.value;
            if (responses.get(itemLocation)) {
                rowCounts[resultIdx]++;
            } else {
                // We should change this in the future to just not count the item;
                // so that we don't return errors if parts were successful
                rowCounts[resultIdx] = Row1.ERROR;
            }
        }
        return rowCounts;
    }

    private static class SetResultFutures implements ActionListener<long[]> {

        private final ArrayList<CompletableFuture<Long>> results;

        SetResultFutures(ArrayList<CompletableFuture<Long>> results) {
            this.results = results;
        }

        @Override
        public void onResponse(long[] rowCounts) {
            for (int i = 0; i < rowCounts.length; i++) {
                results.get(i).complete(rowCounts[i]);
            }
        }

        @Override
        public void onFailure(Exception e) {
            for (CompletableFuture<Long> result : results) {
                result.completeExceptionally(e);
            }
        }
    }
}
