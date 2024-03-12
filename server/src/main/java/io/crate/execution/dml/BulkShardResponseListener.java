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

package io.crate.execution.dml;

import static io.crate.execution.engine.indexing.ShardDMLExecutor.maybeRaiseFailure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.elasticsearch.action.ActionListener;

import com.carrotsearch.hppc.IntCollection;
import com.carrotsearch.hppc.cursors.IntCursor;

import io.crate.data.Row1;
import io.crate.execution.support.MultiActionListener;

/**
 * Listener to aggregate the responses of multiple (bulk-operation-mode) ShardResponses
 */
final class BulkShardResponseListener implements ActionListener<ShardResponse> {

    private final ShardResponse.CompressedResult compressedResult;
    private final ArrayList<CompletableFuture<Long>> results;
    private final MultiActionListener<ShardResponse, ?, long[]> listener;

    /**
     * @param resultIndices a list containing one element per shardRequest-item across all shardRequests being made.
     *                      the values must contain the resultIdx;
     *                      (See {@link #toRowCounts(io.crate.execution.dml.ShardResponse.CompressedResult, IntCollection, int)})
     */
    BulkShardResponseListener(int numCallbacks,
                              int numBulkParams,
                              IntCollection resultIndices,
                              boolean insertFailFast) {
        this.results = new ArrayList<>();
        for (int i = 0; i < numBulkParams; i++) {
            results.add(new CompletableFuture<>());
        }
        this.compressedResult = new ShardResponse.CompressedResult();
        listener = new MultiActionListener<>(
            numCallbacks,
            () -> compressedResult,
            acc(insertFailFast),
            responses -> toRowCounts(responses, resultIndices, numBulkParams),
            new SetResultFutures(results)
        );
    }

    private static BiConsumer<ShardResponse.CompressedResult, ShardResponse> acc(boolean insertFailFast) {
        return (result, response) -> {
            Exception failure = response.failure();
            maybeRaiseFailure(failure); // Try to throw regardless of insert_fail_fast.
            assert failure == null : "No failure as it was not thrown before";
            if (insertFailFast == false) {
                result.update(response);
            } else {
                // This component is used for bulk DeleteById/UpdateById.
                // While single UpdateById throws an error regardless of continueOnError setup (because only single document is affected),
                // bulk UpdateById normally doesn't throw and behaves like regular Update.
                // However, when insert_fail_fast is enabled, we force error execution stop even for bulk update.
                for (int i = 0; i < response.itemIndices().size(); i++) {
                    ShardResponse.Failure itemFailure = response.failures().get(i);
                    if (itemFailure != null) {
                        throw new RuntimeException(itemFailure.message());
                    }
                }
                // No error encountered in items, fall back to normal behavior
                result.update(response);
            }
        };
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
    private static long[] toRowCounts(ShardResponse.CompressedResult result, IntCollection items, int numBulkParams) {
        long[] rowCounts = new long[numBulkParams];
        Arrays.fill(rowCounts, 0L);
        for (IntCursor c : items) {
            int itemLocation = c.index;
            int resultIdx = c.value;
            if (result.successfulWrites(itemLocation)) {
                rowCounts[resultIdx]++;
            } else if (result.failed(itemLocation)) {
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
