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

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import com.carrotsearch.hppc.IntCollection;

import io.crate.action.FutureActionListener;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.support.MultiActionListener;

/**
 * Listener to aggregate the responses of multiple (bulk-operation-mode) ShardResponses
 */
final class BulkShardResponseListener implements ActionListener<ShardResponse> {

    private final ShardResponse.CompressedResult compressedResult;
    private final FutureActionListener<BulkResponse> results;
    private final MultiActionListener<ShardResponse, ShardResponse.CompressedResult, BulkResponse> listener;

    /**
     * @param resultIndices a list containing one element per shardRequest-item across all shardRequests being made.
     *                      the values must contain the resultIdx;
     *                      (See {@link BulkResponse#update(int, long, Throwable)} )
     */
    BulkShardResponseListener(int numCallbacks,
                              int numBulkParams,
                              IntCollection resultIndices,
                              boolean allowFailOnPartialWrites) {
        var bulkResponse = new BulkResponse(numBulkParams);
        this.results = new FutureActionListener<>();
        this.compressedResult = new ShardResponse.CompressedResult();
        listener = new MultiActionListener<>(
            numCallbacks,
            () -> compressedResult,
            onResponse(allowFailOnPartialWrites),
            responses -> bulkResponse.update(responses, resultIndices),
            results
        );
    }

    private static BiConsumer<ShardResponse.CompressedResult, ShardResponse> onResponse(boolean allowFailOnPartialWrites) {
        return (result, response) -> {
            Exception failure = response.failure();
            if (failure == null) {
                if (allowFailOnPartialWrites == false) {
                    result.update(response);
                } else {
                    for (int i = 0; i < response.itemIndices().size(); i++) {
                        ShardResponse.Failure itemFailure = response.failures().get(i);
                        if (itemFailure != null) {
                            throw new RuntimeException(itemFailure.error());
                        }
                    }
                    // No error encountered in items, fall back to normal behavior
                    result.update(response);
                }
            } else {
                // If shardResponse has a failure we must throw.
                // This is not necessarily controlled by the allowFailOnPartialWrites setting,
                // KILL can use it as well, so this behavior is kept intact.
                Throwable t = SQLExceptions.unwrap(failure);
                if (!(t instanceof DocumentMissingException) && !(t instanceof VersionConflictEngineException)) {
                    throw new RuntimeException(t);
                }
            }

        };
    }

    public CompletableFuture<BulkResponse> bulkResponseFuture() {
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
}
