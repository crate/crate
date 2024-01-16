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

import static io.crate.data.breaker.BlockBasedRamAccounting.MAX_BLOCK_SIZE_IN_BYTES;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectMap;

import io.crate.Streamer;
import io.crate.action.FutureActionListener;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Bucket;
import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.support.ActionExecutor;


public class TransportFetchOperation implements FetchOperation {

    private static final Function<NodeFetchResponse, IntObjectMap<? extends Bucket>> GET_FETCHED = NodeFetchResponse::fetched;
    private final ActionExecutor<NodeFetchRequest, NodeFetchResponse> fetchNodeAction;
    private final Map<String, ? extends IntObjectMap<Streamer<?>[]>> nodeIdToReaderIdToStreamers;
    private final UUID jobId;
    private final int fetchPhaseId;
    private final RamAccounting ramAccounting;

    public TransportFetchOperation(ActionExecutor<NodeFetchRequest, NodeFetchResponse> fetchNodeAction,
                                   Map<String, ? extends IntObjectMap<Streamer<?>[]>> nodeIdToReaderIdToStreamers,
                                   UUID jobId,
                                   int fetchPhaseId,
                                   RamAccounting ramAccounting) {
        this.fetchNodeAction = fetchNodeAction;
        this.nodeIdToReaderIdToStreamers = nodeIdToReaderIdToStreamers;
        this.jobId = jobId;
        this.fetchPhaseId = fetchPhaseId;
        this.ramAccounting = ramAccounting;
    }

    @Override
    public CompletableFuture<IntObjectMap<? extends Bucket>> fetch(String nodeId,
                                                                   IntObjectMap<IntArrayList> toFetch,
                                                                   boolean closeContext) {
        FutureActionListener<NodeFetchResponse> listener = new FutureActionListener<>();
        return fetchNodeAction
            .execute(
                new NodeFetchRequest(nodeId,
                                     jobId,
                                     fetchPhaseId,
                                     closeContext,
                                     toFetch,
                                     nodeIdToReaderIdToStreamers.get(nodeId),
                                     ramAccountingForIncomingResponse(ramAccounting, toFetch, closeContext)))
            .whenComplete(listener)
            .thenApply(GET_FETCHED);
    }

    @VisibleForTesting
    static RamAccounting ramAccountingForIncomingResponse(RamAccounting ramAccounting,
                                                          IntObjectMap<? extends IntContainer> toFetch,
                                                          boolean closeContext) {
        if (toFetch.isEmpty() && closeContext) {
            // No data will arrive, so no ram accounting needed.
            // Indeed, with valid ram accounting, incoming accounted bytes may never be released because the release
            // logic may already happened (BatchAccumulator.close() calls do not block/wait for asynchronous responses)
            return RamAccounting.NO_ACCOUNTING;
        }
        // Each response may run in a different thread and thus should use its own ram accounting instance
        return new BlockBasedRamAccounting(
            usedBytes -> {
                // Projectors usually operate single-threaded and can receive a RamAccounting instance that is not thread-safe
                // So we must ensure thread-safety here.
                synchronized (ramAccounting) {
                    ramAccounting.addBytes(usedBytes);
                }
            },
            MAX_BLOCK_SIZE_IN_BYTES
        );
    }
}
