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

package io.crate.replication.logical;

import io.crate.replication.logical.action.ReplayChangesAction;
import io.crate.replication.logical.action.ShardChangesAction;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A TranslogSequencer allows multiple producers of [Translog.Operation]s to write them in sequence number order to an
 * index. Producer can call the [send] method to add a batch of operations to the queue. Operations can be
 * sent out of order i.e. the operation with sequence number 2 can be sent before the operation with sequence number 1.
 * In this case the Sequencer will internally buffer the operations that cannot be delivered until the missing in-order
 * operations arrive.
 *
 * Derived from org.opensearch.replication.task.shard.TranslogSequencer
 */
public class ShardTranslogSequencer {

    private static final Logger LOGGER = Loggers.getLogger(ShardTranslogSequencer.class);

    private final ShardId shardId;
    private final Client client;
    private final long initialSeqNo;
    private final ConcurrentHashMap<Long, ShardChangesAction.Response> unAppliedChanges = new ConcurrentHashMap<>();

    public ShardTranslogSequencer(ShardId shardId,
                                  Client client,
                                  long initialSeqNo) {
        this.shardId = shardId;
        this.client = client;
        this.initialSeqNo = initialSeqNo;
    }

    public void send(ShardChangesAction.Response response) throws Exception {
        unAppliedChanges.put(response.fromSeqNo(), response);
        process();
    }

    private synchronized void process() throws Exception {
        AtomicReference<Exception> lastError = new AtomicReference<>();
        var highWatermark = initialSeqNo;
        while (unAppliedChanges.containsKey(highWatermark + 1) && lastError.get() == null) {
            var next = unAppliedChanges.remove(highWatermark + 1);
            var changes = next.changes();
            if (changes.size() > 0) {
                var replayRequest = new ReplayChangesAction.Request(
                    shardId,
                    next.changes(),
                    next.maxSeqNoOfUpdatesOrDeletes()
                );
                client.execute(
                    ReplayChangesAction.INSTANCE,
                    replayRequest,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(ReplayChangesAction.Response replayResponse) {
                            var shardInfo = replayResponse.getShardInfo();
                            if (shardInfo.getFailed() > 0) {
                                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                                    LOGGER.error("Failed replaying changes. Failure:{}", failure);
                                }
                                lastError.set(new RuntimeException("Some changed failed while replaying"));
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            LOGGER.error("Failed replaying changes", e);
                            lastError.set(e);
                        }
                    }
                );
                highWatermark = changes.get(changes.size() - 1).seqNo();
            }
        }
        var error = lastError.get();
        if (error != null) {
            throw error;
        }
    }
}
