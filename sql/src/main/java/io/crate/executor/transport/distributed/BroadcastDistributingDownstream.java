/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.distributed;

import com.google.common.base.Throwables;
import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.transport.StreamBucket;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.UUID;

public class BroadcastDistributingDownstream extends DistributingDownstream {

    private static final ESLogger LOGGER = Loggers.getLogger(BroadcastDistributingDownstream.class);

    private final StreamBucket.Builder bucketBuilder;

    public BroadcastDistributingDownstream(UUID jobId,
                                           int targetExecutionNodeId,
                                           int bucketIdx,
                                           Collection<String> downstreamNodeIds,
                                           TransportDistributedResultAction transportDistributedResultAction,
                                           Streamer<?>[] streamers,
                                           Settings settings,
                                           int pageSize) {
        super(jobId, targetExecutionNodeId, bucketIdx, downstreamNodeIds,
                transportDistributedResultAction, streamers, settings, pageSize);
        bucketBuilder = new StreamBucket.Builder(streamers);
    }

    private Bucket consumePage() throws Exception {
        for (Row row : currentPage) {
            bucketBuilder.add(row);
        }
        Bucket bucket = bucketBuilder.build();
        bucketBuilder.reset();
        return bucket;
    }

    @Override
    protected void sendRequests() {
        Bucket bucket = null;
        try {
            bucket = consumePage();
        } catch (Throwable e) {
            Throwables.propagate(e);
        }
        for (Downstream downstream : downstreams) {
            if (downstream.wantMore.get()) {
                downstream.sendRequest(bucket, remainingUpstreams.get() <= 0);
            }
        }
    }

    @Override
    protected ESLogger logger() {
        return LOGGER;
    }
}
