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
import io.crate.core.collections.Row;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Collection;
import java.util.UUID;

public class ModuloDistributingDownstream extends DistributingDownstream {

    private static final ESLogger LOGGER = Loggers.getLogger(ModuloDistributingDownstream.class);

    private final MultiBucketBuilder bucketBuilder;

    public ModuloDistributingDownstream(UUID jobId,
                                        int targetExecutionNodeId,
                                        int bucketIdx,
                                        Collection<String> downstreamNodeIds,
                                        TransportDistributedResultAction transportDistributedResultAction,
                                        Streamer<?>[] streamers) {
        super(jobId, targetExecutionNodeId, bucketIdx, downstreamNodeIds, transportDistributedResultAction, streamers);

        bucketBuilder = new MultiBucketBuilder(streamers, downstreams.length);
    }

    private void consumePage() throws Exception {
        for (Row row : currentPage) {
            int downstreamIdx = bucketBuilder.getBucket(row);
            // only collect if downstream want more rows, otherwise just ignore the row
            if (downstreams[downstreamIdx].wantMore.get()) {
                bucketBuilder.setNextRow(downstreamIdx, row);
            }
        }
    }

    @Override
    protected void sendRequests() {
        try {
            consumePage();
        } catch (Throwable e) {
            Throwables.propagate(e);
        }

        for (int i = 0; i < downstreams.length; i++) {
            Downstream downstream = downstreams[i];
            if (downstream.wantMore.get()) {
                downstream.sendRequest(bucketBuilder.build(i), isLast());
            }
        }
    }

    @Override
    protected ESLogger logger() {
        return LOGGER;
    }
}
