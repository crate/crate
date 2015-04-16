/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class UpdateProjector implements Projector {

    private Projector downstream;
    private static final ESLogger LOGGER = Loggers.getLogger(UpdateProjector.class);
    public static final int DEFAULT_BULK_SIZE = 1024;

    private final BulkShardProcessor bulkShardProcessor;
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicReference<Throwable> upstreamFailure = new AtomicReference<>(null);

    private final ShardId shardId;
    private final CollectExpression<?> collectUidExpression;
    private final Symbol[] assignments;
    @Nullable
    private final Long requiredVersion;
    private final Object lock = new Object();

    public UpdateProjector(ClusterService clusterService,
                           Settings settings,
                           ShardId shardId,
                           TransportCreateIndexAction transportCreateIndexAction,
                           BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                           CollectExpression<?> collectUidExpression,
                           String[] assignmentsColumns,
                           Symbol[] assignments,
                           @Nullable Long requiredVersion) {
        this.shardId = shardId;
        this.collectUidExpression = collectUidExpression;
        this.assignments = assignments;
        this.requiredVersion = requiredVersion;
        this.bulkShardProcessor = new BulkShardProcessor(
                clusterService,
                settings,
                transportCreateIndexAction,
                false,
                false,
                DEFAULT_BULK_SIZE,
                bulkRetryCoordinatorPool,
                false,
                assignmentsColumns,
                null
            );
    }

    @Override
    public void startProjection() {
        if (remainingUpstreams.get() <= 0) {
            upstreamFinished();
        }
    }

    @Override
    public boolean setNextRow(Object... row) {
        final Uid uid;
        synchronized (lock) {
            // resolve the Uid
            collectUidExpression.setNextRow(row);
            uid = Uid.createUid(((BytesRef)collectUidExpression.value()).utf8ToString());
        }
        // routing is already resolved
        bulkShardProcessor.addForExistingShard(shardId, uid.id(), assignments, null, null, requiredVersion);
        return true;
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        remainingUpstreams.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }

        bulkShardProcessor.close();
        if (downstream != null) {
            collectUpdateResultsAndPassOverRowCount();
        }
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
        upstreamFailure.set(throwable);
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }

        bulkShardProcessor.close();
        if (downstream != null) {
            collectUpdateResultsAndPassOverRowCount();
        }
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    private void collectUpdateResultsAndPassOverRowCount() {
        Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
            @Override
            public void onSuccess(@Nullable BitSet result) {
                assert result != null;
                downstream.setNextRow(result.cardinality());
                Throwable throwable = upstreamFailure.get();
                if (throwable == null) {
                    downstream.upstreamFinished();
                } else {
                    downstream.upstreamFailed(throwable);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                downstream.setNextRow(0L);
                downstream.upstreamFailed(t);
            }
        });
    }
}
