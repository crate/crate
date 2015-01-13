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
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.transport.ShardUpdateRequest;
import io.crate.executor.transport.TransportShardUpdateAction;
import io.crate.executor.transport.task.ShardUpdateResponse;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class UpdateProjector implements Projector {

    private Projector downstream;
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicReference<Throwable> upstreamFailure = new AtomicReference<>(null);
    private final List<SettableFuture<Long>> updateResults = new ArrayList<>();

    private final ShardId shardId;
    private final TransportShardUpdateAction transportUpdateAction;
    private final CollectExpression<?> collectUidExpression;
    // The key of this map is expected to be a FQN columnIdent.
    private final Map<String, Symbol> assignments;
    @Nullable
    private final Long requiredVersion;
    private final Object lock = new Object();

    private final ESLogger logger = Loggers.getLogger(getClass());

    public UpdateProjector(ShardId shardId,
                           TransportShardUpdateAction transportUpdateAction,
                           CollectExpression<?> collectUidExpression,
                           Map<String, Symbol> assignments,
                           @Nullable Long requiredVersion) {
        this.shardId = shardId;
        this.transportUpdateAction = transportUpdateAction;
        this.collectUidExpression = collectUidExpression;
        this.assignments = assignments;
        this.requiredVersion = requiredVersion;
    }

    @Override
    public void startProjection() {
        if (remainingUpstreams.get() <= 0) {
            upstreamFinished();
        }
    }

    @Override
    public boolean setNextRow(Object... row) {
        Uid uid;
        synchronized (lock) {
            // resolve the Uid
            collectUidExpression.setNextRow(row);
            uid = Uid.createUid(((BytesRef)collectUidExpression.value()).utf8ToString());
        }

        final SettableFuture<Long> future = SettableFuture.create();
        updateResults.add(future);

        final ShardUpdateRequest updateRequest = new ShardUpdateRequest(shardId, uid, requiredVersion);
        updateRequest.assignments(assignments);

        transportUpdateAction.execute(updateRequest, new ActionListener<ShardUpdateResponse>() {
            @Override
            public void onResponse(ShardUpdateResponse updateResponse) {
                future.set(1L);
            }

            @Override
            public void onFailure(Throwable e) {
                if (e instanceof VersionConflictEngineException) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Updating document with id {} failed because of a version conflict", updateRequest.id());
                    }
                    future.set(0L);
                } else {
                    logger.error("Updating document with id {} failed", e, updateRequest.id());
                    future.set(0L);
                }
            }
        });

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
        Futures.addCallback(Futures.allAsList(updateResults), new FutureCallback<List<Long>>() {
            @Override
            public void onSuccess(@Nullable List<Long> result) {
                long rowCount = 0;
                for (Long val : result) {
                    rowCount =+ val;
                }
                downstream.setNextRow(rowCount);
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
