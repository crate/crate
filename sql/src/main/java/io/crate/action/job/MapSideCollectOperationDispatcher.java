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

package io.crate.action.job;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.distributed.DistributingDownstream;
import io.crate.executor.transport.distributed.SingleBucketBuilder;
import io.crate.operation.collect.DistributingCollectOperation;
import io.crate.operation.collect.NonDistributingCollectOperation;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;

/**
 * executes map side collect operations
 */
@Singleton
public class MapSideCollectOperationDispatcher {

    private static final ESLogger LOGGER = Loggers.getLogger(MapSideCollectOperationDispatcher.class);

    private final DistributingCollectOperation distributingCollectOperation;
    private final NonDistributingCollectOperation nonDistributingCollectOperation;
    private final StatsTables statsTables;

    @Inject
    public MapSideCollectOperationDispatcher(StatsTables statsTables,
                                             DistributingCollectOperation distributingCollectOperation,
                                             NonDistributingCollectOperation nonDistributingCollectOperation) {
        this.distributingCollectOperation = distributingCollectOperation;
        this.nonDistributingCollectOperation = nonDistributingCollectOperation;
        this.statsTables = statsTables;
    }

    public void executeCollect(UUID jobId,
                               CollectNode collectNode,
                               final RamAccountingContext ramAccountingContext,
                               final SettableFuture<Bucket> directResultFuture) {
        final UUID operationId = UUID.randomUUID();
        statsTables.operationStarted(operationId, jobId, collectNode.name());
        try {
            if (collectNode.hasDownstreams()) {
                DistributingDownstream downstream = distributingCollectOperation.createDownstream(collectNode);
                Futures.addCallback(downstream, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(@Nullable Void result) {
                        statsTables.operationFinished(operationId, null, ramAccountingContext.totalBytes());
                        ramAccountingContext.close();
                        directResultFuture.set(Bucket.EMPTY);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        statsTables.operationFinished(operationId, Exceptions.messageOf(t),
                                ramAccountingContext.totalBytes());
                        ramAccountingContext.close();
                        directResultFuture.setException(t);
                    }
                });
                distributingCollectOperation.collect(collectNode, downstream, ramAccountingContext);
            } else {
                SingleBucketBuilder downstream = nonDistributingCollectOperation.createDownstream(collectNode);
                Futures.addCallback(downstream.result(), new FutureCallback<Bucket>() {
                    @Override
                    public void onSuccess(@Nullable Bucket result) {
                        statsTables.operationFinished(
                                operationId,
                                null,
                                ramAccountingContext.totalBytes());
                        ramAccountingContext.close();
                        directResultFuture.set(result);
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        statsTables.operationFinished(
                                operationId,
                                Exceptions.messageOf(t),
                                ramAccountingContext.totalBytes());
                        ramAccountingContext.close();
                        directResultFuture.setException(t);
                    }
                });
                nonDistributingCollectOperation.collect(collectNode, downstream, ramAccountingContext);
            }
        } catch (Throwable e) {
            LOGGER.error("Error executing collect operation [{}]", e, collectNode);
            if (directResultFuture != null) {
                directResultFuture.setException(e);
            }
            statsTables.operationFinished(operationId, Exceptions.messageOf(e), ramAccountingContext.totalBytes());
            ramAccountingContext.close();
        }
    }
}
