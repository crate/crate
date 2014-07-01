/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.task;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.exceptions.Exceptions;
import io.crate.executor.Task;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.merge.MergeOperation;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * merging rows locally on the handler
 */
public class LocalMergeTask implements Task<Object[][]> {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final MergeNode mergeNode;
    private final StatsTables statsTables;
    private final ClusterService clusterService;
    private final Settings settings;
    private final TransportShardBulkAction transportShardBulkAction;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final ImplementationSymbolVisitor symbolVisitor;
    private final ThreadPool threadPool;
    private final SettableFuture<Object[][]> result;
    private final List<ListenableFuture<Object[][]>> resultList;

    private List<ListenableFuture<Object[][]>> upstreamResults;

    /**
     *
     * @param implementationSymbolVisitor symbol visitor (on cluster level)
     */
    public LocalMergeTask(ThreadPool threadPool,
                          ClusterService clusterService,
                          Settings settings,
                          TransportShardBulkAction transportShardBulkAction,
                          TransportCreateIndexAction transportCreateIndexAction,
                          ImplementationSymbolVisitor implementationSymbolVisitor,
                          MergeNode mergeNode,
                          StatsTables statsTables) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.settings = settings;
        this.transportShardBulkAction = transportShardBulkAction;
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.symbolVisitor = implementationSymbolVisitor;
        this.mergeNode = mergeNode;
        this.statsTables = statsTables;
        this.result = SettableFuture.create();
        this.resultList = Arrays.<ListenableFuture<Object[][]>>asList(this.result);
    }

    /**
     * only operate if we have an upstream as data always comes in via upstream results
     *
     * start listener for every upStream Result
     * if last listener is done he sets the local result.
     */
    @Override
    public void start() {
        if (upstreamResults == null) {
            result.set(Constants.EMPTY_RESULT);
            return;
        }

        final MergeOperation mergeOperation = new MergeOperation(
                clusterService, settings, transportShardBulkAction, transportCreateIndexAction,
                symbolVisitor, mergeNode);
        final AtomicInteger countdown = new AtomicInteger(upstreamResults.size());
        final UUID operationId = UUID.randomUUID();
        statsTables.operationStarted(operationId, mergeNode.contextId(), mergeNode.id());

        Futures.addCallback(mergeOperation.result(), new FutureCallback<Object[][]>() {
            @Override
            public void onSuccess(@Nullable Object[][] rows) {
                statsTables.operationFinished(operationId, null);
                result.set(rows);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                statsTables.operationFinished(operationId, Exceptions.messageOf(t));
                result.setException(t);
            }
        });

        for (final ListenableFuture<Object[][]> upstreamResult : upstreamResults) {
            Futures.addCallback(upstreamResult, new FutureCallback<Object[][]>() {
                @Override
                public void onSuccess(@Nullable Object[][] rows) {
                    assert rows != null;
                    traceLogResult(rows);
                    boolean shouldContinue;

                    try {
                        shouldContinue = mergeOperation.addRows(rows);
                    } catch (Exception ex) {
                        statsTables.operationFinished(operationId, Exceptions.messageOf(ex));
                        result.setException(ex);
                        logger.error("Failed to add rows", ex);
                        return;
                    }

                    if (countdown.decrementAndGet() == 0 || !shouldContinue) {
                        mergeOperation.finished();
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    statsTables.operationFinished(operationId, Exceptions.messageOf(t));
                    result.setException(t);
                }
            }, threadPool.executor(ThreadPool.Names.GENERIC));
        }
    }

    private void traceLogResult(Object[][] rows) {
        if (logger.isTraceEnabled()) {
            String result = Joiner.on(", ").join(Collections2.transform(Arrays.asList(rows),
                new Function<Object[], String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable Object[] input) {
                        return Arrays.toString(input);
                    }
                })
            );
            logger.trace(String.format("received result: %s", result));
        }
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return resultList;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        upstreamResults = result;
    }
}
