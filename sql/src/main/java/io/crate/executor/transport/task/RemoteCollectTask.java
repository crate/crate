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

package io.crate.executor.transport.task;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.QueryResult;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.NodeCollectRequest;
import io.crate.executor.transport.NodeCollectResponse;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.executor.transport.TransportCollectNodeAction;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.HandlerSideDataCollectOperation;
import io.crate.operation.merge.MergeOperation;
import io.crate.planner.node.dql.QueryAndFetchNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executes collect and merge operations remotely
 */
public class RemoteCollectTask implements Task<QueryResult> {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final SettableFuture<QueryResult> result;
    private final List<ListenableFuture<QueryResult>> results;

    private final QueryAndFetchNode queryAndFetchNode;
    private final String[] nodeIds;
    private final TransportCollectNodeAction transportCollectNodeAction;
    private final HandlerSideDataCollectOperation handlerSideDataCollectOperation;
    private final Optional<MergeOperation> mergeOperation;
    private final AtomicInteger countdown;
    private final ThreadPool threadPool;

    public RemoteCollectTask(QueryAndFetchNode queryAndFetchNode,
                             ClusterService clusterService,
                             Settings settings,
                             TransportActionProvider transportActionProvider,
                             HandlerSideDataCollectOperation handlerSideDataCollectOperation,
                             ReferenceResolver referenceResolver,
                             Functions functions,
                             ThreadPool threadPool) {

        this.queryAndFetchNode = queryAndFetchNode;
        this.transportCollectNodeAction = transportActionProvider.transportCollectNodeAction();
        this.handlerSideDataCollectOperation = handlerSideDataCollectOperation;
        this.threadPool = threadPool;

        Preconditions.checkArgument(queryAndFetchNode.isRouted(),
                "RemoteCollectTask currently only works for plans with routing");

        Preconditions.checkArgument(queryAndFetchNode.routing().hasLocations(),
                "RemoteCollectTask does not need to be executed.");

        int resultSize = queryAndFetchNode.routing().nodes().size();
        nodeIds = queryAndFetchNode.routing().nodes().toArray(new String[resultSize]);
        result = SettableFuture.create();
        results = new ArrayList<>(resultSize);
        for (int i = 0; i < resultSize; i++) {
            results.add(SettableFuture.<QueryResult>create());
        }
        countdown = new AtomicInteger(resultSize);


        if (!queryAndFetchNode.hasDownstreams()) {
            // used to merge results from the remote collect operations
            // and executing the projections from QueryAndFetchNode
            mergeOperation = Optional.of(
                    new MergeOperation(
                        clusterService,
                        settings,
                        transportActionProvider,
                        new ImplementationSymbolVisitor(referenceResolver, functions, queryAndFetchNode.maxRowGranularity()),
                        resultSize,
                        queryAndFetchNode.projections()
                    )
            );
            initMergeOperationCallbacks(mergeOperation.get());
        } else {
            mergeOperation = Optional.absent();
        }
    }

    /**
     * hook up the necessary callbacks to be used
     * when we have no downstreams so we incorporate a merge operation
     */
    private void initMergeOperationCallbacks(final MergeOperation operation) {
        Futures.addCallback(operation.result(), new FutureCallback<Object[][]>() {
            @Override
            public void onSuccess(@Nullable Object[][] rows) {
                result.set(new QueryResult(rows));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                result.setException(t);
            }
        });

        for (ListenableFuture<QueryResult> resultListenableFuture : results) {
            Futures.addCallback(resultListenableFuture, new FutureCallback<QueryResult>() {
                @Override
                public void onSuccess(@Nullable QueryResult rows) {
                    assert rows != null;
                    boolean shouldContinue;
                    try {
                        shouldContinue = operation.addRows(rows.rows());
                    } catch (Exception ex) {
                        result.setException(ex);
                        logger.error("Failed to add rows", ex);
                        return;
                    }
                    if (countdown.decrementAndGet() == 0 || !shouldContinue) {
                        operation.finished();
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    result.setException(t);
                }
            }, threadPool.executor(ThreadPool.Names.GENERIC));
        }
    }

    @Override
    public void start() {
        NodeCollectRequest request = new NodeCollectRequest(queryAndFetchNode);

        for (int i = 0; i < nodeIds.length; i++) {
            final int resultIdx = i;

            if (nodeIds[i] == null) {
                handlerSideCollect(resultIdx);
                continue;
            }

            transportCollectNodeAction.execute(
                    nodeIds[i],
                    request,
                    new ActionListener<NodeCollectResponse>() {
                        @Override
                        public void onResponse(NodeCollectResponse response) {
                            ((SettableFuture<QueryResult>)results.get(resultIdx)).set(new QueryResult(response.rows()));
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            ((SettableFuture<QueryResult>)results.get(resultIdx)).setException(e);
                        }
                    }
            );
        }
    }

    private void handlerSideCollect(final int resultIdx) {
        ListenableFuture<Object[][]> future = handlerSideDataCollectOperation.collect(queryAndFetchNode);
        Futures.addCallback(future, new FutureCallback<Object[][]>() {
            @Override
            public void onSuccess(@Nullable Object[][] rows) {
                ((SettableFuture<QueryResult>)results.get(resultIdx)).set(new QueryResult(rows));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                ((SettableFuture<QueryResult>)results.get(resultIdx)).setException(t);
            }
        });
    }

    @Override
    public List<ListenableFuture<QueryResult>> result() {
        return mergeOperation.isPresent()
                ? Arrays.<ListenableFuture<QueryResult>>asList(result)
                : results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("nope");
    }
}
