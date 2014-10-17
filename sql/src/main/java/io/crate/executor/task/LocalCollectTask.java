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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.QueryResult;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.CollectOperation;
import io.crate.operation.merge.MergeOperation;
import io.crate.planner.node.dql.QueryAndFetchNode;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;


/**
 * A collect task which returns one future and runs a collectOperation locally and synchronous
 *
 * FIXME: this task should be merged with RemoteCollectTask and decide on its own how
 * to collect data
 */
public class LocalCollectTask implements Task<QueryResult> {

    private ESLogger logger = Loggers.getLogger(getClass());

    private final ThreadPool threadPool;
    private final QueryAndFetchNode queryAndFetchNode;
    private final CollectOperation collectOperation;
    private final List<ListenableFuture<QueryResult>> resultList;
    private final SettableFuture<QueryResult> result;
    private final SettableFuture<QueryResult> collectResult;

    public LocalCollectTask(QueryAndFetchNode queryAndFetchNode,
                            ClusterService clusterService,
                            Settings settings,
                            TransportActionProvider transportActionProvider,
                            CollectOperation<Object[][]> collectOperation,
                            ReferenceResolver referenceResolver,
                            Functions functions,
                            ThreadPool threadPool) {
        this.queryAndFetchNode = queryAndFetchNode;
        this.collectOperation = collectOperation;
        this.threadPool = threadPool;
        this.resultList = new ArrayList<>(1);
        this.result = SettableFuture.create();
        this.collectResult = SettableFuture.create();
        if (!queryAndFetchNode.projections().isEmpty()) {
            /// used to execute the projections of the given QueryAndFetchNode
            MergeOperation mergeOperation = new MergeOperation(
                            clusterService,
                            settings,
                            transportActionProvider,
                            new ImplementationSymbolVisitor(referenceResolver, functions, queryAndFetchNode.maxRowGranularity()),
                            1,
                            queryAndFetchNode.projections()
            );
            resultList.add(result);
            initMergeOperationCallbacks(mergeOperation);
        } else {
            resultList.add(collectResult);
        }
    }

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
        Futures.addCallback(collectResult, new FutureCallback<QueryResult>() {
            @Override
            public void onSuccess(@Nullable QueryResult rows) {
                assert rows != null;
                try {
                    operation.addRows(rows.rows());
                } catch (Exception ex) {
                    result.setException(ex);
                    logger.error("Failed to add rows", ex);
                    return;
                }
                operation.finished();
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                result.setException(t);
            }
        }, threadPool.executor(ThreadPool.Names.GENERIC));
    }

    @Override
    public void start() {
        Futures.addCallback(collectOperation.collect(queryAndFetchNode), new FutureCallback<Object[][]>() {
            @Override
            public void onSuccess(@Nullable Object[][] rows) {
                collectResult.set(new QueryResult(rows));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                collectResult.setException(t);
            }
        });
    }

    @Override
    public List<ListenableFuture<QueryResult>> result() {
        return resultList;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        // ignored, comes always first
    }
}
