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

package io.crate.action.sql;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.ActionListeners;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.executor.Executor;
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class TransportSQLBulkAction extends TransportBaseSQLAction<SQLBulkRequest, SQLBulkResponse> {

    @Inject
    public TransportSQLBulkAction(ClusterService clusterService,
                                  Settings settings,
                                  ThreadPool threadPool,
                                  Analyzer analyzer,
                                  Planner planner,
                                  Provider<Executor> executor,
                                  TransportService transportService,
                                  StatsTables statsTables,
                                  ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                  TransportKillJobsNodeAction transportKillJobsNodeAction) {
        super(clusterService, settings, SQLBulkAction.NAME, threadPool, analyzer,
            planner, executor, statsTables, actionFilters, indexNameExpressionResolver, transportKillJobsNodeAction,
            transportService.getTaskManager());

        transportService.registerRequestHandler(SQLBulkAction.NAME, SQLBulkRequest.class, ThreadPool.Names.SAME, new TransportHandler());
    }

    @Override
    public ParameterContext getParamContext(SQLBulkRequest request) {
        return new ParameterContext(
                SQLRequest.EMPTY_ARGS, request.bulkArgs(), request.getDefaultSchema(), request.getRequestFlags());
    }

    @Override
    void executePlan(Executor executor,
                     Analysis analysis,
                     Plan plan,
                     final ActionListener<SQLBulkResponse> listener,
                     final SQLBulkRequest request,
                     final long startTime) {
        if (!analysis.expectsAffectedRows()) {
            listener.onFailure(new UnsupportedOperationException(
                "Bulk operations for statements that return result sets is not supported"));
            return;
        }

        ListenableFuture<List<TaskResult>> future = Futures.allAsList(executor.executeBulk(plan));
        Futures.addCallback(future, new FutureCallback<List<TaskResult>>() {
            @Override
            public void onSuccess(@Nullable List<TaskResult> result) {
                listener.onResponse(createResponse(result, startTime));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    private SQLBulkResponse createResponse(List<TaskResult> result, long startTime) {
        SQLBulkResponse.Result[] results = new SQLBulkResponse.Result[result.size()];
        for (int i = 0, resultSize = result.size(); i < resultSize; i++) {
            assert result.get(i) instanceof RowCountResult : "Query operation not supported with bulk requests";
            RowCountResult taskResult = (RowCountResult) result.get(i);
            results[i] = new SQLBulkResponse.Result(taskResult.errorMessage(), taskResult.rowCount());
        }
        float duration = (float)((System.nanoTime() - startTime) / 1_000_000.0);
        return new SQLBulkResponse(results, duration);
    }

    private class TransportHandler extends TransportRequestHandler<SQLBulkRequest> {
        @Override
        public void messageReceived(SQLBulkRequest request, final TransportChannel channel) throws Exception {
            ActionListener<SQLBulkResponse> listener = ActionListeners.forwardTo(channel);
            execute(request, listener);
        }
    }
}
