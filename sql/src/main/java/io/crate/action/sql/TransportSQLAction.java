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

package io.crate.action.sql;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.action.ActionListeners;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.executor.BytesRefUtils;
import io.crate.executor.Executor;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;


@Singleton
public class TransportSQLAction extends TransportBaseSQLAction<SQLRequest, SQLResponse> {

    private static final DataType[] EMPTY_TYPES = new DataType[0];
    private static final String[] EMPTY_NAMES = new String[0];

    @Inject
    public TransportSQLAction(
            ClusterService clusterService,
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
        super(clusterService, settings, SQLAction.NAME, threadPool,
            analyzer, planner, executor, statsTables, actionFilters,
            indexNameExpressionResolver, transportKillJobsNodeAction,
            transportService.getTaskManager());

        transportService.registerRequestHandler(SQLAction.NAME, SQLRequest.class, ThreadPool.Names.SAME, new TransportHandler());
    }

    @Override
    public ParameterContext getParamContext(SQLRequest request) {
        return new ParameterContext(
                request.args(), SQLBulkRequest.EMPTY_BULK_ARGS, request.getDefaultSchema(), request.getRequestFlags());
    }

    @Override
    void executePlan(Executor executor,
                     final Analysis analysis,
                     final Plan plan,
                     final ActionListener<SQLResponse> listener,
                     final SQLRequest request,
                     final long startTime) {

        Futures.addCallback(executor.execute(plan), new FutureCallback<TaskResult>() {
            @Override
            public void onSuccess(@Nullable TaskResult result) {
                listener.onResponse(createResponse(plan.jobId(), analysis, request, result, startTime));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    private SQLResponse createResponse(UUID jobId,
                                       Analysis analysis,
                                       SQLRequest request,
                                       @Nullable TaskResult result,
                                       long startTime) {
        Bucket bucket = result == null ? Bucket.EMPTY : result.rows();
        Object[][] rows;
        String[] outputNames = EMPTY_NAMES;
        DataType[] outputTypes = EMPTY_TYPES;
        long rowCount = 0L;

        if (analysis.expectsAffectedRows()) {
            if (bucket.size() >= 1){
                Row first = bucket.iterator().next();
                if (first.size()>=1){
                    rowCount = ((Number) first.get(0)).longValue();
                }
            }
            rows = TaskResult.EMPTY_OBJS;
        } else {
            assert analysis.rootRelation() != null;
            outputNames = new String[analysis.rootRelation().fields().size()];
            outputTypes = new DataType[analysis.rootRelation().fields().size()];
            for (int i = 0; i < analysis.rootRelation().fields().size(); i++) {
                Field field = analysis.rootRelation().fields().get(i);
                outputNames[i] = field.path().outputName();
                outputTypes[i] = field.valueType();
            }

            rowCount = bucket.size();
            rows = Buckets.materialize(bucket);
        }
        BytesRefUtils.ensureStringTypesAreStrings(outputTypes, rows);
        float duration = (float)((System.nanoTime() - startTime) / 1_000_000.0);
        return new SQLResponse(
            outputNames,
            rows,
            outputTypes,
            rowCount,
            duration,
            request.includeTypesOnResponse(),
            jobId
        );
    }

    private class TransportHandler extends TransportRequestHandler<SQLRequest> {
        @Override
        public void messageReceived(SQLRequest request, final TransportChannel channel) throws Exception {
            ActionListener<SQLResponse> listener = ActionListeners.forwardTo(channel);
            execute(request, listener);
        }
    }
}
