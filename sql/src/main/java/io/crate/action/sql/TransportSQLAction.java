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

import io.crate.action.ActionListeners;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionMultiListener;
import io.crate.core.collections.Row;
import io.crate.executor.BytesRefUtils;
import io.crate.executor.Executor;
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
import java.util.ArrayList;
import java.util.List;


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
                     Plan plan,
                     final ActionListener<SQLResponse> listener,
                     final SQLRequest request,
                     final long startTime) {

        executor.execute(plan, new ResultReceiver() {

            private CompletionListener completionListener = CompletionListener.NO_OP;

            @Override
            public void addListener(CompletionListener listener) {
                this.completionListener = CompletionMultiListener.merge(this.completionListener, listener);
            }

            final List<Object[]> rows = new ArrayList<>();
            boolean shouldContinue = true;

            @Override
            public boolean setNextRow(Row row) {
                rows.add(row.materialize());
                return shouldContinue;
            }

            @Override
            public void finish() {
                shouldContinue = true;
                listener.onResponse(createResponse(analysis, request, rows, startTime));
                completionListener.onSuccess(null);
            }

            @Override
            public void fail(@Nonnull Throwable t) {
                shouldContinue = true;
                listener.onFailure(t);
                completionListener.onFailure(t);
            }
        });
    }

    private SQLResponse createResponse(Analysis analysis, SQLRequest request, List<Object[]> rows, long startTime) {
        String[] outputNames = EMPTY_NAMES;
        DataType[] outputTypes = EMPTY_TYPES;
        long rowCount = 0L;

        if (analysis.expectsAffectedRows()) {
            if (rows.size() >= 1){
                Object[] first = rows.iterator().next();
                if (first.length >= 1){
                    rowCount = ((Number) first[0]).longValue();
                }
                rows.clear();
            }
        } else {
            assert analysis.rootRelation() != null;
            outputNames = new String[analysis.rootRelation().fields().size()];
            outputTypes = new DataType[analysis.rootRelation().fields().size()];
            for (int i = 0; i < analysis.rootRelation().fields().size(); i++) {
                Field field = analysis.rootRelation().fields().get(i);
                outputNames[i] = field.path().outputName();
                outputTypes[i] = field.valueType();
            }

            rowCount = rows.size();
        }
        Object[][] rowsArr = rows.toArray(new Object[0][]);
        BytesRefUtils.ensureStringTypesAreStrings(outputTypes, rowsArr);
        float duration = (float)((System.nanoTime() - startTime) / 1_000_000.0);
        return new SQLResponse(
            outputNames,
            rowsArr,
            outputTypes,
            rowCount,
            duration,
            request.includeTypesOnResponse()
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
