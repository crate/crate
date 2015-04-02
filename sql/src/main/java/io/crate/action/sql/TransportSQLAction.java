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

import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.executor.BytesRefUtils;
import io.crate.executor.Executor;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.ResponseForwarder;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Planner;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.util.List;


public class TransportSQLAction extends TransportBaseSQLAction<SQLRequest, SQLResponse> {

    @Inject
    protected TransportSQLAction(
            ClusterService clusterService,
            Settings settings,
            ThreadPool threadPool,
            Analyzer analyzer,
            Planner planner,
            Provider<Executor> executor,
            TransportService transportService,
            StatsTables statsTables,
            ActionFilters actionFilters) {
        super(clusterService, settings, SQLAction.NAME, threadPool,
                analyzer, planner, executor, statsTables, actionFilters);
        transportService.registerHandler(SQLAction.NAME, new TransportHandler());
    }

    @Override
    public Analysis getAnalysis(Statement statement, SQLRequest request) {
        return analyzer.analyze(statement, new ParameterContext(
                request.args(), SQLBulkRequest.EMPTY_BULK_ARGS, request.getDefaultSchema()));
    }

    @Override
    public SQLResponse emptyResponse(SQLRequest request,
                                     String[] outputNames,
                                     @Nullable DataType[] types) {
        return new SQLResponse(outputNames,
                TaskResult.EMPTY_OBJS,
                types,
                0L,
                request.creationTime(),
                request.includeTypesOnResponse());
    }

    @Override
    protected SQLResponse createResponseFromResult(String[] outputNames,
                                                   DataType[] outputTypes,
                                                   List<TaskResult> result,
                                                   boolean expectsAffectedRows,
                                                   SQLRequest request) {
        assert result.size() == 1;
        TaskResult taskResult = result.get(0);
        Bucket rows = taskResult.rows();

        Object[][] objs;
        long rowCount = 0;
        if (expectsAffectedRows) {
            if (rows.size() >= 1){
                Row first = rows.iterator().next();
                if (first.size()>=1){
                    rowCount = ((Number) first.get(0)).longValue();
                }
            }
            objs = TaskResult.EMPTY_OBJS;
        } else {
            rowCount = rows.size();
            objs = Buckets.materialize(rows);
        }
        BytesRefUtils.ensureStringTypesAreStrings(outputTypes, objs);
        return new SQLResponse(
                outputNames,
                objs,
                outputTypes,
                rowCount,
                request.creationTime(),
                request.includeTypesOnResponse()
        );
    }

    private class TransportHandler extends BaseTransportRequestHandler<SQLRequest> {

        @Override
        public SQLRequest newInstance() {
            return new SQLRequest();
        }

        @Override
        public void messageReceived(SQLRequest request, final TransportChannel channel) throws Exception {
            // no need for a threaded listener
            request.listenerThreaded(false);
            ActionListener<SQLResponse> listener = ResponseForwarder.forwardTo(channel);
            execute(request, listener);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
