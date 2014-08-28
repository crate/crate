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

import com.google.common.base.Objects;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.executor.BytesRefUtils;
import io.crate.executor.Executor;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
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
            Provider<DDLAnalysisDispatcher> dispatcher,
            TransportService transportService,
            StatsTables statsTables) {
        super(clusterService, settings, SQLAction.NAME, threadPool, analyzer, planner, executor, dispatcher, statsTables);
        transportService.registerHandler(SQLAction.NAME, new TransportHandler());
    }

    @Override
    public Analysis getAnalysis(Statement statement, SQLRequest request) {
        return analyzer.analyze(statement, request.args(), SQLBulkRequest.EMPTY_BULK_ARGS);
    }

    @Override
    public SQLResponse emptyResponse(SQLRequest request,
                                     String[] outputNames,
                                     @Nullable DataType[] types) {
        return new SQLResponse(outputNames,
                TaskResult.EMPTY_RESULT.rows(),
                types,
                0L,
                request.creationTime(),
                request.includeTypesOnResponse());
    }

    @Override
    protected SQLResponse emptyResponse(SQLRequest request, Plan plan, String[] outputNames) {
        return new SQLResponse(
                outputNames,
                TaskResult.EMPTY_RESULT.rows(),
                plan.outputTypes().toArray(new DataType[plan.outputTypes().size()]),
                0L,
                request.creationTime,
                request.includeTypesOnResponse());
    }

    @Override
    protected SQLResponse createResponseFromResult(SQLRequest request,
                                                   String[] outputNames,
                                                   Long rowCount,
                                                   @Nullable DataType[] types) {
        return new SQLResponse(
                outputNames,
                TaskResult.EMPTY_RESULT.rows(),
                types,
                Objects.firstNonNull(rowCount, SQLResponse.NO_ROW_COUNT),
                request.creationTime(),
                request.includeTypesOnResponse()
        );
    }

    @Override
    protected SQLResponse createResponseFromResult(Plan plan,
                                                   String[] outputNames,
                                                   List<TaskResult> result,
                                                   long requestCreationTime,
                                                   boolean includeTypesOnResponse) {
        assert result.size() == 1;

        TaskResult taskResult = result.get(0);
        Object[][] rows = taskResult.rows();
        long rowCount = 0;
        if (plan.expectsAffectedRows() && taskResult instanceof QueryResult) {
            if (rows.length >= 1 && rows[0].length >= 1) {
                rowCount = ((Number) rows[0][0]).longValue();
            }
        } else {
            rowCount = taskResult.rowCount();
        }

        DataType[] dataTypes = plan.outputTypes().toArray(new DataType[plan.outputTypes().size()]);
        BytesRefUtils.ensureStringTypesAreStrings(dataTypes, rows);
        return new SQLResponse(
                outputNames,
                rows,
                dataTypes,
                rowCount,
                requestCreationTime,
                includeTypesOnResponse
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
            execute(request, new ActionListener<SQLResponse>() {
                @Override
                public void onResponse(SQLResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.error("Failed to send response for sql query", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
