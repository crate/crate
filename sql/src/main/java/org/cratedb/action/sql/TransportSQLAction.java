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

package org.cratedb.action.sql;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.executor.AffectedRowsResponseBuilder;
import io.crate.executor.Job;
import io.crate.executor.ResponseBuilder;
import io.crate.executor.RowsResponseBuilder;
import io.crate.executor.transport.TransportExecutor;
import io.crate.planner.Plan;
import io.crate.planner.PlanPrinter;
import io.crate.planner.Planner;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.DropTable;
import io.crate.sql.tree.Statement;
import org.cratedb.Constants;
import org.cratedb.DataType;
import org.cratedb.action.parser.ESRequestBuilder;
import org.cratedb.action.parser.SQLResponseBuilder;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.ExceptionHelper;
import org.cratedb.sql.SQLParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.util.List;


public class TransportSQLAction extends TransportAction<SQLRequest, SQLResponse> {

    private final Analyzer analyzer;
    private final TransportExecutor transportExecutor;
    private final static Planner planner = new Planner();


    private final TransportCreateIndexAction transportCreateIndexAction;
    private final TransportDeleteIndexAction transportDeleteIndexAction;
    private final SQLParseService sqlParseService;
    private final TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction;


    @Inject
    protected TransportSQLAction(Settings settings, ThreadPool threadPool,
            SQLParseService sqlParseService,
            Analyzer analyzer,
            TransportExecutor transportExecutor,
            TransportService transportService,
            TransportCreateIndexAction transportCreateIndexAction,
            TransportDeleteIndexAction transportDeleteIndexAction,
            TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction) {
        super(settings, threadPool);
        this.sqlParseService = sqlParseService;
        this.analyzer = analyzer;
        this.transportExecutor = transportExecutor;
        transportService.registerHandler(SQLAction.NAME, new TransportHandler());
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.transportDeleteIndexAction = transportDeleteIndexAction;
        this.transportClusterUpdateSettingsAction = transportClusterUpdateSettingsAction;
    }

    private abstract class ESResponseToSQLResponseListener<T extends ActionResponse> implements ActionListener<T> {

        protected final ActionListener<SQLResponse> listener;
        protected final SQLResponseBuilder builder;
        protected final long requestStartedTime;

        public ESResponseToSQLResponseListener(ParsedStatement stmt,
                                               ActionListener<SQLResponse> listener,
                                               long requestStartedTime) {
            this.listener = listener;
            this.builder = new SQLResponseBuilder(sqlParseService.context, stmt);
            this.requestStartedTime = requestStartedTime;
        }

        @Override
        public void onFailure(Throwable e) {
            listener.onFailure(ExceptionHelper.transformToCrateException(e));
        }
    }

    @Override
    protected void doExecute(final SQLRequest request, final ActionListener<SQLResponse> listener) {
        logger.trace("doExecute: " + request);

        Statement statement;
        try {
            try {
                statement = SqlParser.createStatement(request.stmt());
                if (statement instanceof DropTable || statement instanceof CreateTable) {
                    fallback(request, listener, null);
                    return;
                }
            } catch (ParsingException ex) {
                logger.info("Fallback to akiban based parser/execution layer");
                fallback(request, listener, ex);
                return;
            }


            Analysis analysis = analyzer.analyze(statement, request.args());
            final String[] outputNames = analysis.outputNames().toArray(new String[analysis.outputNames().size()]);

            if (analysis.hasNoResult()) {
                emptyResponse(request, analysis, listener);
                return;
            }
            final Plan plan = planner.plan(analysis);
            if (logger.isTraceEnabled()) {
                PlanPrinter printer = new PlanPrinter();
                logger.trace(printer.print(plan));
            }
            final ResponseBuilder responseBuilder = getResponseBuilder(plan);
            final Job job = transportExecutor.newJob(plan);
            final ListenableFuture<List<Object[][]>> resultFuture = Futures.allAsList(transportExecutor.execute(job));

            addResultCallback(request, listener, outputNames, plan, responseBuilder, resultFuture);
        } catch (Exception e) {
            listener.onFailure(ExceptionHelper.transformToCrateException(e));
        }
    }

    private void addResultCallback(final SQLRequest request,
                                   final ActionListener<SQLResponse> listener,
                                   final String[] outputNames,
                                   final Plan plan,
                                   final ResponseBuilder responseBuilder,
                                   ListenableFuture<List<Object[][]>> resultFuture) {
        Futures.addCallback(resultFuture, new FutureCallback<List<Object[][]>>() {
            @Override
            public void onSuccess(@Nullable List<Object[][]> result) {
                Object[][] rows;
                if (result == null) {
                    rows = Constants.EMPTY_RESULT;
                } else {
                    assert result.size() == 1;
                    rows = result.get(0);
                }

                SQLResponse response = responseBuilder.buildResponse(
                    plan.outputTypes().toArray(new DataType[plan.outputTypes().size()]),
                    outputNames,
                    rows,
                    request.creationTime());
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(ExceptionHelper.transformToCrateException(t));
            }
        });
    }

    private void fallback(SQLRequest request, ActionListener<SQLResponse> listener, ParsingException ex) {
        ParsedStatement stmt;
        try {
            stmt = sqlParseService.parse(request.stmt(), request.args());
        } catch (SQLParseException e) {
            listener.onFailure(e);
            return;
        } catch (UnsupportedOperationException e) {
            listener.onFailure(Objects.firstNonNull(ex, e));
            return;
        }

        try {
            ESRequestBuilder builder = new ESRequestBuilder(stmt);
            switch (stmt.type()) {
                case CREATE_INDEX_ACTION:
                    CreateIndexRequest createIndexRequest = builder.buildCreateIndexRequest();
                    transportCreateIndexAction.execute(createIndexRequest,
                        new CreateIndexResponseListener(stmt, listener, request.creationTime()));
                    break;
                case DELETE_INDEX_ACTION:
                    DeleteIndexRequest deleteIndexRequest = builder.buildDeleteIndexRequest();
                    transportDeleteIndexAction.execute(deleteIndexRequest,
                        new DeleteIndexResponseListener(stmt, listener, request.creationTime()));
                    break;
                case CREATE_ANALYZER_ACTION:
                    ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = builder.buildClusterUpdateSettingsRequest();
                    transportClusterUpdateSettingsAction.execute(clusterUpdateSettingsRequest,
                        new ClusterUpdateSettingsResponseListener(stmt, listener, request.creationTime()));
                    break;

                default:
                    listener.onFailure(ex);
            }
        } catch (Exception e) {
            if (ex == null) {
                listener.onFailure(e);
            } else {
                listener.onFailure(ex);
            }
        }
    }

    private void emptyResponse(SQLRequest request, Analysis analysis, final ActionListener<SQLResponse> listener) {
        SQLResponse response = new SQLResponse(
                analysis.outputNames().toArray(new String[analysis.outputNames().size()]),
                Constants.EMPTY_RESULT,
                0,
                request.creationTime());
        listener.onResponse(response);
    }

    private ResponseBuilder getResponseBuilder(Plan plan) {
        if (plan.expectsAffectedRows()) {
            return new AffectedRowsResponseBuilder();
        } else {
            return new RowsResponseBuilder(true);
        }
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
                        logger.warn("Failed to send response for sql query", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    private class CreateIndexResponseListener extends ESResponseToSQLResponseListener<CreateIndexResponse> {

        public CreateIndexResponseListener(ParsedStatement stmt,
                                           ActionListener<SQLResponse> listener,
                                           long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(CreateIndexResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

    private class DeleteIndexResponseListener extends ESResponseToSQLResponseListener<DeleteIndexResponse> {

        public DeleteIndexResponseListener(ParsedStatement stmt,
                                           ActionListener<SQLResponse> listener,
                                           long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(DeleteIndexResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }

    private class ClusterUpdateSettingsResponseListener extends ESResponseToSQLResponseListener<ClusterUpdateSettingsResponse> {

        public ClusterUpdateSettingsResponseListener(ParsedStatement stmt,
                                                     ActionListener<SQLResponse> listener,
                                                     long requestStartedTime) {
            super(stmt, listener, requestStartedTime);
        }

        @Override
        public void onResponse(ClusterUpdateSettingsResponse response) {
            listener.onResponse(builder.buildResponse(response, requestStartedTime));
        }
    }
}
