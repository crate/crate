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
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.DataType;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.exceptions.ExceptionHelper;
import io.crate.executor.*;
import io.crate.planner.Plan;
import io.crate.planner.PlanPrinter;
import io.crate.planner.Planner;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.elasticsearch.action.ActionListener;
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
    private final Planner planner;
    private final Executor executor;
    private final DDLAnalysisDispatcher dispatcher;

    @Inject
    protected TransportSQLAction(Settings settings, ThreadPool threadPool,
            Analyzer analyzer,
            Planner planner,
            Executor executor,
            DDLAnalysisDispatcher dispatcher,
            TransportService transportService) {
        super(settings, threadPool);
        this.analyzer = analyzer;
        this.planner = planner;
        this.executor = executor;
        this.dispatcher = dispatcher;
        transportService.registerHandler(SQLAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(final SQLRequest request, final ActionListener<SQLResponse> listener) {
        logger.trace("doExecute: " + request);

        try {
            Statement statement = SqlParser.createStatement(request.stmt());
            Analysis analysis = analyzer.analyze(statement, request.args());

            if (analysis.isData()) {
                processWithPlanner(analysis, request, listener);
            } else {
                processNonData(analysis, request, listener);
            }
        } catch (Exception e) {
            listener.onFailure(ExceptionHelper.transformToCrateException(e));
        }
    }

    private void processNonData(final Analysis analysis,
                                final SQLRequest request,
                                final ActionListener<SQLResponse> listener) {
        ListenableFuture<Long> future = dispatcher.process(analysis, null);
        Futures.addCallback(future, new FutureCallback<Long>() {
            @Override
            public void onSuccess(@Nullable Long rowCount) {
                listener.onResponse(
                        new SQLResponse(
                                analysis.outputNames().toArray(new String[analysis.outputNames().size()]),
                                Constants.EMPTY_RESULT,
                                rowCount == null ? SQLResponse.NO_ROW_COUNT : rowCount,
                                request.creationTime()));
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    private void processWithPlanner(Analysis analysis, SQLRequest request, ActionListener<SQLResponse> listener) {
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
        final Job job = executor.newJob(plan);
        final ListenableFuture<List<Object[][]>> resultFuture = Futures.allAsList(executor.execute(job));

        addResultCallback(request, listener, outputNames, plan, responseBuilder, resultFuture);
    }

    private static void emptyResponse(SQLRequest request,
                                      Analysis analysis,
                                      final ActionListener<SQLResponse> listener) {
        SQLResponse response = new SQLResponse(
                analysis.outputNames().toArray(new String[analysis.outputNames().size()]),
                Constants.EMPTY_RESULT,
                0,
                request.creationTime());
        listener.onResponse(response);
    }

    private static ResponseBuilder getResponseBuilder(Plan plan) {
        if (plan.expectsAffectedRows()) {
            return new AffectedRowsResponseBuilder();
        } else {
            return new RowsResponseBuilder(true);
        }
    }

    private static void addResultCallback(final SQLRequest request,
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