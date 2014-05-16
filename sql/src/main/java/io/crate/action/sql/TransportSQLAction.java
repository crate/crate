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
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.crate.Constants;
import io.crate.types.DataType;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.exceptions.*;
import io.crate.executor.*;
import io.crate.planner.Plan;
import io.crate.planner.PlanPrinter;
import io.crate.planner.Planner;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
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
        logger.debug("{}", request);

        try {
            Statement statement = SqlParser.createStatement(request.stmt());
            Analysis analysis = analyzer.analyze(statement, request.args());

            if (analysis.isData()) {
                processWithPlanner(analysis, request, listener);
            } else {
                processNonData(analysis, request, listener);
            }
        } catch (Exception e) {
            logger.debug("Error executing SQLRequest", e);
            listener.onFailure(buildSQLActionException(e));
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
                                request.creationTime())
                );
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("Error processing non data SQLRequest", t);
                listener.onFailure(buildSQLActionException(t));
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
                        request.creationTime(),
                        request.includeTypesOnResponse());
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("Error processing SQLRequest", t);
                listener.onFailure(buildSQLActionException(t));
            }
        });
    }

    /**
     * Returns the cause throwable of a {@link org.elasticsearch.transport.RemoteTransportException}
     * and {@link org.elasticsearch.action.search.ReduceSearchPhaseException}.
     * Also transform throwable to {@link io.crate.exceptions.CrateException}.
     *
     * @param e
     * @return
     */
    public Throwable esToCrateException(Throwable e) {
        if (e instanceof RemoteTransportException || e instanceof UncheckedExecutionException || e instanceof UncategorizedExecutionException) {
            // if its a transport exception get the real cause throwable
            e = e.getCause();
        }
        if (e instanceof ExecutionException) {
            e = e.getCause();
        }

        if (e instanceof IllegalArgumentException || e instanceof ParsingException) {
            return new SQLParseException(e.getMessage(), (Exception)e);
        } else if (e instanceof UnsupportedOperationException) {
            return new UnsupportedFeatureException(e.getMessage());
        } else if (e instanceof DocumentAlreadyExistsException) {
            return new DuplicateKeyException(
                    "A document with the same primary key exists already", e);
        } else if (e instanceof IndexAlreadyExistsException) {
            return new TableAlreadyExistsException(((IndexAlreadyExistsException)e).index().name(), e);
        } else if ((e instanceof InvalidIndexNameException)) {
            if (e.getMessage().contains("already exists as alias")) {
                // treat an alias like a table as aliases are not officially supported
                return new TableAlreadyExistsException(((InvalidIndexNameException)e).index().getName(),
                        e);
            }
            return new InvalidTableNameException(((InvalidIndexNameException) e).index().getName(), e);
        } else if (e instanceof IndexMissingException) {
            return new TableUnknownException(((IndexMissingException)e).index().name(), e);
        } else if (e instanceof ReduceSearchPhaseException && e.getCause() instanceof VersionConflictException) {
            /**
             * For update or search requests we use upstream ES SearchRequests
             * These requests are executed using the transportSearchAction.
             *
             * The transportSearchAction (or the more specific QueryThenFetch/../ Action inside it
             * executes the TransportSQLAction.SearchResponseListener onResponse/onFailure
             * but adds its own error handling around it.
             * By doing so it wraps every exception raised inside our onResponse in its own ReduceSearchPhaseException
             * Here we unwrap it to get the original exception.
             */
            return e.getCause();
        }
        return e;
    }

    /**
     * Create a {@link io.crate.action.sql.SQLActionException} out of a {@link java.lang.Throwable}.
     * If concrete {@link org.elasticsearch.ElasticsearchException} is found, first transform it
     * to a {@link io.crate.exceptions.CrateException}
     *
     * @param e
     * @return
     */
    public SQLActionException buildSQLActionException(Throwable e) {
        if (e instanceof SQLActionException) {
            return (SQLActionException) e;
        }
        e = esToCrateException(e);

        int errorCode = 5000;
        RestStatus restStatus = RestStatus.INTERNAL_SERVER_ERROR;
        String message = e.getMessage();
        StringWriter stackTrace = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTrace));

        if (e instanceof CrateException) {
            CrateException crateException = (CrateException)e;
            if (e instanceof ValidationException) {
                errorCode = 4000 + crateException.errorCode();
                restStatus = RestStatus.BAD_REQUEST;
            } else if (e instanceof ResourceUnknownException) {
                errorCode = 4040 + crateException.errorCode();
                restStatus = RestStatus.NOT_FOUND;
            } else if (e instanceof ConflictException) {
                errorCode = 4090 + crateException.errorCode();
                restStatus = RestStatus.CONFLICT;
            } else if (e instanceof UnhandledServerException) {
                errorCode = 5000 + crateException.errorCode();
            }
        } else if (e instanceof ParsingException) {
            errorCode = 4000;
            restStatus = RestStatus.BAD_REQUEST;
        }

        return new SQLActionException(message, errorCode, restStatus, stackTrace.toString());
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
