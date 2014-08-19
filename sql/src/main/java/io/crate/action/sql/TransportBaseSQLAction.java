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

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.exceptions.*;
import io.crate.executor.Executor;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Plan;
import io.crate.planner.PlanPrinter;
import io.crate.planner.Planner;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.UUID;

public abstract class TransportBaseSQLAction<TRequest extends SQLBaseRequest, TResponse extends SQLBaseResponse>
        extends TransportAction<TRequest, TResponse> {

    private final LoadingCache<String, Statement> statementCache = CacheBuilder.newBuilder()
            .maximumSize(100)
            .build(
                    new CacheLoader<String, Statement>() {
                        @Override
                        public Statement load(@Nonnull String key) throws Exception {
                            return SqlParser.createStatement(key);
                        }
                    }
            );

    protected final Analyzer analyzer;
    protected final Planner planner;
    private final Executor executor;
    private final DDLAnalysisDispatcher dispatcher;
    private final StatsTables statsTables;

    public TransportBaseSQLAction(Settings settings,
                                  ThreadPool threadPool,
                                  Analyzer analyzer,
                                  Planner planner,
                                  Executor executor,
                                  DDLAnalysisDispatcher dispatcher,
                                  StatsTables statsTables) {
        super(settings, threadPool);
        this.analyzer = analyzer;
        this.planner = planner;
        this.executor = executor;
        this.dispatcher = dispatcher;
        this.statsTables = statsTables;
    }

    public abstract Analysis getAnalysis(Statement statement, TRequest request);

    protected abstract TResponse emptyResponse(String[] outputNames, long requestCreationTime);

    protected abstract TResponse createResponseFromResult(
            String[] outputNames, Long rowCount, long requestCreationTime);

    protected abstract TResponse createResponseFromResult(
            Plan plan, String[] outputNames, List<TaskResult> result,
            long requestCreationTime, boolean includeTypesOnResponse);


    @Override
    protected void doExecute(TRequest request, ActionListener<TResponse> listener) {
        logger.debug("{}", request);
        try {
            Statement statement = statementCache.get(request.stmt());
            Analysis analysis = getAnalysis(statement, request);

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

    private void processNonData(Analysis analysis,
                                final TRequest request,
                                final ActionListener<TResponse> listener) {
        final String[] outputNames = analysis.outputNames().toArray(new String[analysis.outputNames().size()]);
        ListenableFuture<Long> future = dispatcher.process(analysis, null);
        Futures.addCallback(future, new FutureCallback<Long>() {
            @Override
            public void onSuccess(@Nullable Long rowCount) {
                listener.onResponse(createResponseFromResult(
                        outputNames,
                        rowCount,
                        request.creationTime()
                ));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                logger.debug("Error processing non data SQLRequest", t);
                listener.onFailure(buildSQLActionException(t));
            }
        });
    }


    private void processWithPlanner(Analysis analysis, TRequest request, ActionListener<TResponse> listener) {
        final String[] outputNames = analysis.outputNames().toArray(new String[analysis.outputNames().size()]);

        if (analysis.hasNoResult()) {
            listener.onResponse(emptyResponse(outputNames, request.creationTime()));
            return;
        }
        final Plan plan = planner.plan(analysis);
        tracePlan(plan);

        if (plan.isEmpty()) {
            assert plan.expectsAffectedRows();

            UUID jobId = UUID.randomUUID();
            statsTables.jobStarted(jobId, request.stmt());
            listener.onResponse(emptyResponse(outputNames, request.creationTime()));
            statsTables.jobFinished(jobId, null);
        } else {
            executePlan(plan, outputNames, listener, request);
        }
    }

    private void executePlan(final Plan plan, final String[] outputNames, final ActionListener<TResponse> listener, final TRequest request) {
        Job job = executor.newJob(plan);
        final UUID jobId = job.id();
        if (jobId != null) {
            statsTables.jobStarted(jobId, request.stmt());
        }
        List<ListenableFuture<TaskResult>> resultFutureList = executor.execute(job);
        Futures.addCallback(Futures.allAsList(resultFutureList), new FutureCallback<List<TaskResult>>() {
            @Override
            public void onSuccess(@Nullable List<TaskResult> result) {
                TResponse response;

                try {
                    if (result == null) {
                        response = emptyResponse(outputNames, request.creationTime());
                    } else {
                        response = createResponseFromResult(
                                plan,
                                outputNames,
                                result,
                                request.creationTime(),
                                request.includeTypesOnResponse()
                        );
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }

                if (jobId != null) {
                    statsTables.jobFinished(jobId, null);
                }
                listener.onResponse(response);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                logger.debug("Error processing SQLRequest", t);
                if (jobId != null) {
                    statsTables.jobFinished(jobId, Exceptions.messageOf(t));
                }
                listener.onFailure(buildSQLActionException(t));
            }
        });
    }

    private void tracePlan(Plan plan) {
        if (logger.isTraceEnabled()) {
            PlanPrinter printer = new PlanPrinter();
            logger.trace(printer.print(plan));
        }
    }


    /**
     * Returns the cause throwable of a {@link org.elasticsearch.transport.RemoteTransportException}
     * and {@link org.elasticsearch.action.search.ReduceSearchPhaseException}.
     * Also transform throwable to {@link io.crate.exceptions.CrateException}.
     *
     */
    public Throwable esToCrateException(Throwable e) {
        e = Exceptions.unwrap(e);

        if (e instanceof IllegalArgumentException || e instanceof ParsingException) {
            return new SQLParseException(e.getMessage(), (Exception)e);
        } else if (e instanceof UnsupportedOperationException) {
            return new UnsupportedFeatureException(e.getMessage(), (Exception)e);
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

        if (e instanceof NullPointerException && message == null) {
            StackTraceElement[] stackTrace1 = e.getStackTrace();
            if (stackTrace1.length > 0) {
                message = String.format("NPE in %s", stackTrace1[0]);
            }
        }
        if (logger.isTraceEnabled()) {
            message = Objects.firstNonNull(message, stackTrace.toString());
        }
        return new SQLActionException(message, errorCode, restStatus, stackTrace.toString());
    }
}
