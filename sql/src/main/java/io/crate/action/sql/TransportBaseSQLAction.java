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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.executor.TaskResult;
import io.crate.metadata.PartitionName;
import io.crate.analyze.Analysis;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.Analyzer;
import io.crate.exceptions.*;
import io.crate.executor.Executor;
import io.crate.executor.Job;
import io.crate.metadata.TableIdent;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Plan;
import io.crate.planner.PlanPrinter;
import io.crate.planner.Planner;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.MoreObjects.firstNonNull;

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

    private final ClusterService clusterService;
    protected final Analyzer analyzer;
    protected final Planner planner;
    private final Provider<Executor> executorProvider;
    private final StatsTables statsTables;
    private volatile boolean disabled;

    public TransportBaseSQLAction(ClusterService clusterService,
                                  Settings settings,
                                  String actionName,
                                  ThreadPool threadPool,
                                  Analyzer analyzer,
                                  Planner planner,
                                  Provider<Executor> executorProvider,
                                  StatsTables statsTables,
                                  ActionFilters actionFilters) {
        super(settings, actionName, threadPool, actionFilters);
        this.clusterService = clusterService;
        this.analyzer = analyzer;
        this.planner = planner;
        this.executorProvider = executorProvider;
        this.statsTables = statsTables;
    }

    public abstract Analysis getAnalysis(Statement statement, TRequest request);

    /**
     * create an empty SQLBaseResponse instance with no rows
     * and a rowCount of 0
     *
     * @param request the request that results in the response to be created
     * @param outputNames an array of output column names
     * @param types an array of types of the output columns,
     *              if not null it must be of the same length as <code>outputNames</code>
     */
    protected abstract TResponse emptyResponse(TRequest request, String[] outputNames, @Nullable DataType[] types);

    /**
     * create an instance of SQLBaseResponse from a plan and a TaskResult
     *
     * @param outputNames an array of output column names
     * @param outputTypes the DataTypes of the columns/rows in the response
     * @param result the result of the executed plan
     * @param requestCreationTime the time the request was instantiated on the server
     * @param includeTypesOnResponse true if the response must contain columnTypes
     */
    protected abstract TResponse createResponseFromResult(String[] outputNames,
                                                          DataType[] outputTypes,
                                                          List<TaskResult> result,
                                                          boolean expectsAffectedRows,
                                                          long requestCreationTime,
                                                          boolean includeTypesOnResponse);

    @Override
    protected void doExecute(TRequest request, ActionListener<TResponse> listener) {
        logger.debug("{}", request);
        statsTables.activeRequestsInc();
        if (disabled) {
            sendResponse(listener, new NodeDisconnectedException(clusterService.localNode(), actionName));
            return;
        }
        try {
            Statement statement = statementCache.get(request.stmt());
            Analysis analysis = getAnalysis(statement, request);
            processAnalysis(analysis, request, listener);
        } catch (Throwable e) {
            logger.debug("Error executing SQLRequest", e);
            sendResponse(listener, buildSQLActionException(e));
        }
    }

    private void sendResponse(ActionListener<TResponse> listener, Throwable throwable) {
        listener.onFailure(throwable);
        statsTables.activeRequestsDec();
    }

    private void sendResponse(ActionListener<TResponse> listener, TResponse response) {
        listener.onResponse(response);
        statsTables.activeRequestsDec();
    }

    private void processAnalysis(Analysis analysis, TRequest request, ActionListener<TResponse> listener) {
        AnalyzedStatement analyzedStatement = analysis.analyzedStatement();
        final String[] outputNames = analyzedStatement.outputNames().toArray(new String[analyzedStatement.outputNames().size()]);
        DataType[] outputTypes = analyzedStatement.outputTypes().toArray(new DataType[analyzedStatement.outputTypes().size()]);

        if (analyzedStatement.hasNoResult()) {
            listener.onResponse(emptyResponse(request, outputNames, outputTypes));
            return;
        }
        final Plan plan = planner.plan(analysis);
        tracePlan(plan);

        if (plan.isEmpty()) {
            assert analyzedStatement.expectsAffectedRows();

            UUID jobId = UUID.randomUUID();
            statsTables.jobStarted(jobId, request.stmt());
            sendResponse(listener, emptyResponse(request, outputNames, outputTypes));
            statsTables.jobFinished(jobId, null);
        } else {
            executePlan(analyzedStatement, plan, outputNames, outputTypes, listener, request);
        }
    }

    private void executePlan(final AnalyzedStatement analyzedStatement,
                             final Plan plan,
                             final String[] outputNames,
                             final DataType[] outputTypes,
                             final ActionListener<TResponse> listener,
                             final TRequest request) {
        Executor executor = executorProvider.get();
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
                        response = emptyResponse(request, outputNames, outputTypes);
                    } else {
                        response = createResponseFromResult(
                                outputNames,
                                outputTypes,
                                result,
                                analyzedStatement.expectsAffectedRows(),
                                request.creationTime(),
                                request.includeTypesOnResponse()
                        );
                    }
                } catch (Throwable e) {
                    sendResponse(listener, e);
                    return;
                }

                if (jobId != null) {
                    statsTables.jobFinished(jobId, null);
                }
                sendResponse(listener, response);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                logger.debug("Error processing SQLRequest", t);
                if (jobId != null) {
                    statsTables.jobFinished(jobId, Exceptions.messageOf(t));
                }
                sendResponse(listener, buildSQLActionException(t));
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
                return new TableAlreadyExistsException(((InvalidIndexNameException) e).index().getName(),
                        e);
            }
            return new InvalidTableNameException(((InvalidIndexNameException) e).index().getName(), e);
        } else if (e instanceof InvalidIndexTemplateException) {
            Tuple<String, String> schemaAndTable = PartitionName.schemaAndTableName(((InvalidIndexTemplateException) e).name());
            return new InvalidTableNameException(new TableIdent(schemaAndTable.v1(), schemaAndTable.v2()).fqn(), e);
        } else if (e instanceof IndexMissingException) {
            return new TableUnknownException(((IndexMissingException)e).index().name(), e);
        } else if (e instanceof org.elasticsearch.common.breaker.CircuitBreakingException) {
            return new CircuitBreakingException(e.getMessage());
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
        } else if (e instanceof MapperParsingException) {
            errorCode = 4000;
            restStatus = RestStatus.BAD_REQUEST;
        }

        if (e instanceof NullPointerException && message == null) {
            StackTraceElement[] stackTrace1 = e.getStackTrace();
            if (stackTrace1.length > 0) {
                message = String.format("NPE in %s", stackTrace1[0]);
            }
        } else if (e instanceof ArrayIndexOutOfBoundsException) {
            // in case of ArrayIndexOutOfBoundsExceptions the message is just the index number ...
            StackTraceElement[] stackTrace1 = e.getStackTrace();
            if (stackTrace1.length > 0) {
                message = String.format("ArrayIndexOutOfBoundsException in %s", stackTrace1[0]);
            }
        }
        if (logger.isTraceEnabled()) {
            message = firstNonNull(message, stackTrace.toString());
        } else if (Constants.DEBUG_MODE) {
            // will be optimized/removed at compile time
            Throwable t;
            if (e instanceof CrateException && e.getCause() != null) {
                // CrateException stackTrace will most likely just show a stackTrace which leads to some kind of transport execution
                // the cause will probably have a more helpful stackTrace;
                t = e.getCause();
            } else {
                t = e;
            }
            StringWriter stringWriter = new StringWriter();
            t.printStackTrace(new PrintWriter(stringWriter));
            stackTrace = stringWriter;
            message = firstNonNull(message, stackTrace.toString());
        }
        return new SQLActionException(message, errorCode, restStatus, stackTrace.toString());
    }

    public void enable() {
        disabled = false;
    }

    public void disable() {
        disabled = true;
    }
}
