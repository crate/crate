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
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.exceptions.*;
import io.crate.executor.Executor;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.metadata.PartitionName;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.ShardProjectorChain;
import io.crate.planner.Plan;
import io.crate.planner.PlanPrinter;
import io.crate.planner.Planner;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.InvalidSnapshotNameException;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public abstract class TransportBaseSQLAction<TRequest extends SQLBaseRequest, TResponse extends SQLBaseResponse>
        extends TransportAction<TRequest, TResponse> {

    public static final String NODE_READ_ONLY_SETTING = "node.sql.read_only";

    private static final String KILLED_MESSAGE = "KILLED";
    private static final DataType[] EMPTY_TYPES = new DataType[0];
    private static final String[] EMPTY_NAMES = new String[0];
    private static final int MAX_SHARD_MISSING_RETRIES = 3;


    private final LoadingCache<String, Statement> statementCache = CacheBuilder.newBuilder()
            .maximumSize(100)
            .build(
                    new CacheLoader<String, Statement>() {
                        @Override
                        public Statement load(@Nonnull String statement) throws Exception {
                            return SqlParser.createStatement(statement);
                        }
                    }
            );

    private final ClusterService clusterService;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final Analyzer analyzer;
    protected final Planner planner;
    private final Provider<Executor> executorProvider;
    private final StatsTables statsTables;
    private volatile boolean disabled;

    protected TransportBaseSQLAction(ClusterService clusterService,
                                     Settings settings,
                                     String actionName,
                                     ThreadPool threadPool,
                                     Analyzer analyzer,
                                     Planner planner,
                                     Provider<Executor> executorProvider,
                                     StatsTables statsTables,
                                     ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver,
                                     TransportKillJobsNodeAction transportKillJobsNodeAction,
                                     TaskManager taskManager) {
        super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver, taskManager);
        this.clusterService = clusterService;
        this.analyzer = analyzer;
        this.planner = planner;
        this.executorProvider = executorProvider;
        this.statsTables = statsTables;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
    }

    public abstract ParameterContext getParamContext(TRequest request);


    /**
     * create an empty SQLBaseResponse instance with no rows
     * and a rowCount of 0
     *
     * @param request     the request that results in the response to be created
     * @param outputNames an array of output column names
     * @param types       an array of types of the output columns,
     *                    if not null it must be of the same length as <code>outputNames</code>
     */
    protected abstract TResponse emptyResponse(TRequest request, float duration, String[] outputNames, @Nullable DataType[] types);

    /**
     * create an instance of SQLBaseResponse from a plan and a TaskResult
     *
     * @param outputNames an array of output column names
     * @param outputTypes the DataTypes of the columns/rows in the response
     * @param result      the result of the executed plan
     * @param request     the request that created which issued the execution
     */
    protected abstract TResponse createResponseFromResult(String[] outputNames,
                                                          DataType[] outputTypes,
                                                          List<TaskResult> result,
                                                          boolean expectsAffectedRows,
                                                          TRequest request,
                                                          float duration);

    /**
     * create an instance of SQLBaseResponse
     *
     * @param result   the result of the executed plan
     * @param analysis the analysis for the result
     * @param request  the request that created which issued the execution
     */
    private TResponse createResponseFromResult(@Nullable List<TaskResult> result, Analysis analysis, TRequest request, long startTime) {
        String[] outputNames;
        DataType[] outputTypes;
        if (analysis.expectsAffectedRows()) {
            outputNames = EMPTY_NAMES;
            outputTypes = EMPTY_TYPES;
        } else {
            assert analysis.rootRelation() != null;
            outputNames = new String[analysis.rootRelation().fields().size()];
            outputTypes = new DataType[analysis.rootRelation().fields().size()];
            for (int i = 0; i < analysis.rootRelation().fields().size(); i++) {
                Field field = analysis.rootRelation().fields().get(i);
                outputNames[i] = field.path().outputName();
                outputTypes[i] = field.valueType();
            }
        }
        float duration = (float)((System.nanoTime() - startTime) / 1_000_000.0);
        if (result == null) {
            return emptyResponse(request, duration, outputNames, outputTypes);
        } else {
            return createResponseFromResult(outputNames, outputTypes, result, analysis.expectsAffectedRows(), request, duration);
        }

    }

    @Override
    protected void doExecute(TRequest request, ActionListener<TResponse> listener) {
        logger.debug("{}", request);
        UUID jobId = UUID.randomUUID();
        long startTime = System.nanoTime();
        statsTables.jobStarted(jobId, request.stmt());
        doExecute(request, listener, 1, jobId, startTime);
    }

    private void doExecute(TRequest request, ActionListener<TResponse> listener, final int attempt, UUID jobId, long startTime) {
        statsTables.activeRequestsInc();
        if (disabled) {
            sendResponse(listener, new NodeDisconnectedException(clusterService.localNode(), actionName));
            return;
        }
        try {
            Statement statement = statementCache.get(request.stmt());
            Analysis analysis = analyzer.analyze(statement, getParamContext(request));
            if (analysis.analyzedStatement().isWriteOperation() && settings.getAsBoolean(NODE_READ_ONLY_SETTING, false)) {
                throw new ReadOnlyException();
            }
            processAnalysis(analysis, request, listener, attempt, jobId, startTime);
        } catch (Throwable e) {
            logger.debug("Error executing SQLRequest", e);
            sendResponse(listener, buildSQLActionException(e));
            statsTables.jobFinished(jobId, e.getMessage());
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

    private void processAnalysis(Analysis analysis, TRequest request, ActionListener<TResponse> listener,
                                 final int attempt, UUID jobId, long startTime) {
        final Plan plan = planner.plan(analysis, jobId);
        assert plan != null;
        tracePlan(plan);
        executePlan(analysis, plan, listener, request, attempt, startTime);
    }

    private void executePlan(final Analysis analysis,
                             final Plan plan,
                             final ActionListener<TResponse> listener,
                             final TRequest request,
                             final int attempt,
                             final long startTime) {
        Executor executor = executorProvider.get();
        Job job = executor.newJob(plan);

        List<? extends ListenableFuture<TaskResult>> resultFutureList = executor.execute(job);
        Futures.addCallback(Futures.allAsList(resultFutureList), new FutureCallback<List<TaskResult>>() {

                    @Override
                    public void onSuccess(@Nullable List<TaskResult> result) {
                        TResponse response;
                        try {
                            response = createResponseFromResult(result, analysis, request, startTime);
                        } catch (Throwable e) {
                            sendResponse(listener, buildSQLActionException(e));
                            return;
                        }
                        statsTables.jobFinished(plan.jobId(), null);
                        sendResponse(listener, response);
                    }

                    @Override
                    public void onFailure(final @Nonnull Throwable t) {
                        String message;
                        Throwable unwrappedException = Exceptions.unwrap(t);
                        if (unwrappedException instanceof InterruptedException) {
                            message = KILLED_MESSAGE;
                            logger.debug("KILLED: [{}]", request.stmt());
                        } else if ((unwrappedException instanceof ShardNotFoundException || unwrappedException instanceof IllegalIndexShardStateException)
                                && attempt <= MAX_SHARD_MISSING_RETRIES) {
                            logger.debug("FAILED ({}/{} attempts) - Retry: [{}]", attempt, MAX_SHARD_MISSING_RETRIES,  request.stmt());

                            killAndRetry(t);
                            return;
                        } else {
                            message = Exceptions.messageOf(t);
                            logger.debug("Error processing SQLRequest", t);
                        }
                        statsTables.jobFinished(plan.jobId(), message);
                        sendResponse(listener, buildSQLActionException(t));
                    }

                    private void killAndRetry(@Nonnull final Throwable t) {
                        /**
                         * retry should only be done for select operations or absolute write operations
                         *
                         * relative write operations (x = x + 1) must not be retried
                         *  - if one shard was successful that operation would be executed twice.
                         *
                         * This is currently ensured by the fact that
                         * {@link io.crate.operation.collect.sources.ShardCollectSource#getDocCollectors(JobCollectContext, RoutedCollectPhase, ShardProjectorChain, Map)}
                         * doesn't raise shard errors but instead uses a remoteCollector as fallback
                         *
                         * ORDERED collect operations would raise shard failures, but currently there is no case where
                         * ordered + relative write operations happen
                         */
                        transportKillJobsNodeAction.executeKillOnAllNodes(
                                new KillJobsRequest(Collections.singletonList(plan.jobId())), new ActionListener<KillResponse>() {
                                    @Override
                                    public void onResponse(KillResponse killResponse) {
                                        logger.debug("Killed {} jobs before Retry", killResponse.numKilled());
                                        doExecute(request, listener, attempt + 1, plan.jobId(), startTime);
                                    }

                                    @Override
                                    public void onFailure(Throwable e) {
                                        logger.warn("Failed to kill job before Retry", e);
                                        statsTables.jobFinished(plan.jobId(), Exceptions.messageOf(t));
                                        sendResponse(listener, buildSQLActionException(t));
                                    }
                                }
                        );
                    }
                }

        );
    }

    private void tracePlan(Plan plan) {
        if (logger.isTraceEnabled()) {
            String json = null;
            try {
                json = JsonXContent.contentBuilder()
                        .humanReadable(true)
                        .prettyPrint()
                        .value(PlanPrinter.objectMap(plan)).bytes().toUtf8();
            } catch (IOException e) {
                logger.error("Failed to print plan", e);
            }
            logger.trace(json);
        }
    }

    /**
     * Returns the cause throwable of a {@link org.elasticsearch.transport.RemoteTransportException}
     * and {@link org.elasticsearch.action.search.ReduceSearchPhaseException}.
     * Also transform throwable to {@link io.crate.exceptions.CrateException}.
     */
    private Throwable esToCrateException(Throwable e) {
        e = Exceptions.unwrap(e);

        if (e instanceof IllegalArgumentException || e instanceof ParsingException) {
            return new SQLParseException(e.getMessage(), (Exception) e);
        } else if (e instanceof UnsupportedOperationException) {
            return new UnsupportedFeatureException(e.getMessage(), (Exception) e);
        } else if (e instanceof DocumentAlreadyExistsException) {
            return new DuplicateKeyException(
                    "A document with the same primary key exists already", e);
        } else if (e instanceof IndexAlreadyExistsException) {
            return new TableAlreadyExistsException(((IndexAlreadyExistsException) e).getIndex(), e);
        } else if ((e instanceof InvalidIndexNameException)) {
            if (e.getMessage().contains("already exists as alias")) {
                // treat an alias like a table as aliases are not officially supported
                return new TableAlreadyExistsException(((InvalidIndexNameException) e).getIndex(),
                        e);
            }
            return new InvalidTableNameException(((InvalidIndexNameException) e).getIndex(), e);
        } else if (e instanceof InvalidIndexTemplateException) {
            PartitionName partitionName = PartitionName.fromIndexOrTemplate(((InvalidIndexTemplateException) e).name());
            return new InvalidTableNameException(partitionName.tableIdent().fqn(), e);
        } else if (e instanceof IndexNotFoundException) {
            return new TableUnknownException(((IndexNotFoundException) e).getIndex(), e);
        } else if (e instanceof org.elasticsearch.common.breaker.CircuitBreakingException) {
            return new CircuitBreakingException(e.getMessage());
        } else if (e instanceof InterruptedException) {
            return new JobKilledException();
        } else if (e instanceof RepositoryMissingException) {
            return new RepositoryUnknownException(((RepositoryMissingException) e).repository());
        } else if (e instanceof SnapshotMissingException) {
            return new SnapshotUnknownException(((SnapshotMissingException) e).snapshot(), e);
        } else if (e instanceof InvalidSnapshotNameException) {
            if (((InvalidSnapshotNameException) e).getDetailedMessage().contains("snapshot with such name already exists")) {
                return new SnapShotAlreadyExistsExeption(((InvalidSnapshotNameException) e).snapshot());
            }
        }
        return e;
    }

    /**
     * Create a {@link io.crate.action.sql.SQLActionException} out of a {@link java.lang.Throwable}.
     * If concrete {@link org.elasticsearch.ElasticsearchException} is found, first transform it
     * to a {@link io.crate.exceptions.CrateException}
     */
    private SQLActionException buildSQLActionException(Throwable e) {
        if (e instanceof SQLActionException) {
            return (SQLActionException) e;
        }
        e = esToCrateException(e);

        int errorCode = 5000;
        RestStatus restStatus = RestStatus.INTERNAL_SERVER_ERROR;
        if (e instanceof CrateException) {
            CrateException crateException = (CrateException) e;
            if (e instanceof ValidationException) {
                errorCode = 4000 + crateException.errorCode();
                restStatus = RestStatus.BAD_REQUEST;
            } else if (e instanceof ForbiddenException) {
                errorCode = 4030 + crateException.errorCode();
                restStatus = RestStatus.FORBIDDEN;
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

        String message = e.getMessage();
        if (message == null) {
            if (e instanceof CrateException && e.getCause() != null) {
                e = e.getCause();   // use cause because it contains a more meaningful error in most cases
            }
            StackTraceElement[] stackTraceElements = e.getStackTrace();
            if (stackTraceElements.length > 0) {
                message = String.format(Locale.ENGLISH, "%s in %s", e.getClass().getSimpleName(), stackTraceElements[0]);
            } else {
                message = "Error in " + e.getClass().getSimpleName();
            }
        } else {
            message = e.getClass().getSimpleName() + ": " + message;
        }
        return new SQLActionException(message, errorCode, restStatus, e.getStackTrace());
    }

    public void enable() {
        disabled = false;
    }

    public void disable() {
        disabled = true;
    }
}
