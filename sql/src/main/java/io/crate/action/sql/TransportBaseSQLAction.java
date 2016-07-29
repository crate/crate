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
import io.crate.Constants;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.exceptions.*;
import io.crate.executor.Executor;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.metadata.PartitionName;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Plan;
import io.crate.planner.PlanPrinter;
import io.crate.planner.Planner;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
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
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.UUID;

public abstract class TransportBaseSQLAction<TRequest extends SQLBaseRequest, TResponse extends SQLBaseResponse>
    extends TransportAction<TRequest, TResponse> {

    public static final String NODE_READ_ONLY_SETTING = "node.sql.read_only";

    private static final int DEFAULT_SOFT_LIMIT = 10_000;


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

    TransportBaseSQLAction(ClusterService clusterService,
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

    abstract void executePlan(Executor executor,
                              Analysis analysis,
                              Plan plan,
                              ActionListener<TResponse> listener,
                              TRequest request,
                              long startTime);


    @Override
    protected void doExecute(TRequest request, final ActionListener<TResponse> listener) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}", request);
        }
        final UUID jobId = UUID.randomUUID();
        long startTime = System.nanoTime();

        doExecute(request, listener, jobId, startTime);
    }

    private void doExecute(TRequest request, ActionListener<TResponse> listener, UUID jobId, long startTime) {
        if (disabled) {
            listener.onFailure(new NodeDisconnectedException(clusterService.localNode(), actionName));
            return;
        }
        Executor executor = executorProvider.get();
        Analysis analysis;
        Plan plan;
        try {
            Statement statement = statementCache.get(request.stmt());
            analysis = analyzer.analyze(statement, getParamContext(request));
            if (analysis.analyzedStatement().isWriteOperation()) {
                if (settings.getAsBoolean(NODE_READ_ONLY_SETTING, false)) {
                    throw new ReadOnlyException();
                }
            } else {
                // full retry is only used for read-only operations
                listener = new KillAndRetryListenerWrapper(listener, statement, jobId, executor, request, startTime);
            }
            plan = planner.plan(analysis, jobId, DEFAULT_SOFT_LIMIT, 0);
            assert plan != null;
            tracePlan(plan);
        } catch (Throwable e) {
            listener.onFailure(buildSQLActionException(e));
            statsTables.logPreExecutionFailure(jobId, request.stmt(), Exceptions.messageOf(e));
            return;
        }

        try {
            statsTables.logExecutionStart(jobId, request.stmt());
            listener = new StatsTableListenerWrapper<>(listener, statsTables, jobId);
            executePlan(executor, analysis, plan, listener, request, startTime);
        } catch (Throwable e) {
            logger.debug("Error executing SQLRequest", e);
            listener.onFailure(buildSQLActionException(e));
        }
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
    private static Throwable esToCrateException(Throwable e) {
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
    private static SQLActionException buildSQLActionException(Throwable e) {
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
            } else if (e instanceof ReadOnlyException) {
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


    private static class StatsTableListenerWrapper<TResponse extends SQLBaseResponse> implements ActionListener<TResponse> {
        private final ActionListener<TResponse> delegate;
        private final StatsTables statsTables;
        private final UUID jobId;

        StatsTableListenerWrapper(ActionListener<TResponse> delegate, StatsTables statsTables, UUID jobId) {
            this.delegate = delegate;
            this.statsTables = statsTables;
            this.jobId = jobId;
        }

        @Override
        public void onResponse(TResponse tResponse) {
            delegate.onResponse(tResponse);
            statsTables.logExecutionEnd(jobId, null);
        }

        @Override
        public void onFailure(Throwable t) {
            SQLActionException e = buildSQLActionException(t);
            delegate.onFailure(e);
            statsTables.logExecutionEnd(jobId, e.getMessage());
        }
    }

    private class KillAndRetryListenerWrapper implements ActionListener<TResponse> {

        private final ActionListener<TResponse> delegate;
        private final Statement statement;
        private final UUID jobId;
        private final Executor executor;
        private final TRequest request;
        private final long startTime;
        int attempt = 1;

        KillAndRetryListenerWrapper(ActionListener<TResponse> delegate,
                                    Statement statement,
                                    UUID jobId,
                                    Executor executor,
                                    TRequest request,
                                    long startTime) {
            this.delegate = delegate;
            this.statement = statement;
            this.jobId = jobId;
            this.executor = executor;
            this.request = request;
            this.startTime = startTime;
        }

        @Override
        public void onResponse(TResponse tResponse) {
            delegate.onResponse(tResponse);
        }

        @Override
        public void onFailure(Throwable e) {
            if (attempt <= Constants.MAX_SHARD_MISSING_RETRIES && Exceptions.isShardFailure(e)) {
                attempt += 1;
                killAndRetry();
            } else {
                delegate.onFailure(e);
            }
        }

        private void killAndRetry() {
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
                new KillJobsRequest(Collections.singletonList(jobId)), new ActionListener<KillResponse>() {
                    @Override
                    public void onResponse(KillResponse killResponse) {
                        logger.debug("Killed {} jobs before Retry", killResponse.numKilled());
                        Analysis analysis = analyzer.analyze(statement, getParamContext(request));
                        Plan newPlan = planner.plan(analysis, jobId, DEFAULT_SOFT_LIMIT, 0);
                        executePlan(executor, analysis, newPlan, KillAndRetryListenerWrapper.this, request, startTime);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.warn("Failed to kill job before Retry", e);
                        delegate.onFailure(e);
                    }
                }
            );
        }
    }
}
