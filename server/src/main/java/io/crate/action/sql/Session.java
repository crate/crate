/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import javax.annotation.Nullable;

import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;

import io.crate.analyze.AnalyzedBegin;
import io.crate.analyze.AnalyzedCommit;
import io.crate.analyze.AnalyzedDeallocate;
import io.crate.analyze.AnalyzedDiscard;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.Relations;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.auth.AccessControl;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.data.RowN;
import io.crate.exceptions.ReadOnlyException;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.StatementClassifier;
import io.crate.planner.operators.SubQueryResults;
import io.crate.protocols.postgres.FormatCodes;
import io.crate.protocols.postgres.JobsLogsUpdateListener;
import io.crate.protocols.postgres.Portal;
import io.crate.protocols.postgres.RetryOnFailureResultReceiver;
import io.crate.protocols.postgres.TransactionState;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.DiscardStatement.Target;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;

/**
 * Stateful Session
 * In the PSQL case there is one session per connection.
 * <p>
 * <p>
 * Methods are usually called in the following order:
 * <p>
 * <pre>
 * parse(...)
 * bind(...)
 * describe(...) // optional
 * execute(...)
 * sync()
 * </pre>
 * <p>
 * Or:
 * <p>
 * <pre>
 * parse(...)
 * loop:
 *      bind(...)
 *      execute(...)
 * sync()
 * </pre>
 * <p>
 * (https://www.postgresql.org/docs/9.2/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
 */
public class Session implements AutoCloseable {

    // Logger name should be SQLOperations here
    private static final Logger LOGGER = LogManager.getLogger(SQLOperations.class);

    // Parser can't handle empty statement but postgres requires support for it.
    // This rewrite is done so that bind/describe calls on an empty statement will work as well
    private static final Statement EMPTY_STMT = SqlParser.createStatement("select '' from sys.cluster limit 0");

    public static final String UNNAMED = "";
    private final DependencyCarrier executor;
    private final AccessControl accessControl;
    private final SessionContext sessionContext;

    @VisibleForTesting
    final Map<String, PreparedStmt> preparedStatements = new HashMap<>();
    @VisibleForTesting
    final Map<String, Portal> portals = new HashMap<>();

    @VisibleForTesting
    final Map<Statement, List<DeferredExecution>> deferredExecutionsByStmt = new LinkedHashMap<>();

    @VisibleForTesting
    @Nullable
    CompletableFuture<?> activeExecution;
    private UUID activeJobID;

    private final NodeContext nodeCtx;
    private final Analyzer analyzer;
    private final Planner planner;
    private final JobsLogs jobsLogs;
    private final boolean isReadOnly;
    private final ParameterTypeExtractor parameterTypeExtractor;

    private TransactionState currentTransactionState = TransactionState.IDLE;

    public Session(NodeContext nodeCtx,
                   Analyzer analyzer,
                   Planner planner,
                   JobsLogs jobsLogs,
                   boolean isReadOnly,
                   DependencyCarrier executor,
                   AccessControl accessControl,
                   SessionContext sessionContext) {
        this.nodeCtx = nodeCtx;
        this.analyzer = analyzer;
        this.planner = planner;
        this.jobsLogs = jobsLogs;
        this.isReadOnly = isReadOnly;
        this.executor = executor;
        this.accessControl = accessControl;
        this.sessionContext = sessionContext;
        this.parameterTypeExtractor = new ParameterTypeExtractor();
    }

    /**
     * See {@link #quickExec(String, Function, ResultReceiver, Row)}
     */
    public void quickExec(String statement, ResultReceiver<?> resultReceiver, Row params) {
        quickExec(statement, SqlParser::createStatement, resultReceiver, params);
    }

    /**
     * Execute a query in one step, avoiding the parse/bind/execute/sync procedure.
     * Opposed to using parse/bind/execute/sync this method is thread-safe.
     *
     * @param parse A function to parse the statement; This can be used to cache the parsed statement.
     *              Use {@link #quickExec(String, ResultReceiver, Row)} to use the regular parser
     */
    public void quickExec(String statement, Function<String, Statement> parse, ResultReceiver<?> resultReceiver, Row params) {
        CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(sessionContext);
        Statement parsedStmt = parse.apply(statement);
        AnalyzedStatement analyzedStatement = analyzer.analyze(parsedStmt, sessionContext, ParamTypeHints.EMPTY);
        RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), planner.getAwarenessAttributes());
        UUID jobId = UUIDs.dirtyUUID();
        ClusterState clusterState = planner.currentClusterState();
        PlannerContext plannerContext = new PlannerContext(
            clusterState,
            routingProvider,
            jobId,
            txnCtx,
            nodeCtx,
            0,
            params
        );
        Plan plan;
        try {
            plan = planner.plan(analyzedStatement, plannerContext);
        } catch (Throwable t) {
            jobsLogs.logPreExecutionFailure(jobId, statement, SQLExceptions.messageOf(t), sessionContext.sessionUser());
            throw t;
        }

        StatementClassifier.Classification classification = StatementClassifier.classify(plan);
        jobsLogs.logExecutionStart(jobId, statement, sessionContext.sessionUser(), classification);
        JobsLogsUpdateListener jobsLogsUpdateListener = new JobsLogsUpdateListener(jobId, jobsLogs);
        if (!analyzedStatement.isWriteOperation()) {
            resultReceiver = new RetryOnFailureResultReceiver(
                executor.clusterService(),
                clusterState,
                // not using planner.currentClusterState().metadata()::hasIndex to make sure the *current*
                // clusterState at the time of the index check is used
                indexName -> clusterState.metadata().hasIndex(indexName),
                resultReceiver,
                jobId,
                (newJobId, retryResultReceiver) -> retryQuery(
                    newJobId,
                    analyzedStatement,
                    routingProvider,
                    new RowConsumerToResultReceiver(retryResultReceiver, 0, jobsLogsUpdateListener),
                    params,
                    txnCtx,
                    nodeCtx
                )
            );
        }
        RowConsumerToResultReceiver consumer = new RowConsumerToResultReceiver(resultReceiver, 0, jobsLogsUpdateListener);
        plan.execute(executor, plannerContext, consumer, params, SubQueryResults.EMPTY);
    }

    private void retryQuery(UUID jobId,
                            AnalyzedStatement stmt,
                            RoutingProvider routingProvider,
                            RowConsumer consumer,
                            Row params,
                            CoordinatorTxnCtx txnCtx,
                            NodeContext nodeCtx) {
        PlannerContext plannerContext = new PlannerContext(
            planner.currentClusterState(),
            routingProvider,
            jobId,
            txnCtx,
            nodeCtx,
            0,
            params
        );
        Plan plan = planner.plan(stmt, plannerContext);
        plan.execute(executor, plannerContext, consumer, params, SubQueryResults.EMPTY);
    }

    private Portal getSafePortal(String portalName) {
        Portal portal = portals.get(portalName);
        if (portal == null) {
            throw new IllegalArgumentException("Cannot find portal: " + portalName);
        }
        return portal;
    }

    public SessionContext sessionContext() {
        return sessionContext;
    }

    public void parse(String statementName, String query, List<DataType> paramTypes) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("method=parse stmtName={} query={} paramTypes={}", statementName, query, paramTypes);
        }

        Statement statement;
        try {
            statement = SqlParser.createStatement(query);
        } catch (Throwable t) {
            if ("".equals(query)) {
                statement = EMPTY_STMT;
            } else {
                jobsLogs.logPreExecutionFailure(UUIDs.dirtyUUID(), query, SQLExceptions.messageOf(t), sessionContext.sessionUser());
                throw t;
            }
        }
        analyze(statementName, statement, paramTypes, query);
    }

    public void analyze(String statementName,
                        Statement statement,
                        List<DataType> paramTypes,
                        @Nullable String query) {
        AnalyzedStatement analyzedStatement;
        DataType[] parameterTypes;
        try {
            analyzedStatement = analyzer.analyze(
                statement,
                sessionContext,
                new ParamTypeHints(paramTypes));

            parameterTypes = parameterTypeExtractor.getParameterTypes(
                x -> Relations.traverseDeepSymbols(analyzedStatement, x)
            );
        } catch (Throwable t) {
            jobsLogs.logPreExecutionFailure(
                UUIDs.dirtyUUID(),
                query == null ? statementName : query,
                SQLExceptions.messageOf(t),
                sessionContext.sessionUser());
            throw t;
        }
        preparedStatements.put(
            statementName,
            new PreparedStmt(statement, analyzedStatement, query, parameterTypes));
    }

    public void bind(String portalName,
                     String statementName,
                     List<Object> params,
                     @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("method=bind portalName={} statementName={} params={}", portalName, statementName, params);
        }
        PreparedStmt preparedStmt;
        try {
            preparedStmt = getSafeStmt(statementName);
        } catch (Throwable t) {
            jobsLogs.logPreExecutionFailure(UUIDs.dirtyUUID(), null, SQLExceptions.messageOf(t), sessionContext.sessionUser());
            throw t;
        }

        Portal portal = new Portal(
            portalName,
            preparedStmt,
            params,
            preparedStmt.analyzedStatement(),
            resultFormatCodes);
        Portal oldPortal = portals.put(portalName, portal);
        if (oldPortal != null) {
            // According to the wire protocol spec named portals should be removed explicitly and only
            // unnamed portals are implicitly closed/overridden.
            // We don't comply with the spec because we allow batching of statements, see #execute
            oldPortal.closeActiveConsumer();
        }
    }

    public DescribeResult describe(char type, String portalOrStatement) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("method=describe type={} portalOrStatement={}", type, portalOrStatement);
        }
        switch (type) {
            case 'P':
                Portal portal = getSafePortal(portalOrStatement);
                var analyzedStmt = portal.analyzedStatement();
                return new DescribeResult(
                    portal.preparedStmt().parameterTypes(),
                    analyzedStmt.outputs(),
                    resolveTableFromSelect(analyzedStmt)
                );
            case 'S':
                /*
                 * describe might be called without prior bind call.
                 *
                 * If the client uses server-side prepared statements this is usually the case.
                 *
                 * E.g. the statement is first prepared:
                 *
                 *      parse stmtName=S_1 query=insert into t (x) values ($1) paramTypes=[integer]
                 *      describe type=S portalOrStatement=S_1
                 *      sync
                 *
                 * and then used with different bind calls:
                 *
                 *      bind portalName= statementName=S_1 params=[0]
                 *      describe type=P portalOrStatement=
                 *      execute
                 *
                 *      bind portalName= statementName=S_1 params=[1]
                 *      describe type=P portalOrStatement=
                 *      execute
                 */
                PreparedStmt preparedStmt = preparedStatements.get(portalOrStatement);
                AnalyzedStatement analyzedStatement = preparedStmt.analyzedStatement();
                return new DescribeResult(
                    preparedStmt.parameterTypes(),
                    analyzedStatement.outputs(),
                    resolveTableFromSelect(analyzedStatement)
                );
            default:
                throw new AssertionError("Unsupported type: " + type);
        }
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private RelationInfo resolveTableFromSelect(AnalyzedStatement stmt) {
        // See description of {@link DescribeResult#relation()}
        // It is only populated if it is a SELECT on a single table
        if (stmt instanceof QueriedSelectRelation) {
            var relation = ((QueriedSelectRelation) stmt);
            List<AnalyzedRelation> from = relation.from();
            if (from.size() == 1 && from.get(0) instanceof AbstractTableRelation) {
                return ((AbstractTableRelation<? extends TableInfo>) from.get(0)).tableInfo();
            }
        }
        return null;
    }

    @Nullable
    public CompletableFuture<?> execute(String portalName, int maxRows, ResultReceiver<?> resultReceiver) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("method=execute portalName={} maxRows={}", portalName, maxRows);
        }
        Portal portal = getSafePortal(portalName);
        var analyzedStmt = portal.analyzedStatement();
        if (isReadOnly && analyzedStmt.isWriteOperation()) {
            throw new ReadOnlyException(portal.preparedStmt().rawStatement());
        }
        if (analyzedStmt instanceof AnalyzedBegin) {
            currentTransactionState = TransactionState.IN_TRANSACTION;
            resultReceiver.allFinished(false);
        } else if (analyzedStmt instanceof AnalyzedCommit) {
            currentTransactionState = TransactionState.IDLE;
            resultReceiver.allFinished(false);
            return resultReceiver.completionFuture();
        } else if (analyzedStmt instanceof AnalyzedDeallocate) {
            String stmtToDeallocate = ((AnalyzedDeallocate) analyzedStmt).preparedStmtName();
            if (stmtToDeallocate != null) {
                close((byte) 'S', stmtToDeallocate);
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("deallocating all prepared statements");
                }
                preparedStatements.clear();
            }
            resultReceiver.allFinished(false);
        } else if (analyzedStmt instanceof AnalyzedDiscard) {
            AnalyzedDiscard discard = (AnalyzedDiscard) analyzedStmt;
            // We don't cache plans, don't have sequences or temporary tables
            // See https://www.postgresql.org/docs/current/sql-discard.html
            if (discard.target() == Target.ALL) {
                close();
            }
            resultReceiver.allFinished(false);
        } else if (analyzedStmt.isWriteOperation()) {
            /* We defer the execution for any other statements to `sync` messages so that we can efficiently process
             * bulk operations. E.g. If we receive `INSERT INTO (x) VALUES (?)` bindings/execute multiple times
             * We want to create bulk requests internally:                                                          /
             * - To reduce network overhead
             * - To have 1 disk flush per shard instead of 1 disk flush per item
             *
             * Many clients support this by doing something like this:
             *
             *      var preparedStatement = conn.prepareStatement("...")
             *      for (var args in manyArgs):
             *          preparedStatement.execute(args)
             *      conn.commit()
             */
            deferredExecutionsByStmt.compute(
                portal.preparedStmt().parsedStatement(), (key, oldValue) -> {
                    DeferredExecution deferredExecution = new DeferredExecution(portal, maxRows, resultReceiver);
                    if (oldValue == null) {
                        ArrayList<DeferredExecution> deferredExecutions = new ArrayList<>();
                        deferredExecutions.add(deferredExecution);
                        return deferredExecutions;
                    } else {
                        oldValue.add(deferredExecution);
                        return oldValue;
                    }
                }
            );
            return resultReceiver.completionFuture();
        } else {
            if (!deferredExecutionsByStmt.isEmpty()) {
                throw new UnsupportedOperationException(
                    "Only write operations are allowed in Batch statements");
            }
            if (activeExecution == null) {
                activeExecution = singleExec(portal, resultReceiver, maxRows);
            } else {
                activeExecution = activeExecution
                    .thenCompose(ignored -> singleExec(portal, resultReceiver, maxRows));
            }
            return activeExecution;
        }
        return null;
    }

    public void flush() {
        assert !deferredExecutionsByStmt.isEmpty()
            : "Session.flush() must only be called if there are deferred executions";

        // This will make `sync` use the triggered execution instead of
        // returning a new completed future.
        //
        // We need to wait for the operation that is now being triggered to complete
        // before any further messages are sent to clients.
        // E.g. PostgresWireProtocol would otherwise send a `ReadyForQuery` message too
        // early.
        activeExecution = triggerDeferredExecutions();
    }

    public CompletableFuture<?> sync() {
        if (activeExecution == null) {
            return triggerDeferredExecutions();
        } else {
            var result = activeExecution;
            activeExecution = null;
            return result;
        }
    }

    private CompletableFuture<?> triggerDeferredExecutions() {
        switch (deferredExecutionsByStmt.size()) {
            case 0:
                LOGGER.debug("method=sync deferredExecutions=0");
                return CompletableFuture.completedFuture(null);
            case 1: {
                var entry = deferredExecutionsByStmt.entrySet().iterator().next();
                deferredExecutionsByStmt.clear();
                return exec(entry.getKey(), entry.getValue());
            }
            default: {
                // sequentiallize execution to ensure client receives row counts in correct order
                CompletableFuture<?> allCompleted = null;
                for (var entry : deferredExecutionsByStmt.entrySet()) {
                    var statement = entry.getKey();
                    var deferredExecutions = entry.getValue();
                    if (allCompleted == null) {
                        allCompleted = exec(statement, deferredExecutions);
                    } else {
                        allCompleted = allCompleted
                            // individual rowReceiver will receive failure; must not break execution chain due to failures.
                            .exceptionally(swallowException -> null)
                            .thenCompose(ignored -> exec(statement, deferredExecutions));
                    }
                }
                deferredExecutionsByStmt.clear();
                return allCompleted;
            }
        }
    }

    private CompletableFuture<?> exec(Statement statement, List<DeferredExecution> executions) {
        if (executions.size() == 1) {
            var toExec = executions.get(0);
            return singleExec(toExec.portal(), toExec.resultReceiver(), toExec.maxRows());
        } else {
            return bulkExec(statement, executions);
        }
    }

    private CompletableFuture<?> bulkExec(Statement statement, List<DeferredExecution> toExec) {
        assert toExec.size() >= 1 : "Must have at least 1 deferred execution for bulk exec";
        var jobId = UUIDs.dirtyUUID();
        var routingProvider = new RoutingProvider(Randomness.get().nextInt(), planner.getAwarenessAttributes());
        var clusterState = executor.clusterService().state();
        var txnCtx = new CoordinatorTxnCtx(sessionContext);
        var plannerContext = new PlannerContext(
            clusterState,
            routingProvider,
            jobId,
            txnCtx,
            nodeCtx,
            0,
            null);

        PreparedStmt firstPreparedStatement = toExec.get(0).portal().preparedStmt();
        AnalyzedStatement analyzedStatement = firstPreparedStatement.analyzedStatement();

        Plan plan;
        try {
            plan = planner.plan(analyzedStatement, plannerContext);
        } catch (Throwable t) {
            jobsLogs.logPreExecutionFailure(
                jobId,
                firstPreparedStatement.rawStatement(),
                SQLExceptions.messageOf(t),
                sessionContext.sessionUser());
            throw t;
        }
        jobsLogs.logExecutionStart(
            jobId,
            firstPreparedStatement.rawStatement(),
            sessionContext.sessionUser(),
            StatementClassifier.classify(plan)
        );

        var bulkArgs = Lists2.map(toExec, x -> (Row) new RowN(x.portal().params().toArray()));
        List<CompletableFuture<Long>> rowCounts = plan.executeBulk(
            executor,
            plannerContext,
            bulkArgs,
            SubQueryResults.EMPTY
        );
        CompletableFuture<Void> allRowCounts = CompletableFuture.allOf(rowCounts.toArray(new CompletableFuture[0]));
        List<CompletableFuture<?>> resultReceiverFutures = Lists2.map(toExec, x -> x.resultReceiver().completionFuture());
        CompletableFuture<Void> allResultReceivers = CompletableFuture.allOf(resultReceiverFutures.toArray(new CompletableFuture[0]));

        return allRowCounts
            .exceptionally(t -> null) // swallow exception - failures are set per item in emitResults
            .thenAccept(ignored -> emitRowCountsToResultReceivers(jobId, jobsLogs, toExec, rowCounts))
            .runAfterBoth(allResultReceivers, () -> {});
    }

    private static void emitRowCountsToResultReceivers(UUID jobId,
                                                       JobsLogs jobsLogs,
                                                       List<DeferredExecution> executions,
                                                       List<CompletableFuture<Long>> completedRowCounts) {
        Long[] cells = new Long[1];
        RowN row = new RowN(cells);
        for (int i = 0; i < completedRowCounts.size(); i++) {
            CompletableFuture<Long> completedRowCount = completedRowCounts.get(i);
            ResultReceiver<?> resultReceiver = executions.get(i).resultReceiver();
            try {
                Long rowCount = completedRowCount.join();
                cells[0] = rowCount == null ? Row1.ERROR : rowCount;
            } catch (Throwable t) {
                cells[0] = Row1.ERROR;
            }
            resultReceiver.setNextRow(row);
            resultReceiver.allFinished(false);
        }
        jobsLogs.logExecutionEnd(jobId, null);
    }

    @VisibleForTesting
    CompletableFuture<?> singleExec(Portal portal, ResultReceiver<?> resultReceiver, int maxRows) {
        var activeConsumer = portal.activeConsumer();
        if (activeConsumer != null && activeConsumer.suspended()) {
            activeConsumer.replaceResultReceiver(resultReceiver, maxRows);
            activeConsumer.resume();
            return resultReceiver.completionFuture();
        }

        activeJobID = UUIDs.dirtyUUID();
        var routingProvider = new RoutingProvider(Randomness.get().nextInt(), planner.getAwarenessAttributes());
        var clusterState = executor.clusterService().state();
        var txnCtx = new CoordinatorTxnCtx(sessionContext);
        var nodeCtx = executor.nodeContext();
        var params = new RowN(portal.params().toArray());
        var plannerContext = new PlannerContext(
            clusterState, routingProvider, activeJobID, txnCtx, nodeCtx, maxRows, params);
        var analyzedStmt = portal.analyzedStatement();
        String rawStatement = portal.preparedStmt().rawStatement();
        if (analyzedStmt == null) {
            String errorMsg = "Statement must have been analyzed: " + rawStatement;
            jobsLogs.logPreExecutionFailure(activeJobID, rawStatement, errorMsg, sessionContext.sessionUser());
            throw new IllegalStateException(errorMsg);
        }
        Plan plan;
        try {
            plan = planner.plan(analyzedStmt, plannerContext);
        } catch (Throwable t) {
            jobsLogs.logPreExecutionFailure(activeJobID, rawStatement, SQLExceptions.messageOf(t), sessionContext.sessionUser());
            throw t;
        }
        if (!analyzedStmt.isWriteOperation()) {
            resultReceiver = new RetryOnFailureResultReceiver(
                executor.clusterService(),
                clusterState,
                indexName -> executor.clusterService().state().metadata().hasIndex(indexName),
                resultReceiver,
                activeJobID,
                (newJobId, resultRec) -> retryQuery(
                    newJobId,
                    analyzedStmt,
                    routingProvider,
                    new RowConsumerToResultReceiver(
                        resultRec,
                        maxRows,
                        new JobsLogsUpdateListener(newJobId, jobsLogs)),
                    params,
                    txnCtx,
                    nodeCtx
                )
            );
        }
        jobsLogs.logExecutionStart(
            activeJobID, rawStatement, sessionContext.sessionUser(), StatementClassifier.classify(plan));
        RowConsumerToResultReceiver consumer = new RowConsumerToResultReceiver(
            resultReceiver, maxRows, new JobsLogsUpdateListener(activeJobID, jobsLogs));
        portal.setActiveConsumer(consumer);
        plan.execute(executor, plannerContext, consumer, params, SubQueryResults.EMPTY);
        return resultReceiver.completionFuture();
    }

    @Nullable
    public List<? extends DataType> getOutputTypes(String portalName) {
        Portal portal = getSafePortal(portalName);
        var analyzedStatement = portal.analyzedStatement();
        List<Symbol> fields = analyzedStatement.outputs();
        if (fields != null) {
            return Symbols.typeView(fields);
        }
        return null;
    }

    public String getQuery(String portalName) {
        return getSafePortal(portalName).preparedStmt().rawStatement();
    }

    public DataType<?> getParamType(String statementName, int idx) {
        PreparedStmt stmt = getSafeStmt(statementName);
        return stmt.getEffectiveParameterType(idx);
    }

    private PreparedStmt getSafeStmt(String statementName) {
        PreparedStmt preparedStmt = preparedStatements.get(statementName);
        if (preparedStmt == null) {
            throw new IllegalArgumentException("No statement found with name: " + statementName);
        }
        return preparedStmt;
    }

    @Nullable
    public FormatCodes.FormatCode[] getResultFormatCodes(String portal) {
        return getSafePortal(portal).resultFormatCodes();
    }

    /**
     * Close a portal or prepared statement
     *
     * <p>
     *     From PostgreSQL ExtendedQuery protocol spec:
     * </p>
     *
     * <p>
     *     The Close message closes an existing prepared statement or portal and releases resources.
     *     It is not an error to issue Close against a nonexistent statement or portal name.
     *     [..]
     *     Note that closing a prepared statement implicitly closes any open portals that were constructed from that statement.
     * </p>
     *
     * @param type <b>S</b> for prepared statement, <b>P</b> for portal.
     * @param name name of the prepared statement or the portal (depending on type)
     */
    public void close(byte type, String name) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("method=close type={} name={}", (char) type, name);
        }

        switch (type) {
            case 'P': {
                Portal portal = portals.remove(name);
                if (portal != null) {
                    portal.closeActiveConsumer();
                }
                return;
            }
            case 'S': {
                PreparedStmt preparedStmt = preparedStatements.remove(name);
                if (preparedStmt != null) {
                    Iterator<Map.Entry<String, Portal>> it = portals.entrySet().iterator();
                    while (it.hasNext()) {
                        var entry = it.next();
                        var portal = entry.getValue();
                        if (portal.preparedStmt().equals(preparedStmt)) {
                            portal.closeActiveConsumer();
                            it.remove();
                        }
                    }
                }
                return;
            }
            default:
                throw new IllegalArgumentException("Invalid type: " + type + ", valid types are: [P, S]");
        }
    }

    @Override
    public void close() {
        currentTransactionState = TransactionState.IDLE;
        resetDeferredExecutions();
        activeExecution = null;
        for (Portal portal : portals.values()) {
            portal.closeActiveConsumer();
        }
        portals.clear();
        preparedStatements.clear();
    }

    public boolean hasDeferredExecutions() {
        return !deferredExecutionsByStmt.isEmpty();
    }

    public void resetDeferredExecutions() {
        for (var deferredExecutions : deferredExecutionsByStmt.values()) {
            for (DeferredExecution deferredExecution : deferredExecutions) {
                deferredExecution.portal().closeActiveConsumer();
                portals.remove(deferredExecution.portal().name());
            }
        }
        deferredExecutionsByStmt.clear();
    }

    public TransactionState transactionState() {
        return currentTransactionState;
    }

    public java.util.UUID getActiveJobID() {
        return activeJobID;
    }
}
