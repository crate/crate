/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.action.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.crate.analyze.AnalyzedBegin;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.Analyzer;
import io.crate.analyze.DeallocateAnalyzedStatement;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.TransactionContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.protocols.postgres.FormatCodes;
import io.crate.protocols.postgres.JobsLogsUpdateListener;
import io.crate.protocols.postgres.Portal;
import io.crate.protocols.postgres.RetryOnFailureResultReceiver;
import io.crate.protocols.postgres.SimplePortal;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

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
    private static final Logger LOGGER = Loggers.getLogger(SQLOperations.class);

    // Parser can't handle empty statement but postgres requires support for it.
    // This rewrite is done so that bind/describe calls on an empty statement will work as well
    private static final Statement EMPTY_STMT = SqlParser.createStatement("select '' from sys.cluster limit 0");

    public static final String UNNAMED = "";
    private final DependencyCarrier executor;
    private final SessionContext sessionContext;

    @VisibleForTesting
    final Map<String, PreparedStmt> preparedStatements = new HashMap<>();
    @VisibleForTesting
    final Map<String, Portal> portals = new HashMap<>();
    @VisibleForTesting
    final Set<Portal> pendingExecutions = Collections.newSetFromMap(new IdentityHashMap<Portal, Boolean>());

    private final Analyzer analyzer;
    private final Planner planner;
    private final JobsLogs jobsLogs;
    private final boolean isReadOnly;
    private final ParameterTypeExtractor parameterTypeExtractor;

    public Session(Analyzer analyzer,
                   Planner planner,
                   JobsLogs jobsLogs,
                   boolean isReadOnly,
                   DependencyCarrier executor,
                   SessionContext sessionContext) {
        this.analyzer = analyzer;
        this.planner = planner;
        this.jobsLogs = jobsLogs;
        this.isReadOnly = isReadOnly;
        this.executor = executor;
        this.sessionContext = sessionContext;
        this.parameterTypeExtractor = new Session.ParameterTypeExtractor();
    }

    /**
     * See {@link #quickExec(String, Function, ResultReceiver, Row)}
     */
    public void quickExec(String statement, ResultReceiver resultReceiver, Row params) {
        quickExec(statement, SqlParser::createStatement, resultReceiver, params);
    }

    /**
     * Execute a query in one step, avoiding the parse/bind/execute/sync procedure.
     * Opposed to using parse/bind/execute/sync this method is thread-safe.
     *
     * This only works for statements that support unbound analyze
     *
     * @param parse A function to parse the statement; This can be used to cache the parsed statement.
     *              Use {@link #quickExec(String, ResultReceiver, Row)} to use the regular parser
     */
    public void quickExec(String statement, Function<String, Statement> parse, ResultReceiver resultReceiver, Row params) {
        TransactionContext txnCtx = new TransactionContext(sessionContext);
        Statement parsedStmt = parse.apply(statement);
        AnalyzedStatement analyzedStatement = analyzer.unboundAnalyze(parsedStmt, sessionContext, ParamTypeHints.EMPTY);
        assert analyzedStatement.isUnboundPlanningSupported()
            : "quickExec can only be used with statements supporting unbound planning";
        RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), planner.getAwarenessAttributes());
        UUID jobId = UUID.randomUUID();
        ClusterState clusterState = planner.currentClusterState();
        PlannerContext plannerContext = new PlannerContext(
            clusterState,
            routingProvider,
            jobId,
            planner.functions(),
            txnCtx,
            0,
            0
        );
        Plan plan;
        try {
            plan = planner.plan(analyzedStatement, plannerContext);
        } catch (Throwable t) {
            jobsLogs.logPreExecutionFailure(jobId, statement, SQLExceptions.messageOf(t), sessionContext.user());
            throw t;
        }
        jobsLogs.logExecutionStart(jobId, statement, sessionContext.user());
        resultReceiver.completionFuture().whenComplete(new JobsLogsUpdateListener(jobId, jobsLogs));

        if (!analyzedStatement.isWriteOperation()) {
            resultReceiver = new RetryOnFailureResultReceiver(
                executor.clusterService(),
                clusterState,
                executor.threadPool().getThreadContext(),
                // not using planner.currentClusterState().metaData()::hasIndex to make sure the *current*
                // clusterState at the time of the index check is used
                indexName -> clusterState.metaData().hasIndex(indexName),
                resultReceiver,
                jobId,
                (newJobId, retryResultReceiver) -> retryQuery(
                    newJobId,
                    analyzedStatement,
                    routingProvider,
                    new RowConsumerToResultReceiver(retryResultReceiver, 0),
                    params,
                    txnCtx
                )
            );
        }
        RowConsumerToResultReceiver consumer = new RowConsumerToResultReceiver(resultReceiver, 0);
        plan.execute(executor, plannerContext, consumer, params, Collections.emptyMap());
    }

    private void retryQuery(UUID jobId,
                            AnalyzedStatement stmt,
                            RoutingProvider routingProvider,
                            RowConsumer consumer,
                            Row params,
                            TransactionContext txnCtx) {
        PlannerContext plannerContext = new PlannerContext(
            planner.currentClusterState(),
            routingProvider,
            jobId,
            planner.functions(),
            txnCtx,
            0,
            0
        );
        Plan plan = planner.plan(stmt, plannerContext);
        plan.execute(
            executor,
            plannerContext,
            consumer,
            params,
            Collections.emptyMap()
        );
    }

    private Portal getOrCreatePortal(String portalName) {
        Portal portal = portals.get(portalName);
        if (portal == null) {
            portal = new SimplePortal(portalName, analyzer, executor, isReadOnly, sessionContext);
            portals.put(portalName, portal);
        }
        return portal;
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
                jobsLogs.logPreExecutionFailure(UUID.randomUUID(), query, SQLExceptions.messageOf(t), sessionContext.user());
                throw SQLExceptions.createSQLActionException(t, sessionContext);
            }
        }
        preparedStatements.put(statementName, new PreparedStmt(statement, query, paramTypes));
    }

    public void bind(String portalName,
                     String statementName,
                     List<Object> params,
                     @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("method=bind portalName={} statementName={} params={}", portalName, statementName, params);
        }

        Portal portal = getOrCreatePortal(portalName);
        try {
            PreparedStmt preparedStmt = getSafeStmt(statementName);
            Portal newPortal = portal.bind(
                statementName, preparedStmt.query(), preparedStmt.statement(), preparedStmt.analyzedStatement(), params, resultFormatCodes);
            if (portal != newPortal) {
                portals.put(portalName, newPortal);
                pendingExecutions.remove(portal);
            } else if (portal.synced()) {
                // Make sure existing portal stops receiving results!
                portal.close();
            }
        } catch (Throwable t) {
            jobsLogs.logPreExecutionFailure(UUID.randomUUID(), portal.getLastQuery(), SQLExceptions.messageOf(t), sessionContext.user());
            throw SQLExceptions.createSQLActionException(t, sessionContext);
        }
    }

    public DescribeResult describe(char type, String portalOrStatement) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("method=describe type={} portalOrStatement={}", type, portalOrStatement);
        }
        switch (type) {
            case 'P':
                Portal portal = getSafePortal(portalOrStatement);
                return new DescribeResult(portal.describe());
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
                Statement statement = preparedStmt.statement();

                AnalyzedStatement analyzedStatement;
                if (preparedStmt.isRelationInitialized()) {
                    analyzedStatement = preparedStmt.analyzedStatement();
                } else {
                    try {
                        analyzedStatement = analyzer.unboundAnalyze(statement, sessionContext, preparedStmt.paramTypes());
                        preparedStmt.analyzedStatement(analyzedStatement);
                    } catch (Throwable t) {
                        throw SQLExceptions.createSQLActionException(t, sessionContext);
                    }
                }
                if (analyzedStatement == null) {
                    // statement without result set -> return null for NoData msg
                    return new DescribeResult(null);
                }
                DataType[] parameterSymbols = parameterTypeExtractor.getParameterTypes(analyzedStatement::visitSymbols);
                if (parameterSymbols.length > 0) {
                    preparedStmt.setDescribedParameters(parameterSymbols);
                }
                if (analyzedStatement instanceof AnalyzedRelation) {
                    AnalyzedRelation relation = (AnalyzedRelation) analyzedStatement;
                    return new DescribeResult(relation.fields(), parameterSymbols);
                }
                return new DescribeResult(null, parameterSymbols);
            default:
                throw new AssertionError("Unsupported type: " + type);
        }
    }

    public void execute(String portalName, int maxRows, ResultReceiver resultReceiver) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("method=execute portalName={} maxRows={}", portalName, maxRows);
        }

        Portal portal = getSafePortal(portalName);
        portal.execute(resultReceiver, maxRows);

        AnalyzedStatement analyzedStatement = portal.getLastAnalyzedStatement();
        if (analyzedStatement instanceof AnalyzedBegin) {
            portal.sync(planner, jobsLogs);
            clearState();
        } else if (analyzedStatement instanceof DeallocateAnalyzedStatement) {
            String stmtToDeallocate = ((DeallocateAnalyzedStatement) analyzedStatement).preparedStmtName();
            if (stmtToDeallocate != null) {
                close((byte) 'S', stmtToDeallocate);
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("deallocating all prepared statements");
                }
                preparedStatements.clear();
            }
        } else {
            // delay execution to be able to bundle bulk operations
            pendingExecutions.add(portal);
        }
    }

    public CompletableFuture<?> sync() {
        switch (pendingExecutions.size()) {
            case 0:
                LOGGER.debug("method=sync pendingExecutions=0");
                return CompletableFuture.completedFuture(null);
            case 1:
                Portal portal = pendingExecutions.iterator().next();
                LOGGER.debug("method=sync portal={}", portal);
                pendingExecutions.clear();
                clearState();
                return portal.sync(planner, jobsLogs);
            default:
                throw new IllegalStateException(
                    "Shouldn't have more than 1 pending execution. Got: " + pendingExecutions);
        }
    }

    public void clearState() {
        Portal portal = portals.remove(UNNAMED);
        if (portal != null) {
            portal.close();
        }
        preparedStatements.remove(UNNAMED);
    }

    @Nullable
    public List<? extends DataType> getOutputTypes(String portalName) {
        Portal portal = portals.get(portalName);
        if (portal == null) {
            return null;
        }
        return portal.getLastOutputTypes();
    }

    public String getQuery(String portalName) {
        return getSafePortal(portalName).getLastQuery();
    }

    public DataType getParamType(String statementName, int idx) {
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
    public FormatCodes.FormatCode[] getResultFormatCodes(String portalOrStatement) {
        Portal portal = portals.get(portalOrStatement);
        if (portal == null) {
            return null;
        }
        return portal.getLastResultFormatCodes();
    }

    /**
     * Close a portal or prepared statement
     *
     * @param type <b>S</b> for prepared statement, <b>P</b> for portal.
     * @param name name of the prepared statement or the portal (depending on type)
     */
    public void close(byte type, String name) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("method=close type={} name={}", (char) type, name);
        }

        switch (type) {
            case 'P':
                Portal portal = portals.remove(name);
                if (portal != null) {
                    portal.close();
                }
                return;
            case 'S':
                preparedStatements.remove(name);
                return;
            default:
                throw new IllegalArgumentException("Invalid type: " + type);
        }
    }

    @Override
    public void close() {
        for (Portal portal : portals.values()) {
            portal.close();
        }
        portals.clear();
        preparedStatements.clear();
        pendingExecutions.clear();
    }

    static class ParameterTypeExtractor extends DefaultTraversalSymbolVisitor<Void, Void> implements Consumer<Symbol> {

        private final SortedSet<ParameterSymbol> parameterSymbols;

        ParameterTypeExtractor() {
            this.parameterSymbols = new TreeSet<>(Comparator.comparing(ParameterSymbol::index));
        }

        @Override
        public void accept(Symbol symbol) {
            process(symbol, null);
        }

        @Override
        public Void visitParameterSymbol(ParameterSymbol parameterSymbol, Void context) {
            parameterSymbols.add(parameterSymbol);
            return null;
        }

        /**
         * Gets the parameters from the AnalyzedStatement, if possible.
         * @param consumer A consumer which takes a symbolVisitor;
         *                 This symbolVisitor should visit all {@link ParameterSymbol}s in a {@link AnalyzedStatement}
         * @return A sorted array with the parameters ($1 comes first, then $2, etc.) or null if
         *         parameters can't be obtained.
         */
        DataType[] getParameterTypes(@Nonnull Consumer<Consumer<? super Symbol>> consumer) {
            consumer.accept(this);
            Preconditions.checkState(parameterSymbols.isEmpty() ||
                                     parameterSymbols.last().index() == parameterSymbols.size() - 1,
                "The assembled list of ParameterSymbols is invalid. Missing parameters.");
            DataType[] dataTypes = parameterSymbols.stream()
                .map(ParameterSymbol::getBoundType)
                .toArray(DataType[]::new);
            parameterSymbols.clear();
            return dataTypes;
        }
    }

    /**
     * Encapsulates the result of a DescribePortal or DescribeParameter message.
     */
    public static class DescribeResult {

        @Nullable
        private final List<Field> fields;
        @Nullable
        private DataType[] parameters;

        DescribeResult(@Nullable List<Field> fields) {
            this.fields = fields;
        }

        DescribeResult(@Nullable List<Field> fields, @Nullable DataType[] parameters) {
            this.fields = fields;
            this.parameters = parameters;
        }

        @Nullable
        public List<Field> getFields() {
            return fields;
        }

        /**
         * Returns the described parameters in sorted order ($1, $2, etc.)
         * @return An array containing the parameters, or null if they could not be obtained.
         */
        @Nullable
        public DataType[] getParameters() {
            return parameters;
        }
    }
}
