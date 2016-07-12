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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionState;
import io.crate.core.collections.Row;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.ReadOnlyException;
import io.crate.executor.Executor;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.ResumeHandle;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.statement.SetSessionPlan;
import io.crate.protocols.postgres.FormatCodes;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static io.crate.action.sql.SQLBulkRequest.EMPTY_BULK_ARGS;
import static io.crate.action.sql.SQLRequest.EMPTY_ARGS;
import static io.crate.action.sql.TransportBaseSQLAction.NODE_READ_ONLY_SETTING;


@Singleton
public class SQLOperations {

    private final static ESLogger LOGGER = Loggers.getLogger(SQLOperations.class);

    private final Analyzer analyzer;
    private final Planner planner;
    private final Provider<Executor> executorProvider;
    private final Provider<TransportKillJobsNodeAction> transportKillJobsNodeActionProvider;
    private final StatsTables statsTables;
    private final boolean isReadOnly;

    @Inject
    public SQLOperations(Analyzer analyzer,
                         Planner planner,
                         Provider<Executor> executorProvider,
                         Provider<TransportKillJobsNodeAction> transportKillJobsNodeActionProvider,
                         StatsTables statsTables,
                         Settings settings) {
        this.analyzer = analyzer;
        this.planner = planner;
        this.executorProvider = executorProvider;
        this.transportKillJobsNodeActionProvider = transportKillJobsNodeActionProvider;
        this.statsTables = statsTables;
        this.isReadOnly = settings.getAsBoolean(NODE_READ_ONLY_SETTING, false);
    }

    public Session createSession(@Nullable String defaultSchema) {
        return new Session(executorProvider.get(), transportKillJobsNodeActionProvider.get(), defaultSchema);
    }

    /**
     * Stateful Session
     * In the PSQL case there is one session per connection.
     *
     *
     * Methods are usually called in the following order:
     *
     * <pre>
     * parse(...)
     * bind(...)
     * describe(...) // optional
     * execute(...)
     * sync()
     * </pre>
     *
     * Or:
     *
     * <pre>
     * parse(...)
     * loop:
     *      bind(...)
     *      execute(...)
     * sync()
     * </pre>
     *
     * If during one of the operation an error occurs the error will be raised and all subsequent methods will become
     * no-op operations until sync() is called which will again raise an error and "clear" it. (to be able to process new statements)
     *
     * This is done for compatibility with the postgres extended spec:
     *
     * > The purpose of Sync is to provide a resynchronization point for error recovery.
     * > When an error is detected while processing any extended-query message, the backend issues ErrorResponse,
     * > then reads and discards messages until a Sync is reached,
     * > then issues ReadyForQuery and returns to normal message processing.
     * > (But note that no skipping occurs if an error is detected while processing Sync —
     * > this ensures that there is one and only one ReadyForQuery sent for each Sync.)
     *
     * (https://www.postgresql.org/docs/9.2/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
     */
    public class Session {

        private final Executor executor;
        private final TransportKillJobsNodeAction transportKillJobsNodeAction;
        private final String defaultSchema;

        private UUID jobId;
        private List<DataType> paramTypes;
        private Statement statement;
        private String query;

        private Throwable throwable = null;
        private Map<String, String> settings = new HashMap<>();

        private Map<String, Portal> portals = new HashMap<>();
        private String lastPortalName;


        private Session(Executor executor, TransportKillJobsNodeAction transportKillJobsNodeAction, String defaultSchema) {
            this.executor = executor;
            this.transportKillJobsNodeAction = transportKillJobsNodeAction;
            this.defaultSchema = defaultSchema;
        }

        public void simpleQuery(String query,
                                Function<List<Field>, ResultReceiver> resultReceiverFactory,
                                CompletionListener doneListener) {
            // TODO: support multiple statements
            if (query.endsWith(";")) {
                query = query.substring(0, query.length() - 1);
            }

            Statement statement = SqlParser.createStatement(query);
            statsTables.jobStarted(jobId, query);
            Analysis analysis = analyzer.analyze(statement, new ParameterContext(EMPTY_ARGS, EMPTY_BULK_ARGS, defaultSchema));
            validateReadOnly(analysis);
            Plan plan = planner.plan(analysis, UUID.randomUUID(), 0, 0);

            ResultReceiver resultReceiver;
            if (analysis.rootRelation() == null) {
                resultReceiver = resultReceiverFactory.apply(null);
            } else {
                resultReceiver = resultReceiverFactory.apply(analysis.rootRelation().fields());
            }
            assert resultReceiver != null : "resultReceiver must not be null";
            resultReceiver.addListener(doneListener);
            resultReceiver.addListener(new StatsTablesUpdateListener(jobId, statsTables));
            applySessionSettings(plan);
            executor.execute(plan, new RowReceiverToResultReceiver(resultReceiver, 0));
        }

        private void checkError() {
            if (throwable != null) {
                throw Throwables.propagate(throwable);
            }
        }

        private Portal getOrCreatePortal(String portalName, String statementName) {
            Portal portal = portals.get(portalName);
            if (portal == null) {
                portal = new Portal();
                portals.put(portalName, portal);
            }
            return portal;
        }

        private Portal getSafePortal(String portalOrStatement) {
            Portal portal = portals.get(portalOrStatement);
            if (portal == null) {
                throw new IllegalArgumentException("Cannot find portal: " + portalOrStatement);
            }
            return portal;
        }

        public void parse(String statementName, String query, List<DataType> paramTypes) {
            LOGGER.debug("method=parse stmtName={} query={} paramTypes={}", statementName, query, paramTypes);
            checkError();

            try {
                Statement statement = SqlParser.createStatement(query);
                if (this.statement == null) {
                    this.jobId = UUID.randomUUID();
                    statsTables.jobStarted(jobId, query);
                } else if (!statement.equals(this.statement)) {
                    // different query -> no bulk operation -> execute previous query
                    sync(CompletionListener.NO_OP);
                    this.jobId = UUID.randomUUID();
                    statsTables.jobStarted(jobId, query);
                }
                this.statement = statement;
                this.query = query;
                this.paramTypes = paramTypes;
            } catch (Throwable t) {
                throwable = t;
                throw t;
            }
        }

        public void bind(String portalName, String statementName, List<Object> params, @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
            LOGGER.debug("method=bind portalName={} statementName={} params={}", portalName, statementName, params);
            checkError();

            try {
                Portal portal = getOrCreatePortal(portalName, statementName);
                portal.resultFormatCodes = resultFormatCodes;
                portal.bulkParams.add(params);
                if (portal.analysis == null) {
                    // analyze only once in a batch operation where there is
                    // Parse -> Bind -> Describe -> Execute -> Parse -> Bind -> Describe -> Execute -> ... -> Sync
                    portal.analysis = analyzer.analyze(statement, new ParameterContext(portal.getArgs(), EMPTY_BULK_ARGS, defaultSchema));
                }
            } catch (Throwable t) {
                this.throwable = t;
                throw t;
            }
        }

        public List<Field> describe(char type, String portalOrStatement) {
            LOGGER.debug("method=describe type={} portalOrStatement={}", type, portalOrStatement);
            checkError();

            try {
                Portal portal = getSafePortal(portalOrStatement);
                Analysis analysis = portal.analysis;
                if (analysis.rootRelation() == null) {
                    return null;
                }
                List<Field> fields = analysis.rootRelation().fields();
                portal.outputTypes = Symbols.extractTypes(fields);
                return fields;
            } catch (Throwable t) {
                throwable = t;
                throw t;
            }
        }

        public void execute(String portalName, int maxRows, ResultReceiver rowReceiver) {
            LOGGER.debug("method=execute portalName={} maxRows={}", portalName, maxRows);

            checkError();
            try {
                Portal portal = getSafePortal(portalName);
                validateReadOnly(portal.analysis);
                portal.addRowReceiver(rowReceiver);
                portal.maxRows = maxRows;
                lastPortalName = portalName;
            } catch (Throwable t) {
                throwable = t;
                throw t;
            }
        }

        public void sync(CompletionListener listener) {
            LOGGER.debug("method=sync");

            if (throwable == null) {
                /**
                 * Execution is delayed to sync() to be able to make efficient bulk requests.
                 *
                 * JDBC batch execution can cause the following flow:
                 *
                 * Parse -> Bind -> Execute
                 * Parse -> Bind -> Execute
                 * Sync
                 *
                 * If the second parse is called with a *different* statement, the parse call will trigger sync.
                 * Cleanup needs to happen before parse returns because clients don't wait for parseOk but follow up
                 * with bind/execute immediately.
                 *
                 * This is why the global state is assigned to local variables and the cleanup is called immediately.
                 * Otherwise there would be concurrency/async state modification.
                 */
                Portal portal = getSafePortal(lastPortalName);
                if (portal.resultReceivers.size() == 1) {
                    Plan plan = planner.plan(portal.analysis, jobId, 0, portal.maxRows);
                    ResultReceiver resultReceiver = portal.resultReceivers.get(0);
                    if (portal.maxRows > 0) {
                        portal.resultReceivers.clear();
                    }
                    cleanup(lastPortalName);
                    applySessionSettings(plan);
                    resultReceiver.addListener(listener);
                    resultReceiver.addListener(new StatsTablesUpdateListener(jobId, statsTables));

                    if (!portal.analysis.analyzedStatement().isWriteOperation()) {
                        resultReceiver = new ResultReceiverRetryWrapper(resultReceiver, this, portal, analyzer,
                            planner, transportKillJobsNodeAction);
                    }

                    if (resumeIfSuspended(portal, resultReceiver)) return;
                    RowReceiverToResultReceiver rowReceiver = new RowReceiverToResultReceiver(resultReceiver, portal.maxRows);
                    portal.rowReceiver = rowReceiver;
                    executor.execute(plan, rowReceiver);
                } else {
                    Object[][] bulkArgs = toBulkArgs(portal.bulkParams);
                    Analysis analysis = analyzer.analyze(statement, new ParameterContext(new Object[0], bulkArgs, defaultSchema));
                    ImmutableList<ResultReceiver> resultReceivers = ImmutableList.copyOf(portal.resultReceivers);
                    Plan plan = planner.plan(analysis, jobId, 0, portal.maxRows);
                    cleanup(lastPortalName);
                    executeBulk(plan, resultReceivers, listener);
                }
            } else {
                Throwable t = this.throwable;
                cleanup(lastPortalName);
                throw Throwables.propagate(t);
            }
        }

        private boolean resumeIfSuspended(Portal portal, ResultReceiver resultReceiver) {
            LOGGER.trace("method=resumeIfSuspended");
            if (portal.rowReceiver == null) {
                return false;
            }
            RowReceiverToResultReceiver rowReceiver = portal.rowReceiver;
            ResumeHandle resumeHandle = rowReceiver.resumeHandle();
            if (resumeHandle == null) {
                return false;
            }
            rowReceiver.replaceResultReceiver(resultReceiver, portal.maxRows);
            LOGGER.trace("Resuming {}", resumeHandle);
            resumeHandle.resume(true);
            return true;
        }

        private void executeBulk(Plan plan, final List<ResultReceiver> resultReceivers, final CompletionListener listener) {
            List<? extends ListenableFuture<TaskResult>> futures = executor.executeBulk(plan);
            Futures.addCallback(Futures.allAsList(futures), new FutureCallback<List<TaskResult>>() {
                @Override
                public void onSuccess(@Nullable List<TaskResult> result) {
                    assert result != null && result.size() == resultReceivers.size()
                        : "number of result must match number of rowReceivers";

                    for (int i = 0; i < result.size(); i++) {
                        ResultReceiver resultReceiver = resultReceivers.get(i);
                        resultReceiver.setNextRow(result.get(i).rows().iterator().next());
                        resultReceiver.allFinished();
                    }
                    listener.onSuccess(null);
                    statsTables.jobFinished(jobId, null);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    for (ResultReceiver resultReceiver : resultReceivers) {
                        resultReceiver.fail(t);
                    }
                    listener.onFailure(t);
                    statsTables.jobFinished(jobId, Exceptions.messageOf(t));
                }
            });
        }

        private void applySessionSettings(Plan plan) {
            if (plan instanceof SetSessionPlan) {
                settings.putAll(((SetSessionPlan) plan).settings().getAsMap());
            }
        }

        private void validateReadOnly(Analysis analysis) {
            if (analysis != null && analysis.analyzedStatement().isWriteOperation() && isReadOnly) {
                throw new ReadOnlyException();
            }
        }

        private void cleanup(String portalName) {
            if ("".equals(portalName)) { // only close unnamed portal - others require close calls
                Portal portal = portals.remove("");
                if (portal != null) {
                    portal.close();
                }
            }
            throwable = null;
            statement = null;
        }

        @Nullable
        public List<? extends DataType> getOutputTypes(String portalName) {
            Portal portal = portals.get(portalName);
            if (portal == null) {
                return null;
            }
            return portal.outputTypes;
        }

        public String query() {
            return query;
        }

        public DataType getParamType(int idx) {
            return paramTypes.get(idx);
        }

        @Nullable
        public FormatCodes.FormatCode[] getResultFormatCodes(String portalOrStatement) {
            Portal portal = portals.get(portalOrStatement);
            if (portal == null) {
                return null;
            }
            return portal.resultFormatCodes;
        }

        public void closePortal(byte portalOrStatement, String portalOrStatementName) {
            if (portalOrStatement == 'P') {
                Portal portal = portals.remove(portalOrStatementName);
                if (portal != null) {
                    portal.close();
                }
            }
        }

        public void close() {
            for (Portal portal : portals.values()) {
                portal.close();
            }
        }
    }

    private static class Portal {

        private Analysis analysis;
        private final List<List<Object>> bulkParams = new ArrayList<>();
        private final List<ResultReceiver> resultReceivers = new ArrayList<>();

        @Nullable
        private FormatCodes.FormatCode[] resultFormatCodes;
        private int maxRows = 0;
        private RowReceiverToResultReceiver rowReceiver = null;
        private List<? extends DataType> outputTypes;

        private Object[] getArgs() {
            if (bulkParams.size() == 1) {
                return bulkParams.get(0).toArray(new Object[0]);
            } else {
                return EMPTY_ARGS;
            }
        }

        void addRowReceiver(ResultReceiver resultReceiver) {
            resultReceivers.add(resultReceiver);
        }

        void close() {
            if (rowReceiver != null) {
                ResumeHandle resumeHandle = rowReceiver.resumeHandle();
                if (resumeHandle != null) {
                    rowReceiver.kill(new InterruptedException("Client closed portal"));
                    resumeHandle.resume(false);
                }
            }
        }
    }

    private static Object[][] toBulkArgs(List<List<Object>> bulkParams) {
        Object[][] bulkArgs = new Object[bulkParams.size()][];
        for (int i = 0; i < bulkArgs.length; i++) {
            bulkArgs[i] = bulkParams.get(i).toArray(new Object[0]);
        }
        return bulkArgs;
    }

    private static class StatsTablesUpdateListener implements CompletionListener {

        private final UUID jobId;
        private final StatsTables statsTables;

        StatsTablesUpdateListener(UUID jobId, StatsTables statsTables) {
            this.jobId = jobId;
            this.statsTables = statsTables;
        }
        @Override
        public void onSuccess(@Nullable CompletionState result) {
            statsTables.jobFinished(jobId, null);
        }

        @Override
        public void onFailure(Throwable t) {
            statsTables.jobFinished(jobId, Exceptions.messageOf(t));
        }
    }

    private static class ResultReceiverRetryWrapper implements ResultReceiver {

        private final ResultReceiver delegate;
        private final Session session;
        private final Portal portal;
        private final Analyzer analyzer;
        private final Planner planner;
        private final TransportKillJobsNodeAction transportKillJobsNodeAction;
        int attempt = 1;

        ResultReceiverRetryWrapper(ResultReceiver delegate,
                                   Session session,
                                   Portal portal,
                                   Analyzer analyzer,
                                   Planner planner,
                                   TransportKillJobsNodeAction transportKillJobsNodeAction) {
            this.delegate = delegate;
            this.session = session;
            this.portal = portal;
            this.analyzer = analyzer;
            this.planner = planner;
            this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        }

        @Override
        public void setNextRow(Row row) {
            delegate.setNextRow(row);
        }

        @Override
        public void batchFinished() {
            delegate.batchFinished();
        }

        @Override
        public void allFinished() {
            delegate.allFinished();
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            if (attempt <= Constants.MAX_SHARD_MISSING_RETRIES && Exceptions.isShardFailure(t)) {
                attempt += 1;
                killAndRetry();
            } else {
                delegate.fail(t);
            }
        }

        private void killAndRetry() {
            transportKillJobsNodeAction.executeKillOnAllNodes(
                new KillJobsRequest(Collections.singletonList(session.jobId)), new ActionListener<KillResponse>() {
                    @Override
                    public void onResponse(KillResponse killResponse) {
                        LOGGER.debug("Killed {} jobs before Retry", killResponse.numKilled());

                        portal.analysis = analyzer.analyze(
                            session.statement,
                            new ParameterContext(portal.getArgs(),EMPTY_BULK_ARGS, session.defaultSchema));
                        Plan plan = planner.plan(portal.analysis, session.jobId, 0, portal.maxRows);
                        session.executor.execute(plan, portal.rowReceiver);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        LOGGER.warn("Failed to kill job before Retry", e);
                        delegate.fail(e);
                    }
                }
            );

        }

        @Override
        public void addListener(CompletionListener listener) {
            throw new UnsupportedOperationException("not supported, listener should be registered already");
        }
    }
}
