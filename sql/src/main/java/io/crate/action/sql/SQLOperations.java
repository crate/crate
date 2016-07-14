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
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.concurrent.CompletionListener;
import io.crate.exceptions.ReadOnlyException;
import io.crate.executor.Executor;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.statement.SetSessionPlan;
import io.crate.protocols.postgres.FormatCodes;
import io.crate.protocols.postgres.Portal;
import io.crate.protocols.postgres.SimplePortal;
import io.crate.protocols.postgres.StatsTablesUpdateListener;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.BeginStatement;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

        private Portal getOrCreatePortal(String portalName) {
            Portal portal = portals.get(portalName);
            if (portal == null) {
                portal = new SimplePortal(portalName, jobId, defaultSchema, analyzer, executor,
                                          transportKillJobsNodeAction, isReadOnly);
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

        private void validateReadOnly(Analysis analysis) {
            if (analysis != null && analysis.analyzedStatement().isWriteOperation() && isReadOnly) {
                throw new ReadOnlyException();
            }
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
                    if (this.statement instanceof BeginStatement) {
                        sync(CompletionListener.NO_OP);
                    } else {
                        throw new UnsupportedOperationException("Cannot use mixed statements in batch operations");
                    }
                }
                this.statement = statement;
                this.query = query;
                this.paramTypes = paramTypes;
            } catch (Throwable t) {
                throwable = t;
                throw t;
            }
        }

        public void bind(String portalName,
                         String statementName,
                         List<Object> params,
                         @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
            LOGGER.debug("method=bind portalName={} statementName={} params={}", portalName, statementName, params);
            checkError();

            try {
                Portal portal = getOrCreatePortal(portalName);
                portal = portal.bind(statementName, query, statement, params, resultFormatCodes);
                portals.put(portalName, portal);
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
                return portal.describe();
            } catch (Throwable t) {
                throwable = t;
                throw t;
            }
        }

        public void execute(String portalName, int maxRows, ResultReceiver resultReceiver) {
            LOGGER.debug("method=execute portalName={} maxRows={}", portalName, maxRows);

            checkError();
            try {
                Portal portal = getSafePortal(portalName);
                portal.execute(resultReceiver, maxRows);
                lastPortalName = portalName;
            } catch (Throwable t) {
                throwable = t;
                throw t;
            }
        }

        public void sync(CompletionListener listener) {
            LOGGER.debug("method=sync");

            if (throwable == null) {
                Portal portal = getSafePortal(lastPortalName);
                Plan plan = portal.prepareSync(planner);
                applySessionSettings(plan);
                portal.sync(planner, statsTables, listener);
                cleanup(lastPortalName);
            } else {
                Throwable t = this.throwable;
                cleanup(lastPortalName);
                throw Throwables.propagate(t);
            }
        }

        private void applySessionSettings(Plan plan) {
            if (plan instanceof SetSessionPlan) {
                settings.putAll(((SetSessionPlan) plan).settings().getAsMap());
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
            return portal.getOutputTypes();
        }

        public String getQuery(String portalName) {
            return getSafePortal(portalName).getLastQuery();
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
            return portal.getResultFormatCodes();
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

        /**
         * Register an error to make all methods up to sync no-ops.
         * Sync will raise the error and clear the state so that further method calls work again.
         */
        public void setThrowable(Throwable t) {
            this.throwable = t;
        }
    }
}
