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

import io.crate.analyze.Analyzer;
import io.crate.analyze.symbol.Field;
import io.crate.concurrent.CompletionListener;
import io.crate.exceptions.Exceptions;
import io.crate.executor.Executor;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Planner;
import io.crate.protocols.postgres.FormatCodes;
import io.crate.protocols.postgres.Portal;
import io.crate.protocols.postgres.SimplePortal;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.NodeDisconnectedException;

import javax.annotation.Nullable;
import java.util.*;


@Singleton
public class SQLOperations {

    public final static String NODE_READ_ONLY_SETTING = "node.sql.read_only";
    private final static ESLogger LOGGER = Loggers.getLogger(SQLOperations.class);

    // Parser can't handle empty statement but postgres requires support for it.
    // This rewrite is done so that bind/describe calls on an empty statement will work as well
    private static final Statement EMPTY_STMT = SqlParser.createStatement("select '' from sys.cluster limit 0");

    private final Analyzer analyzer;
    private final Planner planner;
    private final Provider<Executor> executorProvider;
    private final Provider<TransportKillJobsNodeAction> transportKillJobsNodeActionProvider;
    private final StatsTables statsTables;
    private final ClusterService clusterService;
    private final boolean isReadOnly;
    private volatile boolean disabled;

    public enum Option {
        ALLOW_QUOTED_SUBSCRIPT;

        public static final EnumSet<Option> NONE = EnumSet.noneOf(Option.class);
    }

    @Inject
    public SQLOperations(Analyzer analyzer,
                         Planner planner,
                         Provider<Executor> executorProvider,
                         Provider<TransportKillJobsNodeAction> transportKillJobsNodeActionProvider,
                         StatsTables statsTables,
                         Settings settings,
                         ClusterService clusterService) {
        this.analyzer = analyzer;
        this.planner = planner;
        this.executorProvider = executorProvider;
        this.transportKillJobsNodeActionProvider = transportKillJobsNodeActionProvider;
        this.statsTables = statsTables;
        this.clusterService = clusterService;
        this.isReadOnly = settings.getAsBoolean(NODE_READ_ONLY_SETTING, false);
    }

    public Session createSession(@Nullable String defaultSchema, Set<Option> options, int defaultLimit) {
        if (disabled) {
            throw new NodeDisconnectedException(clusterService.localNode(), "sql");
        }

        return new Session(
            executorProvider.get(),
            transportKillJobsNodeActionProvider.get(),
            defaultSchema,
            defaultLimit,
            options
        );
    }

    /**
     * Disable processing of new sql statements.
     * {@link io.crate.cluster.gracefulstop.DecommissioningService} must call this while before starting to decommission.
     */
    public void disable() {
        disabled = true;
    }

    /**
     * (Re-)Enable processing of new sql statements
     * {@link io.crate.cluster.gracefulstop.DecommissioningService} must call this when decommissioning is aborted.
     */
    public void enable() {
        disabled = false;
    }

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
    public class Session {

        private static final String UNNAMED = "";
        private final Executor executor;
        private final TransportKillJobsNodeAction transportKillJobsNodeAction;
        private final String defaultSchema;
        private final int defaultLimit;
        private final Set<Option> options;

        private final Map<String, PreparedStmt> preparedStatements = new HashMap<>();
        private final Map<String, Portal> portals = new HashMap<>();
        private final Set<Portal> pendingExecutions = Collections.newSetFromMap(new IdentityHashMap<Portal, Boolean>());

        private Session(Executor executor,
                        TransportKillJobsNodeAction transportKillJobsNodeAction,
                        String defaultSchema,
                        int defaultLimit,
                        Set<Option> options) {
            this.executor = executor;
            this.transportKillJobsNodeAction = transportKillJobsNodeAction;
            this.defaultSchema = defaultSchema;
            this.defaultLimit = defaultLimit;
            this.options = options;
        }

        private Portal getOrCreatePortal(String portalName) {
            Portal portal = portals.get(portalName);
            if (portal == null) {
                portal = new SimplePortal(
                    portalName, defaultSchema, options, analyzer, executor, transportKillJobsNodeAction, isReadOnly, defaultLimit);
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

        public void parse(String statementName, String query, List<DataType> paramTypes) {
            LOGGER.debug("method=parse stmtName={} query={} paramTypes={}", statementName, query, paramTypes);

            Statement statement;
            try {
                statement = SqlParser.createStatement(query);
            } catch (Throwable t) {
                if ("".equals(query)) {
                    statement = EMPTY_STMT;
                } else {
                    statsTables.logPreExecutionFailure(UUID.randomUUID(), query, Exceptions.messageOf(t));
                    throw Exceptions.createSQLActionException(t);
                }
            }
            preparedStatements.put(statementName, new PreparedStmt(statement, query, paramTypes));
        }

        public void bind(String portalName,
                         String statementName,
                         List<Object> params,
                         @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
            LOGGER.debug("method=bind portalName={} statementName={} params={}", portalName, statementName, params);

            Portal portal = getOrCreatePortal(portalName);
            try {
                PreparedStmt preparedStmt = getSafeStmt(statementName);
                Portal newPortal = portal.bind(
                    statementName, preparedStmt.query(), preparedStmt.statement(), params, resultFormatCodes);
                if (portal != newPortal) {
                    portals.put(portalName, newPortal);
                    pendingExecutions.remove(portal);
                }
            } catch (Throwable t) {
                statsTables.logPreExecutionFailure(UUID.randomUUID(), portal.getLastQuery(), Exceptions.messageOf(t));
                throw Exceptions.createSQLActionException(t);
            }
        }

        public List<Field> describe(char type, String portalOrStatement) {
            LOGGER.debug("method=describe type={} portalOrStatement={}", type, portalOrStatement);
            switch (type) {
                case 'P':
                    Portal portal = getSafePortal(portalOrStatement);
                    return portal.describe();
                case 'S':
                    /*
                     * describe might be called without prior bind call. E.g. in batch insert case the statement is prepared first:
                     *
                     *      parse stmtName=S_1 query=insert into t (x) values ($1) paramTypes=[integer]
                     *      describe type=S portalOrStatement=S_1
                     *      sync
                     *
                     * and then per batch:
                     *
                     *      bind portalName= statementName=S_1 params=[0]
                     *      describe type=P portalOrStatement=
                     *      execute
                     *
                     *      bind portalName= statementName=S_1 params=[1]
                     *      describe type=P portalOrStatement=
                     *      execute
                     *
                     * and finally:
                     *
                     *      sync
                     *
                     * Returning null (= "NoData") is correct for insert statements but will cause errors
                     * in JDBC since 9.4.1210
                     *
                     * To prevent describe calls without prior bind calls
                     * it's necessary to set prepareThreshold to 0 in the Connection properties.
                     */
                    return null;
            }
            throw new AssertionError("Unsupported type: " + type);
        }

        public void execute(String portalName, int maxRows, ResultReceiver resultReceiver) {
            LOGGER.debug("method=execute portalName={} maxRows={}", portalName, maxRows);

            Portal portal = getSafePortal(portalName);
            portal.execute(resultReceiver, maxRows);
            if (portal.getLastQuery().equalsIgnoreCase("BEGIN")) {
                portal.sync(planner, statsTables, CompletionListener.NO_OP);
                clearState();
            } else {
                // delay execution to be able to bundle bulk operations
                pendingExecutions.add(portal);
            }
        }

        public void sync(CompletionListener listener) {
            LOGGER.debug("method=sync");

            switch (pendingExecutions.size()) {
                case 0:
                    listener.onSuccess(null);
                    return;

                case 1:
                    Portal portal = pendingExecutions.iterator().next();
                    pendingExecutions.clear();
                    clearState();
                    portal.sync(planner, statsTables, listener);
                    if (UNNAMED.equals(portal.name())) {
                        portal.close();
                    }
                    return;
            }

            throw new IllegalStateException("Shouldn't have more than 1 pending execution. Got: " + pendingExecutions);
        }

        public void clearState() {
            portals.remove(UNNAMED);
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
            return stmt.paramTypes().get(idx);
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
            }
            throw new IllegalArgumentException("Invalid type: " + type);
        }

        public void close() {
            for (Portal portal : portals.values()) {
                portal.close();
            }
        }
    }
}
