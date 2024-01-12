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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.Analyzer;
import io.crate.common.unit.TimeValue;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.transport.CancelRequest;
import io.crate.execution.jobs.transport.TransportCancelAction;
import io.crate.metadata.NodeContext;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Planner;
import io.crate.protocols.postgres.KeyData;
import io.crate.role.Permission;
import io.crate.role.Role;
import io.crate.role.Securable;
import io.crate.statistics.TableStats;


@Singleton
public class Sessions {

    public static final Setting<Boolean> NODE_READ_ONLY_SETTING = Setting.boolSetting(
        "node.sql.read_only",
        false,
        Setting.Property.NodeScope);

    public static final Setting<TimeValue> STATEMENT_TIMEOUT = Setting.timeSetting(
        "statement_timeout",
        TimeValue.timeValueMillis(0),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.Exposed
    );

    public static final Setting<Integer> MEMORY_LIMIT = Setting.intSetting(
        "memory.operation_limit", 0, Property.Dynamic, Property.NodeScope, Property.Exposed);


    private static final Logger LOGGER = LogManager.getLogger(Sessions.class);

    private final NodeContext nodeCtx;
    private final Analyzer analyzer;
    private final Planner planner;
    private final Provider<DependencyCarrier> executorProvider;
    private final JobsLogs jobsLogs;
    private final ClusterService clusterService;
    private final TableStats tableStats;
    private final boolean isReadOnly;
    private final AtomicInteger nextSessionId = new AtomicInteger();
    private final ConcurrentMap<Integer, Session> sessions = new ConcurrentHashMap<>();

    private volatile boolean disabled;
    private volatile TimeValue defaultStatementTimeout;
    private volatile int memoryLimit;


    @Inject
    public Sessions(NodeContext nodeCtx,
                    Analyzer analyzer,
                    Planner planner,
                    Provider<DependencyCarrier> executorProvider,
                    JobsLogs jobsLogs,
                    Settings settings,
                    ClusterService clusterService,
                    TableStats tableStats) {
        this.nodeCtx = nodeCtx;
        this.analyzer = analyzer;
        this.planner = planner;
        this.executorProvider = executorProvider;
        this.jobsLogs = jobsLogs;
        this.clusterService = clusterService;
        this.tableStats = tableStats;
        this.isReadOnly = NODE_READ_ONLY_SETTING.get(settings);
        this.defaultStatementTimeout = STATEMENT_TIMEOUT.get(settings);
        this.memoryLimit = MEMORY_LIMIT.get(settings);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(STATEMENT_TIMEOUT, statementTimeout -> {
            this.defaultStatementTimeout = statementTimeout;
        });
        clusterSettings.addSettingsUpdateConsumer(MEMORY_LIMIT, newLimit -> {
            this.memoryLimit = newLimit;
        });
    }

    private Session newSession(CoordinatorSessionSettings sessionSettings) {
        if (disabled) {
            throw new NodeDisconnectedException(clusterService.localNode(), "sql");
        }
        int sessionId = nextSessionId.incrementAndGet();
        Session session = new Session(
            sessionId,
            nodeCtx,
            analyzer,
            planner,
            jobsLogs,
            isReadOnly,
            executorProvider.get(),
            sessionSettings,
            tableStats,
            () -> sessions.remove(sessionId)
        );
        sessions.put(sessionId, session);
        return session;
    }

    public Session newSession(@Nullable String defaultSchema, Role authenticatedUser) {
        CoordinatorSessionSettings sessionSettings;
        if (defaultSchema == null) {
            sessionSettings = new CoordinatorSessionSettings(authenticatedUser);
        } else {
            sessionSettings = new CoordinatorSessionSettings(authenticatedUser, defaultSchema);
        }
        sessionSettings.statementTimeout(defaultStatementTimeout);
        sessionSettings.memoryLimit(memoryLimit);
        return newSession(sessionSettings);
    }

    public Session newSystemSession() {
        return newSession(CoordinatorSessionSettings.systemDefaults());
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

    public boolean isEnabled() {
        return !disabled;
    }

    /**
     * @return true if a session matches the keyData, false otherwise.
     */
    public boolean cancelLocally(KeyData keyData) {
        Session session = sessions.get(keyData.pid());
        if (session != null && session.secret() == keyData.secretKey()) {
            session.cancelCurrentJob();
            return true;
        } else {
            return false;
        }
    }

    public void cancel(KeyData keyData) {
        boolean cancelled = cancelLocally(keyData);
        if (!cancelled) {
            var client = executorProvider.get().client();
            CancelRequest request = new CancelRequest(keyData);
            client.execute(TransportCancelAction.ACTION, request).whenComplete((res, err) -> {
                if (err != null) {
                    LOGGER.error("Error during cancel broadcast", err);
                }
            });
        }
    }

    public Iterable<Session> getActive() {
        return sessions.values();
    }

    public Iterable<Cursor> getCursors(Role user) {
        return () -> sessions.values().stream()
            .filter(session ->
                nodeCtx.roles().hasPrivilege(user, Permission.AL, Securable.CLUSTER, null)
                || session.sessionSettings().sessionUser().equals(user))
            .flatMap(session -> StreamSupport.stream(session.cursors.spliterator(), false))
            .iterator();
    }
}
