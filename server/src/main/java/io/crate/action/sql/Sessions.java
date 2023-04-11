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

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.NodeDisconnectedException;

import io.crate.analyze.Analyzer;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.transport.CancelRequest;
import io.crate.execution.jobs.transport.TransportCancelAction;
import io.crate.metadata.NodeContext;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Planner;
import io.crate.protocols.postgres.KeyData;
import io.crate.user.Privilege.Clazz;
import io.crate.user.Privilege.Type;
import io.crate.user.User;


@Singleton
public class Sessions {

    public static final Setting<Boolean> NODE_READ_ONLY_SETTING = Setting.boolSetting(
        "node.sql.read_only",
        false,
        Setting.Property.NodeScope);

    private static final Logger LOGGER = LogManager.getLogger(Sessions.class);

    private final NodeContext nodeCtx;
    private final Analyzer analyzer;
    private final Planner planner;
    private final Provider<DependencyCarrier> executorProvider;
    private final JobsLogs jobsLogs;
    private final ClusterService clusterService;
    private final boolean isReadOnly;
    private final AtomicInteger nextSessionId = new AtomicInteger();
    private final ConcurrentMap<Integer, Session> sessions = new ConcurrentHashMap<>();
    private volatile boolean disabled;


    @Inject
    public Sessions(NodeContext nodeCtx,
                    Analyzer analyzer,
                    Planner planner,
                    Provider<DependencyCarrier> executorProvider,
                    JobsLogs jobsLogs,
                    Settings settings,
                    ClusterService clusterService) {
        this.nodeCtx = nodeCtx;
        this.analyzer = analyzer;
        this.planner = planner;
        this.executorProvider = executorProvider;
        this.jobsLogs = jobsLogs;
        this.clusterService = clusterService;
        this.isReadOnly = NODE_READ_ONLY_SETTING.get(settings);
    }

    private Session createSession(CoordinatorSessionSettings sessionSettings) {
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
            () -> sessions.remove(sessionId)
        );
        sessions.put(sessionId, session);
        return session;
    }

    public Session newSystemSession() {
        return createSession(CoordinatorSessionSettings.systemDefaults());
    }

    public Session createSession(@Nullable String defaultSchema, User authenticatedUser) {
        CoordinatorSessionSettings sessionSettings;
        if (defaultSchema == null) {
            sessionSettings = new CoordinatorSessionSettings(authenticatedUser);
        } else {
            sessionSettings = new CoordinatorSessionSettings(authenticatedUser, defaultSchema);
        }

        return createSession(sessionSettings);
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

    public Iterable<Cursor> getCursors(User user) {
        return () -> sessions.values().stream()
            .filter(session ->
                user.hasPrivilege(Type.AL, Clazz.CLUSTER, null, session.sessionSettings().currentSchema())
                || session.sessionSettings().sessionUser().equals(user))
            .flatMap(session -> StreamSupport.stream(session.cursors.spliterator(), false))
            .iterator();
    }
}
