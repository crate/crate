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
import io.crate.auth.user.User;
import io.crate.auth.user.UserManager;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Planner;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.NodeDisconnectedException;

import javax.annotation.Nullable;
import java.util.Set;

import static io.crate.auth.user.User.CRATE_USER;


@Singleton
public class SQLOperations {

    public static final Setting<Boolean> NODE_READ_ONLY_SETTING = Setting.boolSetting(
        "node.sql.read_only",
        false,
        Setting.Property.NodeScope);

    private final Analyzer analyzer;
    private final Planner planner;
    private final Provider<DependencyCarrier> executorProvider;
    private final JobsLogs jobsLogs;
    private final ClusterService clusterService;
    private final UserManager userManager;
    private final boolean isReadOnly;
    private volatile boolean disabled;

    @Inject
    public SQLOperations(Analyzer analyzer,
                         Planner planner,
                         Provider<DependencyCarrier> executorProvider,
                         JobsLogs jobsLogs,
                         Settings settings,
                         ClusterService clusterService,
                         Provider<UserManager> userManagerProvider) {
        this.analyzer = analyzer;
        this.planner = planner;
        this.executorProvider = executorProvider;
        this.jobsLogs = jobsLogs;
        this.clusterService = clusterService;
        this.userManager = userManagerProvider.get();
        this.isReadOnly = NODE_READ_ONLY_SETTING.get(settings);
    }

    private Session createSession(SessionContext sessionContext) {
        if (disabled) {
            throw new NodeDisconnectedException(clusterService.localNode(), "sql");
        }
        return new Session(
            analyzer,
            planner,
            jobsLogs,
            isReadOnly,
            executorProvider.get(),
            sessionContext);
    }

    public Session newSystemSession() {
        return createSession(new SessionContext(
            SysSchemaInfo.NAME,
            CRATE_USER,
            userManager.getStatementValidator(CRATE_USER),
            userManager.getExceptionValidator(CRATE_USER))
        );
    }

    public Session createSession(@Nullable String defaultSchema, User user) {
        return createSession(new SessionContext(defaultSchema, user,
            userManager.getStatementValidator(user), userManager.getExceptionValidator(user)));
    }

    public Session createSession(@Nullable String defaultSchema, User user, Set<Option> options, int defaultLimit) {
        return createSession(new SessionContext(defaultLimit, options, defaultSchema, user,
            userManager.getStatementValidator(user), userManager.getExceptionValidator(user)));
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
}
