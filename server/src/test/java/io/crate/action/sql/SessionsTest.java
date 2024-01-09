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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import io.crate.analyze.Analyzer;
import io.crate.common.unit.TimeValue;
import io.crate.data.InMemoryBatchIterator;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.transport.CancelRequest;
import io.crate.execution.jobs.transport.TransportCancelAction;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Planner;
import io.crate.protocols.postgres.KeyData;
import io.crate.role.Privilege;
import io.crate.role.PrivilegeState;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.metadata.RolesHelper;
import io.crate.sql.tree.Declare.Hold;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class SessionsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_sessions_broadcasts_cancel_if_no_local_match() throws Exception {
        Functions functions = new Functions(Map.of());
        Roles roles = () -> List.of(Role.CRATE_USER);
        NodeContext nodeCtx = new NodeContext(functions, roles);
        DependencyCarrier dependencies = mock(DependencyCarrier.class);
        ElasticsearchClient client = mock(ElasticsearchClient.class, Answers.RETURNS_MOCKS);
        when(dependencies.client()).thenReturn(client);
        Sessions sessions = new Sessions(
            nodeCtx,
            mock(Analyzer.class),
            mock(Planner.class),
            () -> dependencies,
            new JobsLogs(() -> false),
            Settings.EMPTY,
            clusterService,
            new TableStats()
        );

        KeyData keyData = new KeyData(10, 20);
        sessions.cancel(keyData);
        verify(client).execute(
            Mockito.eq(TransportCancelAction.ACTION),
            Mockito.eq(new CancelRequest(keyData))
        );
    }

    @Test
    public void test_super_user_and_al_privileges_can_view_all_cursors() throws Exception {
        Functions functions = new Functions(Map.of());
        Roles roles = () -> List.of(Role.CRATE_USER);
        NodeContext nodeCtx = new NodeContext(functions, roles);
        Sessions sessions = newSessions(nodeCtx);
        Session session1 = sessions.newSession("doc", RolesHelper.userOf("Arthur"));
        session1.cursors.add("c1", newCursor());

        Session session2 = sessions.newSession("doc", RolesHelper.userOf("Trillian"));
        session2.cursors.add("c2", newCursor());

        assertThat(sessions.getCursors(Role.CRATE_USER)).hasSize(2);

        var ALprivilege = new Privilege(
            PrivilegeState.GRANT,
            Privilege.Type.AL,
            Privilege.Clazz.CLUSTER,
            null,
            "crate"
        );
        Role admin = RolesHelper.userOf("admin", Set.of(ALprivilege), null);
        assertThat(sessions.getCursors(admin)).hasSize(2);
    }

    @Test
    public void test_user_can_only_view_their_own_cursors() throws Exception {
        Functions functions = new Functions(Map.of());
        Roles roles = () -> List.of(Role.CRATE_USER);
        NodeContext nodeCtx = new NodeContext(functions, roles);
        Sessions sessions = newSessions(nodeCtx);

        Role arthur = RolesHelper.userOf("Arthur");
        Session session1 = sessions.newSession("doc", arthur);
        session1.cursors.add("c1", newCursor());

        Role trillian = RolesHelper.userOf("Trillian");
        Session session2 = sessions.newSession("doc", trillian);
        session2.cursors.add("c2", newCursor());

        assertThat(sessions.getCursors(arthur)).hasSize(1);
        assertThat(sessions.getCursors(trillian)).hasSize(1);
    }

    @Test
    public void test_uses_global_statement_timeout_as_default_for() throws Exception {
        Functions functions = new Functions(Map.of());
        Roles roles = () -> List.of(Role.CRATE_USER);
        NodeContext nodeCtx = new NodeContext(functions, roles);
        Sessions sessions = new Sessions(
            nodeCtx,
            mock(Analyzer.class),
            mock(Planner.class),
            () -> mock(DependencyCarrier.class),
            new JobsLogs(() -> false),
            Settings.builder()
                .put("statement_timeout", "30s")
                .build(),
            clusterService,
            new TableStats()
        );
        Session session = sessions.newSession("doc", Role.CRATE_USER);
        assertThat(session.sessionSettings().statementTimeout())
            .isEqualTo(TimeValue.timeValueSeconds(30));
    }

    private Sessions newSessions(NodeContext nodeCtx) {
        Sessions sessions = new Sessions(
            nodeCtx,
            mock(Analyzer.class),
            mock(Planner.class),
            () -> mock(DependencyCarrier.class),
            new JobsLogs(() -> false),
            Settings.EMPTY,
            clusterService,
            new TableStats()
        );
        return sessions;
    }

    private static Cursor newCursor() {
        return new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare ..",
            false,
            Hold.WITH,
            CompletableFuture.completedFuture(InMemoryBatchIterator.empty(null)),
            new CompletableFuture<>(),
            List.of()
        );
    }
}
