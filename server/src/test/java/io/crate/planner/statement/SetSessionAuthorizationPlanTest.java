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

package io.crate.planner.statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.junit.Test;

import io.crate.data.Row;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.role.Role;
import io.crate.role.StubRoleManager;
import io.crate.role.metadata.RolesHelper;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class SetSessionAuthorizationPlanTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_set_local_session_auth_results_in_noop() {
        var e = SQLExecutor.builder(clusterService).build();
        assertThat((Plan) e.plan("SET LOCAL SESSION AUTHORIZATION DEFAULT"))
            .isExactlyInstanceOf(NoopPlan.class);
    }

    @Test
    public void test_set_session_auth_modifies_the_session_user() throws Exception {
        var user = RolesHelper.userOf("test");
        var e = SQLExecutor.builder(clusterService)
            .setUserManager(new StubRoleManager(List.of(user, Role.CRATE_USER)))
            .build();
        var sessionSettings = e.getSessionSettings();
        sessionSettings.setSessionUser(Role.CRATE_USER);

        execute(e, e.plan("SET SESSION AUTHORIZATION " + user.name()));

        assertThat(sessionSettings.sessionUser()).isEqualTo(user);
    }

    @Test
    public void test_set_session_auth_to_default_sets_session_user_to_authenticated_user() throws Exception {
        var e = SQLExecutor.builder(clusterService).build();

        var sessionSettings = e.getSessionSettings();
        sessionSettings.setSessionUser(RolesHelper.userOf("test"));
        assertThat(sessionSettings.sessionUser()).isNotEqualTo(sessionSettings.authenticatedUser());

        execute(e, e.plan("SET SESSION AUTHORIZATION DEFAULT"));

        assertThat(sessionSettings.sessionUser()).isEqualTo(sessionSettings.authenticatedUser());
    }

    @Test
    public void test_set_session_auth_to_unknown_user_results_in_exception() throws Exception {
        var e = SQLExecutor.builder(clusterService).build();
        Plan plan = e.plan("SET SESSION AUTHORIZATION 'unknown_user'");
        assertThatThrownBy(() -> execute(e, plan))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("User 'unknown_user' does not exist.");
    }

    private void execute(SQLExecutor e, Plan plan) throws Exception {
        var consumer = new TestingRowConsumer();
        plan.execute(
            mock(DependencyCarrier.class),
            e.getPlannerContext(clusterService.state()),
            consumer,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
        consumer.getResult();
    }
}
