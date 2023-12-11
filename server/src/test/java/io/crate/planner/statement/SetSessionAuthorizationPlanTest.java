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

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import io.crate.auth.AccessControl;
import io.crate.data.Row;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.role.Role;
import io.crate.role.RoleManager;

public class SetSessionAuthorizationPlanTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private RoleManager roleManager;

    @Before
    public void beforeEach() {
        roleManager = mock(RoleManager.class);
        when(roleManager.getAccessControl(any(CoordinatorSessionSettings.class)))
            .thenReturn(AccessControl.DISABLED);
        e = SQLExecutor.builder(clusterService).setUserManager(roleManager).build();
    }

    @Test
    public void test_set_local_session_auth_results_in_noop() {
        assertThat(
            e.plan("SET LOCAL SESSION AUTHORIZATION DEFAULT"),
            instanceOf(NoopPlan.class)
        );
    }

    @Test
    public void test_set_session_auth_modifies_the_session_user() throws Exception {
        var sessionSettings = e.getSessionSettings();
        sessionSettings.setSessionUser(Role.CRATE_USER);
        var user = Role.userOf("test");
        when(roleManager.findUser(eq(user.name()))).thenReturn(user);

        execute(e.plan("SET SESSION AUTHORIZATION " + user.name()));

        assertThat(sessionSettings.sessionUser(), is(user));
    }

    @Test
    public void test_set_session_auth_to_default_sets_session_user_to_authenticated_user() throws Exception {
        var sessionSettings = e.getSessionSettings();
        sessionSettings.setSessionUser(Role.userOf("test"));
        assertThat(
            sessionSettings.sessionUser(),
            is(not(sessionSettings.authenticatedUser()))
        );

        execute(e.plan("SET SESSION AUTHORIZATION DEFAULT"));

        assertThat(
            sessionSettings.sessionUser(),
            is(sessionSettings.authenticatedUser())
        );
    }

    @Test
    public void test_set_session_auth_to_unknown_user_results_in_exception() throws Exception {
        when(roleManager.findUser(eq("unknown_user"))).thenReturn(null);
        Plan plan = e.plan("SET SESSION AUTHORIZATION 'unknown_user'");

        expectedException.expectMessage("User 'unknown_user' does not exist.");
        expectedException.expect(IllegalArgumentException.class);
        execute(plan);
    }

    private void execute(Plan plan) throws Exception {
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
