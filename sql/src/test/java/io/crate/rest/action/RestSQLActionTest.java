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

package io.crate.rest.action;

import io.crate.action.sql.SQLOperations;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.operation.auth.AuthenticationProvider;
import io.crate.operation.user.UserManager;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.DummyUserManager;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class RestSQLActionTest extends CrateUnitTest {

    private static final Provider<UserManager> USER_MANAGER_PROVIDER = DummyUserManager::new;

    private final SQLOperations sqlOperations = mock(SQLOperations.class);
    private final RestController restController = mock(RestController.class);
    private final CrateCircuitBreakerService circuitBreakerService = mock(CrateCircuitBreakerService.class);

    @Test
    public void testDefaultUserIfHttpHeaderNotPresent() throws Exception {
        RestSQLAction restSQLAction = new RestSQLAction(
            Settings.EMPTY,
            restController,
            sqlOperations,
            USER_MANAGER_PROVIDER,
            circuitBreakerService
        );
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Collections.emptyMap())
            .build();
        assertThat(restSQLAction.userFromRequest(request).name(), is("crate"));
    }

    @Test
    public void testSettingUserIfHttpHeaderNotPresent() throws Exception {
        Settings settings = Settings.builder()
            .put(AuthenticationProvider.AUTH_TRUST_HTTP_DEFAULT_HEADER.getKey(), "trillian")
            .build();
        RestSQLAction restSQLAction = new RestSQLAction(
            settings,
            restController,
            sqlOperations,
            USER_MANAGER_PROVIDER,
            circuitBreakerService
        );
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Collections.emptyMap())
            .build();
        assertThat(restSQLAction.userFromRequest(request).name(), is("trillian"));
    }

    @Test
    public void testUserIfHttpHeaderIsPresent() throws Exception {
        RestSQLAction restSQLAction = new RestSQLAction(
            Settings.EMPTY,
            restController,
            sqlOperations,
            USER_MANAGER_PROVIDER,
            circuitBreakerService
        );
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Collections.singletonMap("X-User", "other"))
            .build();
        assertThat(restSQLAction.userFromRequest(request).name(), is("other"));

    }
}
