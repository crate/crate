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

import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SQLOperations;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.operation.auth.AuthSettings;
import io.crate.operation.user.UserManager;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.DummyUserManager;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.SecureString;
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
            .put(AuthSettings.AUTH_TRUST_HTTP_DEFAULT_HEADER.getKey(), "trillian")
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
    public void testUserIfHttpBasicAuthIsPresent() {
        RestSQLAction restSQLAction = new RestSQLAction(
            Settings.EMPTY,
            restController,
            sqlOperations,
            USER_MANAGER_PROVIDER,
            circuitBreakerService
        );
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(
                Collections.singletonMap(HttpHeaderNames.AUTHORIZATION.toString(),
                    Collections.singletonList("Basic QWxhZGRpbjpPcGVuU2VzYW1l")))
            .build();
        assertThat(restSQLAction.userFromRequest(request).name(), is("Aladdin"));
    }

    @Test
    public void testUserIfHttpUserHeaderIsPresent() {
        RestSQLAction restSQLAction = new RestSQLAction(
            Settings.EMPTY,
            restController,
            sqlOperations,
            USER_MANAGER_PROVIDER,
            circuitBreakerService
        );
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Collections.singletonMap(AuthSettings.HTTP_HEADER_USER, Collections.singletonList("other")))
            .build();
        assertThat(restSQLAction.userFromRequest(request).name(), is("other"));
    }

    @Test
    public void testUserIfBothHeadersArePresent() {
        RestSQLAction restSQLAction = new RestSQLAction(
            Settings.EMPTY,
            restController,
            sqlOperations,
            USER_MANAGER_PROVIDER,
            circuitBreakerService
        );
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(ImmutableMap.of(
                AuthSettings.HTTP_HEADER_USER, Collections.singletonList("other"),
                HttpHeaderNames.AUTHORIZATION.toString(), Collections.singletonList("Basic QWxhZGRpbjpPcGVuU2VzYW1l")))
            .build();

        // HTTP Basic Auth Header has higher priority
        assertThat(restSQLAction.userFromRequest(request).name(), is("Aladdin"));
    }

    @Test
    public void testExtractUsernamePasswordFromHttpBasicAuthHeader() {
        Tuple<String, SecureString> creds =
            RestSQLAction.extractCredentialsFromHttpBasicAuthHeader("Basic QXJ0aHVyOkV4Y2FsaWJ1cg==");
        assertThat(creds.v1(), is("Arthur"));
        assertThat(creds.v2().toString(), is("Excalibur"));
    }

    @Test
    public void testExtractUsernamePasswordWithSemiColonsFromHttpBasicAuthHeader() {
        Tuple<String, SecureString> creds =
            RestSQLAction.extractCredentialsFromHttpBasicAuthHeader("Basic QXJ0aHVyOjp0ZXN0OnBhc3N3b3JkOg==");
        assertThat(creds.v1(), is("Arthur"));
        assertThat(creds.v2().toString(), is(":test:password:"));
    }
}
