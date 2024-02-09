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

package io.crate.rest.action;

import static io.crate.role.metadata.RolesHelper.JWT_TOKEN;
import static io.crate.role.metadata.RolesHelper.JWT_USER;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfigBuilder;
import org.junit.Test;

import io.crate.action.sql.Session;
import io.crate.action.sql.Sessions;
import io.crate.auth.AccessControl;
import io.crate.auth.AuthSettings;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.role.Role;
import io.crate.role.metadata.RolesHelper;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;

public class SqlHttpHandlerTest {

    @Test
    public void testDefaultUserIfHttpHeaderNotPresent() {
        SqlHttpHandler handler = new SqlHttpHandler(
            Settings.EMPTY,
            mock(Sessions.class),
            (s) -> new NoopCircuitBreaker("dummy"),
            () -> List.of(Role.CRATE_USER),
            sessionSettings -> AccessControl.DISABLED,
            Netty4CorsConfigBuilder.forAnyOrigin().build()
        );

        Role user = handler.userFromAuthHeader(null);
        assertThat(user, is(Role.CRATE_USER));
    }

    @Test
    public void testSettingUserIfHttpHeaderNotPresent() {
        Settings settings = Settings.builder()
            .put(AuthSettings.AUTH_TRUST_HTTP_DEFAULT_HEADER.getKey(), "trillian")
            .build();
        SqlHttpHandler handler = new SqlHttpHandler(
            settings,
            mock(Sessions.class),
            (s) -> new NoopCircuitBreaker("dummy"),
            () -> List.of(RolesHelper.userOf("trillian")),
            sessionSettings -> AccessControl.DISABLED,
            Netty4CorsConfigBuilder.forAnyOrigin().build()
        );

        Role user = handler.userFromAuthHeader(null);
        assertThat(user.name(), is("trillian"));
    }

    @Test
    public void testUserIfHttpBasicAuthIsPresent() {
        SqlHttpHandler handler = new SqlHttpHandler(
            Settings.EMPTY,
            mock(Sessions.class),
            (s) -> new NoopCircuitBreaker("dummy"),
            () -> List.of(RolesHelper.userOf("Aladdin")),
            sessionSettings -> AccessControl.DISABLED,
            Netty4CorsConfigBuilder.forAnyOrigin().build()
        );

        Role user = handler.userFromAuthHeader("Basic QWxhZGRpbjpPcGVuU2VzYW1l");
        assertThat(user.name(), is("Aladdin"));
    }

    @Test
    public void testSessionSettingsArePreservedAcrossRequests() {
        Role dummyUser = RolesHelper.userOf("crate");
        var sessionSettings = new CoordinatorSessionSettings(dummyUser);

        var mockedSession = mock(Session.class);
        when(mockedSession.sessionSettings()).thenReturn(sessionSettings);

        var mockedSqlOperations = mock(Sessions.class);
        when(mockedSqlOperations.newSession(null, dummyUser)).thenReturn(mockedSession);

        var mockedRequest = mock(FullHttpRequest.class);
        when(mockedRequest.headers()).thenReturn(new DefaultHttpHeaders());

        SqlHttpHandler handler = new SqlHttpHandler(
            Settings.EMPTY,
            mockedSqlOperations,
            (s) -> new NoopCircuitBreaker("dummy"),
            () -> List.of(dummyUser),
            settings -> AccessControl.DISABLED,
            Netty4CorsConfigBuilder.forAnyOrigin().build()
        );

        // 1st call to ensureSession creates a session instance bound to 'dummyUser'
        var session = handler.ensureSession(mockedRequest);
        verify(mockedRequest, atLeast(1)).headers();
        assertThat(session.sessionSettings().authenticatedUser(), is(dummyUser));
        assertThat(session.sessionSettings().searchPath().currentSchema(), containsString("doc"));
        assertTrue(session.sessionSettings().hashJoinsEnabled());

        // modify the session settings
        session.sessionSettings().setSearchPath("dummy_path");
        session.sessionSettings().setHashJoinEnabled(false);

        // test that the 2nd call to ensureSession will retrieve the session settings modified previously
        session = handler.ensureSession(mockedRequest);
        assertFalse(session.sessionSettings().hashJoinsEnabled());
        assertThat(session.sessionSettings().searchPath().currentSchema(), containsString("dummy_path"));
    }

    @Test
    public void test_resolve_user_from_jwt_token() {
        SqlHttpHandler handler = new SqlHttpHandler(
            Settings.EMPTY,
            mock(Sessions.class),
            (s) -> new NoopCircuitBreaker("dummy"),
            () -> List.of(JWT_USER),
            sessionSettings -> AccessControl.DISABLED,
            Netty4CorsConfigBuilder.forAnyOrigin().build()
        );

        Role resolvedUser = handler.userFromAuthHeader("bearer " + JWT_TOKEN);
        assertThat(resolvedUser.name(), is(JWT_USER.name()));
    }
}

