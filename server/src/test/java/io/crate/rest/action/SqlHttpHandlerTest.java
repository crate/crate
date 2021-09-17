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

import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.action.sql.SessionContext;
import io.crate.auth.AuthSettings;
import io.crate.auth.AccessControl;
import io.crate.user.User;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfigBuilder;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SqlHttpHandlerTest {

    @Test
    public void testDefaultUserIfHttpHeaderNotPresent() {
        SqlHttpHandler handler = new SqlHttpHandler(
            Settings.EMPTY,
            mock(SQLOperations.class),
            (s) -> new NoopCircuitBreaker("dummy"),
            userName -> {
                if (userName.equals("crate")) {
                    return User.CRATE_USER;
                }
                return null;
            },
            sessionContext -> AccessControl.DISABLED,
            Netty4CorsConfigBuilder.forAnyOrigin().build()
        );

        User user = handler.userFromAuthHeader(null);
        assertThat(user, is(User.CRATE_USER));
    }

    @Test
    public void testSettingUserIfHttpHeaderNotPresent() {
        Settings settings = Settings.builder()
            .put(AuthSettings.AUTH_TRUST_HTTP_DEFAULT_HEADER.getKey(), "trillian")
            .build();
        SqlHttpHandler handler = new SqlHttpHandler(
            settings,
            mock(SQLOperations.class),
            (s) -> new NoopCircuitBreaker("dummy"),
            User::of,
            sessionContext -> AccessControl.DISABLED,
            Netty4CorsConfigBuilder.forAnyOrigin().build()
        );

        User user = handler.userFromAuthHeader(null);
        assertThat(user.name(), is("trillian"));
    }

    @Test
    public void testUserIfHttpBasicAuthIsPresent() {
        SqlHttpHandler handler = new SqlHttpHandler(
            Settings.EMPTY,
            mock(SQLOperations.class),
            (s) -> new NoopCircuitBreaker("dummy"),
            User::of,
            sessionContext -> AccessControl.DISABLED,
            Netty4CorsConfigBuilder.forAnyOrigin().build()
        );

        User user = handler.userFromAuthHeader("Basic QWxhZGRpbjpPcGVuU2VzYW1l");
        assertThat(user.name(), is("Aladdin"));
    }

    @Test
    public void testSessionSettingsArePreservedAcrossRequests() {
        User dummyUser = User.of("dummy");
        var sessionConext = new SessionContext(dummyUser);

        var mockedSession = mock(Session.class);
        when(mockedSession.sessionContext()).thenReturn(sessionConext);

        var mockedSqlOperations = mock(SQLOperations.class);
        when(mockedSqlOperations.createSession(null, dummyUser)).thenReturn(mockedSession);

        var mockedRequest = mock(FullHttpRequest.class);
        when(mockedRequest.headers()).thenReturn(new DefaultHttpHeaders());

        SqlHttpHandler handler = new SqlHttpHandler(
            Settings.EMPTY,
            mockedSqlOperations,
            (s) -> new NoopCircuitBreaker("dummy"),
            userName -> dummyUser,
            sessionContext -> AccessControl.DISABLED,
            Netty4CorsConfigBuilder.forAnyOrigin().build()
        );

        // 1st call to ensureSession creates a session instance bound to 'dummyUser'
        var session = handler.ensureSession(mockedRequest);
        verify(mockedRequest, atLeast(1)).headers();
        assertThat(session.sessionContext().authenticatedUser(), is(dummyUser));
        assertThat(session.sessionContext().searchPath().currentSchema(), containsString("doc"));
        assertTrue(session.sessionContext().isHashJoinEnabled());

        // modify the session settings
        session.sessionContext().setSearchPath("dummy_path");
        session.sessionContext().setHashJoinEnabled(false);

        // test that the 2nd call to ensureSession will retrieve the session settings modified previously
        session = handler.ensureSession(mockedRequest);
        assertFalse(session.sessionContext().isHashJoinEnabled());
        assertThat(session.sessionContext().searchPath().currentSchema(), containsString("dummy_path"));
    }
}

