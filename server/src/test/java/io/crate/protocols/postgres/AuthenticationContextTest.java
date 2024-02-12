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

package io.crate.protocols.postgres;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.net.InetAddress;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.auth.AlwaysOKAuthentication;
import io.crate.auth.Authentication;
import io.crate.auth.AuthenticationMethod;
import io.crate.auth.Credentials;
import io.crate.auth.Protocol;
import io.crate.role.Role;


public class AuthenticationContextTest extends ESTestCase {

    private static final Authentication AUTHENTICATION = new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER));

    @Test
    public void testAuthenticationContextCycle() throws Exception {
        String userName = "crate";
        char[] passwd = "passwd".toCharArray();
        ConnectionProperties connProperties = new ConnectionProperties(
            InetAddress.getByName("127.0.0.1"), Protocol.POSTGRES, null);
        AuthenticationMethod authMethod = AUTHENTICATION.resolveAuthenticationType(userName, connProperties);
        AuthenticationContext authContext = new AuthenticationContext(
            authMethod, connProperties, Credentials.fromNameAndPassword(userName, null), LogManager.getLogger(AuthenticationContextTest.class));
        authContext.setSecurePassword(passwd);
        assertThat(authContext.authenticate(), is(Role.CRATE_USER));
        assertThat(authContext.password().getChars(), is(passwd));
        authContext.close();

        // once the authContext has been closed it must not been re-used for authenticating a user
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("SecureString has already been closed");
        authContext.password().getChars();
    }
}
