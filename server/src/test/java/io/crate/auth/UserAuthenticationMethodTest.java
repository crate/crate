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

package io.crate.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.SecureHash;
import io.crate.role.metadata.RolesHelper;

public class UserAuthenticationMethodTest extends ESTestCase {

    private static class CrateOrNullRoles implements Roles {

        @Override
        public Collection<Role> roles() {
            SecureHash pwHash;
            try {
                pwHash = SecureHash.of(new SecureString("pw".toCharArray()));
            } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
                throw new RuntimeException(e);
            }
            return List.of(RolesHelper.userOf("crate", pwHash));
        }
    }

    @Test
    public void testTrustAuthentication() throws Exception {
        TrustAuthenticationMethod trustAuth = new TrustAuthenticationMethod(new CrateOrNullRoles());
        assertThat(trustAuth.name()).isEqualTo("trust");
        assertThat(trustAuth.authenticate(new Credentials("crate", null), null).name()).isEqualTo("crate");

        assertThatThrownBy(() -> trustAuth.authenticate(new Credentials("cr8", null), null))
            .hasMessage("trust authentication failed for user \"cr8\"");
    }

    @Test
    public void testAlwaysOKAuthentication() throws Exception {
        AlwaysOKAuthentication alwaysOkAuth = new AlwaysOKAuthentication(new CrateOrNullRoles());
        AuthenticationMethod alwaysOkAuthMethod = alwaysOkAuth.resolveAuthenticationType("crate", null);

        assertThat(alwaysOkAuthMethod.name()).isEqualTo("trust");
        assertThat(alwaysOkAuthMethod.authenticate(new Credentials("crate", null), null).name()).isEqualTo("crate");

        assertThatThrownBy(() -> alwaysOkAuthMethod.authenticate(new Credentials("cr8", null), null))
            .hasMessage("trust authentication failed for user \"cr8\"");
    }

    public void testPasswordAuthentication() throws Exception {
        PasswordAuthenticationMethod pwAuth = new PasswordAuthenticationMethod(new CrateOrNullRoles());
        assertThat(pwAuth.name()).isEqualTo("password");

        assertThat(pwAuth.authenticate(new Credentials("crate", "pw".toCharArray()), null).name()).isEqualTo("crate");
    }

    @Test
    public void testPasswordAuthenticationWrongPassword() throws Exception {
        PasswordAuthenticationMethod pwAuth = new PasswordAuthenticationMethod(new CrateOrNullRoles());
        assertThat(pwAuth.name()).isEqualTo("password");

        assertThatThrownBy(() -> pwAuth.authenticate(new Credentials("crate", "wrong".toCharArray()), null))
            .hasMessage("password authentication failed for user \"crate\"");

    }

    @Test
    public void testPasswordAuthenticationForNonExistingUser() throws Exception {
        PasswordAuthenticationMethod pwAuth = new PasswordAuthenticationMethod(new CrateOrNullRoles());
        assertThatThrownBy(() -> pwAuth.authenticate(new Credentials("cr8", "pw".toCharArray()), null))
            .hasMessage("password authentication failed for user \"cr8\"");
    }
}
