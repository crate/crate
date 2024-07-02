/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import static io.crate.testing.auth.RsaKeys.PRIVATE_KEY_256;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Map;

import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;

public class CredentialsTest extends ESTestCase {

    private static RSAPrivateKey privateKey;

    @BeforeClass
    public static void prepareSigningKey() throws Exception {
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(PRIVATE_KEY_256));
        KeyFactory kf = KeyFactory.getInstance("RSA");
        privateKey = (RSAPrivateKey) kf.generatePrivate(privateKeySpec);
    }

    @Test
    @SuppressWarnings("resource")
    public void test_checks_presence_of_public_key_id() {
        String jwt = JWT.create()
            .withHeader(Map.of("typ", "JWT", "alg", "RS256"))
            .sign(Algorithm.RSA256(null, privateKey));
        assertThatThrownBy(() -> new Credentials(jwt))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The JWT token must contain a public key id (kid)");
    }

    @Test
    @SuppressWarnings("resource")
    public void test_checks_presence_of_issuer() {
        String jwt = JWT.create()
            .withHeader(Map.of("typ", "JWT", "alg", "RS256", "kid", "1"))
            .sign(Algorithm.RSA256(null, privateKey));
        assertThatThrownBy(() -> new Credentials(jwt))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The JWT token must contain an issuer (iss)");
    }

    @Test
    @SuppressWarnings("resource")
    public void test_checks_presence_of_username() {
        String jwt = JWT.create()
            .withHeader(Map.of("typ", "JWT", "alg", "RS256", "kid", "1"))
            .withIssuer("test_issuer")
            .sign(Algorithm.RSA256(null, privateKey));
        assertThatThrownBy(() -> new Credentials(jwt))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The JWT token must contain a 'username' claim");
    }
}
