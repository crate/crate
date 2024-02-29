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

package io.crate.protocols.http;

import static io.crate.protocols.http.Headers.extractCredentialsFromHttpAuthHeader;
import static io.crate.protocols.http.Headers.isAcceptJson;
import static io.crate.protocols.http.Headers.isBrowser;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.auth.Credentials;

public class HeadersTest {

    @Test
    public void testIsBrowser() {
        assertThat(isBrowser(null)).isFalse();
        assertThat(isBrowser("some header")).isFalse();
        assertThat(isBrowser("Mozilla/5.0 (X11; Linux x86_64)")).isTrue();
    }

    @Test
    public void testIsAcceptJson() {
        assertThat(isAcceptJson(null)).isFalse();
        assertThat(isAcceptJson("text/html")).isFalse();
        assertThat(isAcceptJson("application/json")).isTrue();
    }

    @Test
    public void testExtractUsernamePasswordFromHttpBasicAuthHeader() {
        Credentials creds = extractCredentialsFromHttpAuthHeader("");
        assertThat(creds.username()).isNull();
        assertThat(creds.password()).isNull();

        creds = extractCredentialsFromHttpAuthHeader(null);
        assertThat(creds.username()).isNull();
        assertThat(creds.password()).isNull();

        creds = extractCredentialsFromHttpAuthHeader("Basic QXJ0aHVyOkV4Y2FsaWJ1cg==");
        assertThat(creds.username()).isEqualTo("Arthur");
        assertThat(creds.password()).hasToString("Excalibur");

        creds = extractCredentialsFromHttpAuthHeader("Basic QXJ0aHVyOjp0ZXN0OnBhc3N3b3JkOg==");
        assertThat(creds.username()).isEqualTo("Arthur");
        assertThat(creds.password()).hasToString(":test:password:");

        creds = extractCredentialsFromHttpAuthHeader("Basic QXJ0aHVyOg==");
        assertThat(creds.username()).isEqualTo("Arthur");
        assertThat(creds.password()).isNull();

        creds = extractCredentialsFromHttpAuthHeader("Basic OnBhc3N3b3Jk");
        assertThat(creds.username()).isEmpty();
        assertThat(creds.password()).hasToString("password");
        creds.close();
    }

    @Test
    @SuppressWarnings("resource")
    public void test_unsupported_schema_throws_an_error() {
        assertThatThrownBy(
            () -> extractCredentialsFromHttpAuthHeader("Dummy QXJ0aHVyOg=="))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Only basic or bearer HTTP Authentication schemas are allowed.");
    }
}
