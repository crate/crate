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

import static io.crate.protocols.http.Headers.extractCredentialsFromHttpBasicAuthHeader;
import static io.crate.protocols.http.Headers.isAcceptJson;
import static io.crate.protocols.http.Headers.isBrowser;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import io.crate.auth.Credentials;

public class HeadersTest {

    @Test
    public void testIsBrowser() {
        assertThat(isBrowser(null), is(false));
        assertThat(isBrowser("some header"), is(false));
        assertThat(isBrowser("Mozilla/5.0 (X11; Linux x86_64)"), is(true));
    }

    @Test
    public void testIsAcceptJson() {
        assertThat(isAcceptJson(null), is(false));
        assertThat(isAcceptJson("text/html"), is(false));
        assertThat(isAcceptJson("application/json"), is(true));
    }

    @Test
    public void testExtractUsernamePasswordFromHttpBasicAuthHeader() {
        Credentials creds = extractCredentialsFromHttpBasicAuthHeader("");
        assertThat(creds.username(), is(""));
        assertThat(creds.passwordOrToken().toString(), is(""));

        creds = extractCredentialsFromHttpBasicAuthHeader(null);
        assertThat(creds.username(), is(""));
        assertThat(creds.passwordOrToken().toString(), is(""));

        creds = extractCredentialsFromHttpBasicAuthHeader("Basic QXJ0aHVyOkV4Y2FsaWJ1cg==");
        assertThat(creds.username(), is("Arthur"));
        assertThat(creds.passwordOrToken().toString(), is("Excalibur"));

        creds = extractCredentialsFromHttpBasicAuthHeader("Basic QXJ0aHVyOjp0ZXN0OnBhc3N3b3JkOg==");
        assertThat(creds.username(), is("Arthur"));
        assertThat(creds.passwordOrToken().toString(), is(":test:password:"));

        creds = extractCredentialsFromHttpBasicAuthHeader("Basic QXJ0aHVyOg==");
        assertThat(creds.username(), is("Arthur"));
        assertThat(creds.passwordOrToken().toString(), is(""));

        creds = extractCredentialsFromHttpBasicAuthHeader("Basic OnBhc3N3b3Jk");
        assertThat(creds.username(), is(""));
        assertThat(creds.passwordOrToken().toString(), is("password"));
    }
}
