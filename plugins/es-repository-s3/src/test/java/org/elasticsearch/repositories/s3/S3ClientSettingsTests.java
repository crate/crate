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

package org.elasticsearch.repositories.s3;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.text.IsEmptyString.emptyString;
import static org.junit.Assert.assertThat;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;

public class S3ClientSettingsTests extends ESTestCase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testThereIsADefaultClientByDefault() {
        final S3ClientSettings settings = S3ClientSettings.getClientSettings(Settings.EMPTY);

        assertThat(settings.credentials, nullValue());
        assertThat(settings.endpoint, is(emptyString()));
        assertThat(settings.protocol, is(Protocol.HTTPS));
        assertThat(settings.proxyHost, is(emptyString()));
        assertThat(settings.proxyPort, is(80));
        assertThat(settings.proxyUsername, is(emptyString()));
        assertThat(settings.proxyPassword, is(emptyString()));
        assertThat(settings.readTimeoutMillis, is(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT));
        assertThat(settings.maxRetries, is(ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry()));
        assertThat(settings.throttleRetries, is(ClientConfiguration.DEFAULT_THROTTLE_RETRIES));
    }

    @Test
    public void testDefaultClientSettingsCanBeSet() {
        final S3ClientSettings settings = S3ClientSettings.getClientSettings(
            Settings.builder()
                .put("max_retries", 10)
                .build());
        assertThat(settings.maxRetries, is(10));
    }

    @Test
    public void testRejectionOfLoneAccessKey() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing secret key for s3 client");
        S3ClientSettings.getClientSettings(
            Settings.builder()
                .put("access_key", "aws_key")
                .build());
    }

    @Test
    public void testRejectionOfLoneSecretKey() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing access key for s3 client");
        S3ClientSettings.getClientSettings(
            Settings.builder()
                .put("secret_key", "aws_key")
                .build());
    }

    @Test
    public void testRejectionOfLoneSessionToken() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing access key and secret key for s3 client");
        S3ClientSettings.getClientSettings(
            Settings.builder()
                .put("session_token", "aws_key")
                .build());
    }

    @Test
    public void testCredentialsTypeWithAccessKeyAndSecretKey() {
        final S3ClientSettings settings = S3ClientSettings.getClientSettings(
            Settings.builder()
                .put("access_key", "access_key")
                .put("secret_key", "secret_key")
                .build());
        BasicAWSCredentials credentials = (BasicAWSCredentials) settings.credentials;
        assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
        assertThat(credentials.getAWSSecretKey(), is("secret_key"));
    }

    @Test
    public void testCredentialsTypeWithAccessKeyAndSecretKeyAndSessionToken() {
        final S3ClientSettings settings = S3ClientSettings.getClientSettings(
            Settings.builder()
                .put("access_key", "access_key")
                .put("secret_key", "secret_key")
                .put("session_token", "session_token")
                .build());
        BasicSessionCredentials credentials = (BasicSessionCredentials) settings.credentials;
        assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
        assertThat(credentials.getAWSSecretKey(), is("secret_key"));
        assertThat(credentials.getSessionToken(), is("session_token"));
    }
}
