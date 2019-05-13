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

package org.elasticsearch.repositories.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.nullValue;

public class S3ClientSettingsTests extends ESTestCase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testThereIsADefaultClientByDefault() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(Settings.EMPTY);
        assertThat(settings.keySet(), contains("default"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.credentials, nullValue());
        assertThat(defaultSettings.endpoint, isEmptyString());
        assertThat(defaultSettings.protocol, is(Protocol.HTTPS));
        assertThat(defaultSettings.proxyHost, isEmptyString());
        assertThat(defaultSettings.proxyPort, is(80));
        assertThat(defaultSettings.proxyUsername, isEmptyString());
        assertThat(defaultSettings.proxyPassword, isEmptyString());
        assertThat(defaultSettings.readTimeoutMillis, is(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT));
        assertThat(defaultSettings.maxRetries, is(ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry()));
        assertThat(defaultSettings.throttleRetries, is(ClientConfiguration.DEFAULT_THROTTLE_RETRIES));
    }

    @Test
    public void testDefaultClientSettingsCanBeSet() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder()
                .put("s3.client.default.max_retries", 10)
                .build());
        assertThat(settings.keySet(), contains("default"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.maxRetries, is(10));
    }

    @Test
    public void testRejectionOfLoneAccessKey() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing secret key for s3 client [default]");
        S3ClientSettings.load(Settings.builder()
                                  .put("s3.client.default.access_key", "aws_key")
                                  .build());
    }

    @Test
    public void testRejectionOfLoneSecretKey() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing access key for s3 client [default]");
        S3ClientSettings.load(Settings.builder()
                                  .put("s3.client.default.secret_key", "aws_key")
                                  .build());
    }

    @Test
    public void testRejectionOfLoneSessionToken() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing access key and secret key for s3 client [default]");
        S3ClientSettings.load(Settings.builder()
                                  .put("s3.client.default.session_token", "aws_key")
                                  .build());
    }

    @Test
    public void testCredentialsTypeWithAccessKeyAndSecretKey() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder()
                .put("s3.client.default.access_key", "access_key")
                .put("s3.client.default.secret_key", "secret_key")
                .build());
        final S3ClientSettings defaultSettings = settings.get("default");
        BasicAWSCredentials credentials = (BasicAWSCredentials) defaultSettings.credentials;
        assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
        assertThat(credentials.getAWSSecretKey(), is("secret_key"));
    }

    @Test
    public void testCredentialsTypeWithAccessKeyAndSecretKeyAndSessionToken() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder()
                .put("s3.client.default.access_key", "access_key")
                .put("s3.client.default.secret_key", "secret_key")
                .put("s3.client.default.session_token", "session_token")
                .build());
        final S3ClientSettings defaultSettings = settings.get("default");
        BasicSessionCredentials credentials = (BasicSessionCredentials) defaultSettings.credentials;
        assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
        assertThat(credentials.getAWSSecretKey(), is("secret_key"));
        assertThat(credentials.getSessionToken(), is("session_token"));
    }
}
