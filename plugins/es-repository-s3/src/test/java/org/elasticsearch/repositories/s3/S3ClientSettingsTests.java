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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;

public class S3ClientSettingsTests extends ESTestCase {

    @Test
    public void testThereIsADefaultClientByDefault() {
        final S3ClientSettings settings = S3ClientSettings.getClientSettings(Settings.EMPTY);

        assertThat(settings.credentials).isNull();
        assertThat(settings.endpoint).isEmpty();
        assertThat(settings.protocol).isEqualTo(Protocol.HTTPS);
        assertThat(settings.proxyHost).isEmpty();
        assertThat(settings.proxyPort).isEqualTo(80);
        assertThat(settings.proxyUsername).isEmpty();
        assertThat(settings.proxyPassword).isEmpty();
        assertThat(settings.readTimeoutMillis).isEqualTo(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
        assertThat(settings.maxRetries).isEqualTo(ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry());
        assertThat(settings.throttleRetries).isEqualTo(ClientConfiguration.DEFAULT_THROTTLE_RETRIES);
    }

    @Test
    public void testDefaultClientSettingsCanBeSet() {
        final S3ClientSettings settings = S3ClientSettings.getClientSettings(
            Settings.builder()
                .put("max_retries", 10)
                .build());
        assertThat(settings.maxRetries).isEqualTo(10);
    }

    @Test
    public void testRejectionOfLoneAccessKey() {
        assertThatThrownBy(
            () -> S3ClientSettings.getClientSettings(
                Settings.builder()
                    .put("access_key", "aws_key")
                    .build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Missing secret key for s3 client");
    }

    @Test
    public void testRejectionOfLoneSecretKey() {
        assertThatThrownBy(
            () -> S3ClientSettings.getClientSettings(
                Settings.builder()
                    .put("secret_key", "aws_key")
                    .build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Missing access key for s3 client");
    }

    @Test
    public void testRejectionOfLoneSessionToken() {
        assertThatThrownBy(
            () -> S3ClientSettings.getClientSettings(
                Settings.builder()
                    .put("session_token", "aws_key")
                    .build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Missing access key and secret key for s3 client");
    }

    @Test
    public void testCredentialsTypeWithAccessKeyAndSecretKey() {
        final S3ClientSettings settings = S3ClientSettings.getClientSettings(
            Settings.builder()
                .put("access_key", "access_key")
                .put("secret_key", "secret_key")
                .build());
        BasicAWSCredentials credentials = (BasicAWSCredentials) settings.credentials;
        assertThat(credentials.getAWSAccessKeyId()).isEqualTo("access_key");
        assertThat(credentials.getAWSSecretKey()).isEqualTo("secret_key");
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
        assertThat(credentials.getAWSAccessKeyId()).isEqualTo("access_key");
        assertThat(credentials.getAWSSecretKey()).isEqualTo("secret_key");
        assertThat(credentials.getSessionToken()).isEqualTo("session_token");
    }

    @Test
    public void test_hashcode_does_not_throw_NPE_with_null_credentials() {
        final S3ClientSettings settings = S3ClientSettings.getClientSettings(Settings.EMPTY);
        Set<S3ClientSettings> cache = new HashSet<>();
        cache.add(settings);
        assertThat(cache.contains(settings)).isTrue();
    }
}
