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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import io.crate.exceptions.InvalidArgumentException;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class AwsS3ServiceImplTests extends ESTestCase {

    private S3Service service;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void beforeTest() {
        service = new S3Service();
    }

    public void afterTest() {
        service.close();
        service = null;
    }

    @Test
    public void testGetClientForSameSettingsReturnsCachedClient() {
        RepositoryMetadata metadata = new RepositoryMetadata("", "", Settings.builder()
            .put("access_key", "access_key")
            .put("secret_key", "secret_key")
            .build());

        var clientRef = service.client(metadata);
        assertThat(clientRef.refCount(), is(2));
        var newClientRef = service.client(metadata);
        assertThat(newClientRef.refCount(), is(3));

        assertThat(clientRef.client(), is(newClientRef.client()));

        clientRef.client().shutdown();
        newClientRef.client().shutdown();
    }

    @Test
    public void testGetClientForUpdatedSettingsReturnsNewClient() {
        Settings settings = Settings.builder()
            .put("access_key", "access_key")
            .put("secret_key", "secret_key")
            .build();

        Settings newSettings = Settings.builder()
            .put("access_key", "access_key")
            .put("secret_key", "new_secret_key")
            .build();

        RepositoryMetadata metadata = new RepositoryMetadata("", "", settings);
        RepositoryMetadata newMetadata = new RepositoryMetadata("", "", newSettings);

        AmazonS3Reference clientRef = service.client(metadata);
        assertThat(clientRef.refCount(), is(2));
        AmazonS3Reference newClientRef = service.client(newMetadata);
        assertThat(newClientRef.refCount(), is(2));

        assertThat(clientRef.client(), is(not(newClientRef.client())));

        clientRef.client().shutdown();
        newClientRef.client().shutdown();
    }

    @Test
    public void testSetDefaultCredential() {
        final String awsAccessKey = randomAlphaOfLength(8);
        final String awsSecretKey = randomAlphaOfLength(8);
        final Settings settings = Settings.builder()
            .put("access_key", awsAccessKey)
            .put("secret_key", awsSecretKey).build();
        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings);
        // test default exists and is an Instance provider
        final AWSCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(logger, clientSettings);
        assertThat(defaultCredentialsProvider, instanceOf(AWSStaticCredentialsProvider.class));
        assertThat(defaultCredentialsProvider.getCredentials().getAWSAccessKeyId(), is(awsAccessKey));
        assertThat(defaultCredentialsProvider.getCredentials().getAWSSecretKey(), is(awsSecretKey));
    }

    @Test
    public void test_no_credentials_are_not_provided() {
        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(Settings.builder().build());
        expectedException.expect(InvalidArgumentException.class);
        expectedException.expectMessage(containsString("Cannot find required credentials to create a repository of type s3"));
        S3Service.buildCredentials(logger, clientSettings);
    }

    @Test
    public void testAWSDefaultConfiguration() {
        launchAWSConfigurationTest(
            Settings.EMPTY,
            Protocol.HTTPS,
            null,
            -1,
            null,
            null,
            3,
            ClientConfiguration.DEFAULT_THROTTLE_RETRIES,
            ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
    }

    @Test
    public void testAWSConfigurationWithAwsSettings() {
        final Settings settings = Settings.builder()
            .put("proxy_username", "aws_proxy_username")
            .put("proxy_password", "aws_proxy_password")
            .put("protocol", "http")
            .put("proxy_host", "aws_proxy_host")
            .put("proxy_port", 8080)
            .put("read_timeout", "10s")
            .build();
        launchAWSConfigurationTest(
            settings, Protocol.HTTP, "aws_proxy_host", 8080, "aws_proxy_username",
            "aws_proxy_password", 3, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, 10000);
    }

    @Test
    public void testRepositoryMaxRetries() {
        final Settings settings = Settings.builder()
            .put("max_retries", 5)
            .build();
        launchAWSConfigurationTest(
            settings, Protocol.HTTPS, null, -1, null,
            null, 5, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, 50000);
    }

    @Test
    public void testRepositoryThrottleRetries() {
        final boolean throttling = randomBoolean();

        final Settings settings = Settings.builder().put("use_throttle_retries", throttling).build();
        launchAWSConfigurationTest(settings, Protocol.HTTPS, null, -1, null, null, 3, throttling, 50000);
    }

    private void launchAWSConfigurationTest(Settings settings,
                                            Protocol expectedProtocol,
                                            String expectedProxyHost,
                                            int expectedProxyPort,
                                            String expectedProxyUsername,
                                            String expectedProxyPassword,
                                            Integer expectedMaxRetries,
                                            boolean expectedUseThrottleRetries,
                                            int expectedReadTimeout) {

        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings);
        final ClientConfiguration configuration = S3Service.buildConfiguration(clientSettings);

        assertThat(configuration.getResponseMetadataCacheSize(), is(0));
        assertThat(configuration.getProtocol(), is(expectedProtocol));
        assertThat(configuration.getProxyHost(), is(expectedProxyHost));
        assertThat(configuration.getProxyPort(), is(expectedProxyPort));
        assertThat(configuration.getProxyUsername(), is(expectedProxyUsername));
        assertThat(configuration.getProxyPassword(), is(expectedProxyPassword));
        assertThat(configuration.getMaxErrorRetry(), is(expectedMaxRetries));
        assertThat(configuration.useThrottledRetries(), is(expectedUseThrottleRetries));
        assertThat(configuration.getSocketTimeout(), is(expectedReadTimeout));
    }
}
