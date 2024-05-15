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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.carrotsearch.randomizedtesting.annotations.Repeat;

import io.crate.exceptions.InvalidArgumentException;

public class AwsS3ServiceImplTests extends ESTestCase {

    private S3Service service;
    private ExecutorService executor;

    @Before
    public void beforeTest() {
        service = new S3Service();
        executor = Executors.newFixedThreadPool(100);
    }

    @After
    public void afterTest() throws Exception {
        executor.shutdown();
        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
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
        assertThat(clientRef.refCount()).isEqualTo(2);
        var newClientRef = service.client(metadata);
        assertThat(newClientRef.refCount()).isEqualTo(3);

        assertThat(clientRef.client()).isEqualTo(newClientRef.client());

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
        assertThat(clientRef.refCount()).isEqualTo(2);
        AmazonS3Reference newClientRef = service.client(newMetadata);
        assertThat(newClientRef.refCount()).isEqualTo(2);

        assertThat(clientRef.client()).isNotEqualTo(newClientRef.client());

        clientRef.client().shutdown();
        newClientRef.client().shutdown();
    }

    /**
     * Tests a regression where wrong clients were returned from a local cache.
     * Repeat it in order to catch at least one failure.
     */
    @Repeat(iterations = 30)
    @Test
    public void test_concurrent_repro_access_does_not_return_wrong_client() throws Exception {
        Settings settings1 = Settings.builder()
                .put("access_key", "repo1_access_key")
                .put("secret_key", "repo1_secret_key")
                .build();

        Settings settings2 = Settings.builder()
                .put("access_key", "repo2_access_key")
                .put("secret_key", "repo2_secret_key")
                .build();

        RepositoryMetadata metadata1 = new RepositoryMetadata("repo1", "", settings1);
        RepositoryMetadata metadata2 = new RepositoryMetadata("repo2", "", settings2);

        // high number of threads are required to let if fail reliably on the old buggy code
        int numThreads = 100;
        final CyclicBarrier barrier = new CyclicBarrier(1 + numThreads);
        AtomicReference<AmazonS3> clientRef1 = new AtomicReference<>();
        AtomicReference<AmazonS3> clientRef2 = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            if (i % 2 == 0) {
                executor.submit(() -> {
                    try {
                        barrier.await();
                    } catch (final BrokenBarrierException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    var clientRef = service.client(metadata1);
                    clientRef1.set(clientRef.client());
                    latch.countDown();
                    clientRef.close();
                });
            } else {
                executor.submit(() -> {
                    try {
                        barrier.await();
                    } catch (final BrokenBarrierException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    var clientRef = service.client(metadata2);
                    clientRef2.set(clientRef.client());
                    latch.countDown();
                    clientRef.close();
                });
            }

        }
        barrier.await();
        latch.await();

        assertThat(clientRef1.get()).isNotEqualTo(clientRef2.get());
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
        assertThat(defaultCredentialsProvider).isExactlyInstanceOf(AWSStaticCredentialsProvider.class);
        assertThat(defaultCredentialsProvider.getCredentials().getAWSAccessKeyId()).isEqualTo(awsAccessKey);
        assertThat(defaultCredentialsProvider.getCredentials().getAWSSecretKey()).isEqualTo(awsSecretKey);
    }

    @Test
    public void test_no_credentials_are_not_provided() {
        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(Settings.builder().build());
        assertThatThrownBy(() -> S3Service.buildCredentials(logger, clientSettings))
            .isExactlyInstanceOf(InvalidArgumentException.class)
            .hasMessageContaining("Cannot find required credentials to create a repository of type s3");

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

        assertThat(configuration.getResponseMetadataCacheSize()).isEqualTo(0);
        assertThat(configuration.getProtocol()).isEqualTo(expectedProtocol);
        assertThat(configuration.getProxyHost()).isEqualTo(expectedProxyHost);
        assertThat(configuration.getProxyPort()).isEqualTo(expectedProxyPort);
        assertThat(configuration.getProxyUsername()).isEqualTo(expectedProxyUsername);
        assertThat(configuration.getProxyPassword()).isEqualTo(expectedProxyPassword);
        assertThat(configuration.getMaxErrorRetry()).isEqualTo(expectedMaxRetries);
        assertThat(configuration.useThrottledRetries()).isEqualTo(expectedUseThrottleRetries);
        assertThat(configuration.getSocketTimeout()).isEqualTo(expectedReadTimeout);
    }
}
