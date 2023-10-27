/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery.ec2;

import java.net.URI;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

class AwsEc2ServiceImpl implements AwsEc2Service {

    private static final Logger LOGGER = LogManager.getLogger(AwsEc2ServiceImpl.class);

    public static final String EC2_METADATA_URL = "http://169.254.169.254/latest/meta-data/";

    private final AtomicReference<LazyInitializable<AmazonEc2Reference, ElasticsearchException>> lazyClientReference =
            new AtomicReference<>();


    private Ec2Client buildClient(Ec2ClientSettings clientSettings) {
        ProxyConfiguration.Builder proxyConfig = ProxyConfiguration.builder();
        if (Strings.hasText(clientSettings.proxyHost)) {
            // TODO: remove this leniency, these settings should exist together and be validated
            proxyConfig
                .endpoint(URI.create(clientSettings.protocol + "://" + clientSettings.proxyHost + ":" + clientSettings.proxyPort))
                .username(clientSettings.proxyUsername)
                .password(clientSettings.proxyPassword);
        }

        // NOT supported by AWS SDK V2
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        // clientConfiguration.setResponseMetadataCacheSize(0);

        ClientOverrideConfiguration.Builder overrideConfig = ClientOverrideConfiguration.builder();
        BackoffStrategy backoffStrategy = (retryPolicyContext) -> {
            // with 10 retries the max delay time is 320s/320000ms (10 * 2^5 * 1 * 1000)
            LOGGER.warn("EC2 API request failed, retry again. Reason was:", retryPolicyContext.exception());
            final Random rand = Randomness.get();
            return Duration.ofMillis((long)(1000L * (10d * Math.pow(2, retryPolicyContext.retriesAttempted()) / 2.0d) * (1.0d + rand.nextDouble())));
        };
        overrideConfig.retryPolicy(
            RetryPolicy.builder()
                .backoffStrategy(backoffStrategy)
                .retryCondition(RetryCondition.none())
                .numRetries(10)
                .build());

        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
            .socketTimeout(Duration.ofMillis(clientSettings.readTimeoutMillis))
            .proxyConfiguration(proxyConfig.build());

        var clientBuilder = Ec2Client.builder()
            .credentialsProvider(buildCredentials(clientSettings))
            .httpClientBuilder(httpClientBuilder)
            .overrideConfiguration(overrideConfig.build());

        if (Strings.hasText(clientSettings.endpoint)) {
            LOGGER.debug("using explicit ec2 endpoint [{}]", clientSettings.endpoint);
            clientBuilder.endpointOverride(URI.create(clientSettings.protocol + "://" + clientSettings.endpoint));
        } else {
            LOGGER.debug("using ec2 region [{}]", Region.AWS_GLOBAL);
            Ec2Client.builder().region(Region.AWS_GLOBAL);
        }
        return clientBuilder.build();
    }

    private static AwsCredentialsProvider buildCredentials(Ec2ClientSettings clientSettings) {
        final AwsCredentials credentials = clientSettings.credentials;
        if (credentials == null) {
            AwsEc2ServiceImpl.LOGGER.debug("Using either environment variables, system properties or instance profile credentials");
            return DefaultCredentialsProvider.builder().build();
        } else {
            AwsEc2ServiceImpl.LOGGER.debug("Using basic key/secret credentials");
            return StaticCredentialsProvider.create(credentials);
        }
    }

    @Override
    public AmazonEc2Reference client() {
        final LazyInitializable<AmazonEc2Reference, ElasticsearchException> clientReference = this.lazyClientReference.get();
        if (clientReference == null) {
            throw new IllegalStateException("Missing ec2 client configs");
        }
        return clientReference.getOrCompute();
    }

    /**
     * Refreshes the settings for the AmazonEC2 client. The new client will be build
     * using these new settings. The old client is usable until released. On release it
     * will be destroyed instead of being returned to the cache.
     */
    @Override
    public void refreshAndClearCache(Ec2ClientSettings clientSettings) {
        final LazyInitializable<AmazonEc2Reference, ElasticsearchException> newClient = new LazyInitializable<>(
            () -> new AmazonEc2Reference(buildClient(clientSettings)),
            AbstractRefCounted::incRef,
            AbstractRefCounted::decRef
        );
        final LazyInitializable<AmazonEc2Reference, ElasticsearchException> oldClient = this.lazyClientReference.getAndSet(newClient);
        if (oldClient != null) {
            oldClient.reset();
        }
    }

    @Override
    public void close() {
        final LazyInitializable<AmazonEc2Reference, ElasticsearchException> clientReference = this.lazyClientReference.getAndSet(null);
        if (clientReference != null) {
            clientReference.reset();
        }
    }

}
