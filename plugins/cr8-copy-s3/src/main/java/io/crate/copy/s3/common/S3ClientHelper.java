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

package io.crate.copy.s3.common;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

import io.crate.common.annotations.NotThreadSafe;
import io.crate.common.annotations.VisibleForTesting;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.utils.SdkAutoCloseable;

@NotThreadSafe
public class S3ClientHelper {
    private static final String DEFAULT_PROTOCOL = "https";

    private static final ApacheHttpClient.Builder CLIENT_BUILDER = ApacheHttpClient.builder().tcpKeepAlive(true);
    private static final ClientOverrideConfiguration CLIENT_OVERRIDE_CONFIG =
        ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.builder(RetryMode.LEGACY).numRetries(5).build()).build();

    private final Map<Record, S3Client> clientMap = new HashMap<>(1);

    @VisibleForTesting
    protected S3Client initClient(@Nullable String accessKey, @Nullable String secretKey, @Nullable String endPoint,
                                  String protocolSetting) throws IOException {
        assert protocolSetting != null : "protocol setting should not be null";
        S3ClientBuilder s3ClientBuilder = S3Client.builder()
            .credentialsProvider(
                (accessKey == null || secretKey == null) ?
                    AnonymousCredentialsProvider.create() :
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .crossRegionAccessEnabled(true)
            .region(Region.AWS_GLOBAL)
            .httpClientBuilder(CLIENT_BUILDER)
            .overrideConfiguration(CLIENT_OVERRIDE_CONFIG)
            .forcePathStyle(true);
        if (endPoint != null) {
            s3ClientBuilder.endpointOverride(URI.create(protocolSetting + "://" + endPoint));
        }
        return s3ClientBuilder.build();
    }

    public S3Client client(S3URI uri) throws IOException {
        return client(uri, DEFAULT_PROTOCOL);
    }

    public S3Client client(S3URI uri, String protocolSetting) throws IOException {
        if (protocolSetting == null) {
            protocolSetting = DEFAULT_PROTOCOL;
        }
        return client(uri.accessKey(), uri.secretKey(), uri.endpoint(), protocolSetting);
    }

    private S3Client client(@Nullable String accessKey, @Nullable String secretKey, @Nullable String endPoint,
                            String protocolSetting) throws IOException {
        ClientKey clientKey = new ClientKey(accessKey, secretKey, endPoint, protocolSetting);
        S3Client client = clientMap.get(clientKey);
        if (client == null) {
            client = initClient(accessKey, secretKey, endPoint, protocolSetting);
            clientMap.put(clientKey, client);
        }
        return client;
    }

    @VisibleForTesting
    void close() {
        clientMap.values().forEach(SdkAutoCloseable::close);
    }

    private static record ClientKey(String accessKey, String secretKey, String endpoint, String protocol) {
    }
}
