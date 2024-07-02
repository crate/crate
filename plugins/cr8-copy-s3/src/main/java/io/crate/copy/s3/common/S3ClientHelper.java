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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.jetbrains.annotations.VisibleForTesting;

import org.jetbrains.annotations.Nullable;
import io.crate.common.annotations.NotThreadSafe;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@NotThreadSafe
public class S3ClientHelper {

    private static final Protocol DEFAULT_PROTOCOL = Protocol.HTTPS;

    private static final ClientConfiguration CLIENT_CONFIGURATION = new ClientConfiguration().withProtocol(DEFAULT_PROTOCOL);

    static {
        CLIENT_CONFIGURATION.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(5));
        CLIENT_CONFIGURATION.setUseTcpKeepAlive(true);
    }

    private final Map<Record, AmazonS3> clientMap = new HashMap<>(1);

    @VisibleForTesting
    protected AmazonS3 initClient(@Nullable String accessKey, @Nullable String secretKey, @Nullable String endPoint,
                                  String protocolSetting) throws IOException {
        assert protocolSetting != null : "protocol setting should not be null";
        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(
                (accessKey == null || secretKey == null) ? new AnonymousAWSCredentials() : new BasicAWSCredentials(accessKey, secretKey)))
            .withForceGlobalBucketAccessEnabled(true)
            .withClientConfiguration(CLIENT_CONFIGURATION) // does not override protocolSetting passed to EndpointConfiguration
            .withPathStyleAccessEnabled(true);
        if (endPoint != null) {
            amazonS3ClientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(protocolSetting + "://" + endPoint, null));
        } else {
            amazonS3ClientBuilder.withRegion(Regions.DEFAULT_REGION);
        }
        return amazonS3ClientBuilder.build();
    }

    public AmazonS3 client(S3URI uri) throws IOException {
        return client(uri, DEFAULT_PROTOCOL.toString());
    }

    public AmazonS3 client(S3URI uri, String protocolSetting) throws IOException {
        if (protocolSetting == null) {
            protocolSetting = DEFAULT_PROTOCOL.toString();
        }
        return client(uri.accessKey(), uri.secretKey(), uri.endpoint(), protocolSetting);
    }

    private AmazonS3 client(@Nullable String accessKey, @Nullable String secretKey, @Nullable String endPoint,
                            String protocolSetting) throws IOException {
        ClientKey clientKey = new ClientKey(accessKey, secretKey, endPoint, protocolSetting);
        AmazonS3 client = clientMap.get(clientKey);
        if (client == null) {
            client = initClient(accessKey, secretKey, endPoint, protocolSetting);
            clientMap.put(clientKey, client);
        }
        return client;
    }

    private static record ClientKey(String accessKey, String secretKey, String endpoint, String protocol) {
    }
}
