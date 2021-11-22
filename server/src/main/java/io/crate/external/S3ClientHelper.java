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

package io.crate.external;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.URI;

@NotThreadSafe
public class S3ClientHelper {

    private static final AWSCredentialsProvider DEFAULT_CREDENTIALS_PROVIDER_CHAIN =
        new AWSCredentialsProviderChain(
            new EnvironmentVariableCredentialsProvider(),
            new SystemPropertiesCredentialsProvider(),
            new InstanceProfileCredentialsProvider()) {

            private AWSCredentials ANONYMOUS_CREDENTIALS = new AnonymousAWSCredentials();

            public AWSCredentials getCredentials() {
                try {
                    return super.getCredentials();
                } catch (AmazonClientException ace) {
                    // allow for anonymous access
                    return ANONYMOUS_CREDENTIALS;
                }
            }
        };

    private static final ClientConfiguration CLIENT_CONFIGURATION = new ClientConfiguration().withProtocol(Protocol.HTTPS);

    static {
        CLIENT_CONFIGURATION.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(5));
        CLIENT_CONFIGURATION.setUseTcpKeepAlive(true);
    }

    private static final String INVALID_URI_MSG = "Invalid URI. Please make sure that given URI is encoded properly.";

    private final IntObjectMap<AmazonS3> clientMap = new IntObjectHashMap<>(1);

    protected AmazonS3 initClient(@Nullable String accessKey, @Nullable String secretKey, @Nullable String endPoint,
                                  @Nullable String protocolSetting) throws IOException {
        AmazonS3 client;
        if (endPoint == null) {
            if (accessKey == null || secretKey == null) {
                return new AmazonS3Client(DEFAULT_CREDENTIALS_PROVIDER_CHAIN, CLIENT_CONFIGURATION);
            } else {
                return new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), CLIENT_CONFIGURATION);
            }
        } else {
            if (protocolSetting == null) {
                protocolSetting = Protocol.HTTPS.toString();
            }
            client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(protocolSetting + "://" + endPoint, null))
                //.withPathStyleAccessEnabled(true)
                .withClientConfiguration(CLIENT_CONFIGURATION) // does not override protocolSetting passed to EndpointConfiguration
                .withCredentials(
                    (accessKey == null || secretKey == null) ?
                        DEFAULT_CREDENTIALS_PROVIDER_CHAIN :
                        new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();
        }
        return client;
    }

    public AmazonS3 client(URI uri, @Nullable String protocolSetting) throws IOException {
        String accessKey = null;
        String secretKey = null;
        String userInfo = null;
        if (uri.getHost() == null && uri.getPort() == -1 && uri.getUserInfo() == null) {
            // userInfo is provided by the user and is contained in authority but NOT in userInfo
            // this happens when host is not provided
            if (uri.getAuthority() != null) {
                int idx = uri.getAuthority().indexOf('@');
                if (idx != -1) {
                    userInfo = uri.getAuthority().substring(0, idx);
                }
            }
        } else {
            userInfo = uri.getUserInfo();
        }
        if (userInfo != null) {
            String[] userInfoParts = userInfo.split(":");
            try {
                accessKey = userInfoParts[0];
                secretKey = userInfoParts[1];
            } catch (ArrayIndexOutOfBoundsException e) {
                // ignore
            }
            // if the URI contains '@' and ':', a UserInfo is in fact given, but could not
            // be parsed properly because the URI is not valid (e.g. not properly encoded).
        } else if (uri.toString().contains("@") && uri.toString().contains(":")) {
            throw new IllegalArgumentException(INVALID_URI_MSG);
        }

        String endPoint = null;
        if (uri.getHost() != null) {
            endPoint = uri.getHost() + ":" + uri.getPort();
        }

        return client(accessKey, secretKey, endPoint, protocolSetting);
    }

    private AmazonS3 client(@Nullable String accessKey, @Nullable String secretKey, @Nullable String endPoint,
                            @Nullable String protocolSetting) throws IOException {
        int hash = hash(accessKey, secretKey, endPoint, protocolSetting);
        AmazonS3 client = clientMap.get(hash);
        if (client == null) {
            client = initClient(accessKey, secretKey, endPoint, protocolSetting);
            clientMap.put(hash, client);
        }
        return client;
    }

    private static int hash(@Nullable String accessKey, @Nullable String secretKey, @Nullable String endPoint,
                            @Nullable String protocolSetting) {
        return 31 * (31 * (31 * (accessKey == null ? 1 : accessKey.hashCode())
            + (secretKey == null ? 1 : secretKey.hashCode()))
            + (endPoint == null ? 1 : endPoint.hashCode()))
            + (protocolSetting == null ? 1 : protocolSetting.hashCode());
    }

    public static class PathParser {
        public final String bucket;
        public final String key;

        public PathParser(String pathComponent) {
            try {
                int splitIndex = pathComponent.indexOf('/', 1);
                this.bucket = pathComponent.substring(1, splitIndex);
                this.key = pathComponent.substring(splitIndex + 1);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException(INVALID_URI_MSG);
            }
        }
    }
}
