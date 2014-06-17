/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.URI;

@NotThreadSafe
public class S3ClientHelper {

    private final String hostname;
    private final static AWSCredentialsProvider DEFAULT_CREDENTIALS_PROVIDER_CHAIN =
            new DefaultAWSCredentialsProviderChain();

    // TODO: use HTTPS and fix certificate issue
    private final static ClientConfiguration CLIENT_CONFIGURATION = new ClientConfiguration().withProtocol(Protocol.HTTP);
    private final static String INVALID_URI_MSG = "Invalid URI. Please make sure that given URI is encoded properly.";

    public S3ClientHelper(String hostname) {
        this.hostname = hostname;
    }

    private final IntObjectMap<AmazonS3> clientMap = new IntObjectOpenHashMap<>(1);

    protected AmazonS3 initClient(String accessKey, String secretKey) throws IOException {
        if (accessKey == null || secretKey == null) {
            return new AmazonS3Client(DEFAULT_CREDENTIALS_PROVIDER_CHAIN, CLIENT_CONFIGURATION);
        }
        AmazonS3Client client = new AmazonS3Client(
                new BasicAWSCredentials(accessKey, secretKey),
                CLIENT_CONFIGURATION
        );
        client.setEndpoint(hostname);
        return client;
    }

    public AmazonS3 client(URI uri) throws IOException {
        String accessKey = null;
        String secretKey = null;
        if (uri.getHost() == null) {
            throw new IllegalArgumentException(INVALID_URI_MSG);
        }
        if (uri.getUserInfo() != null) {
            String[] userInfoParts = uri.getUserInfo().split(":");
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
        return client(accessKey, secretKey);
    }

    private AmazonS3 client(String accessKey, String secretKey) throws IOException {
        int hash = hash(accessKey, secretKey);
        AmazonS3 client = clientMap.get(hash);
        if (client == null) {
            client = initClient(accessKey, secretKey);
            clientMap.put(hash, client);
        }
        return client;
    }

    private static int hash(String accessKey, String secretKey) {
        return 31 * (accessKey == null ? 1 : accessKey.hashCode()) + (secretKey == null ? 1 : secretKey.hashCode());
    }
}
