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

package org.elasticsearch.repositories.s3;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;

import io.crate.exceptions.InvalidArgumentException;

class S3Service implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(S3Service.class);

    private volatile Map<S3ClientSettings, AmazonS3Reference> clientsCache = new HashMap<>();

    /**
     * Attempts to retrieve a client by name from the cache.
     * If the client does not exist it will be created.
     *
     * @param metadata {@link RepositoryMetadata}
     */
    public AmazonS3Reference client(RepositoryMetadata metadata) {
        final S3ClientSettings clientSettings = S3ClientSettings
            .getClientSettings(metadata.settings());

        var client = clientsCache.get(clientSettings);
        if (client != null && client.tryIncRef()) {
            return client;
        }

        synchronized (this) {
            var existing = clientsCache.get(clientSettings);
            if (existing != null && existing.tryIncRef()) {
                return existing;
            }
            final AmazonS3Reference newClientRef = new AmazonS3Reference(buildClient(clientSettings));
            newClientRef.incRef();
            clientsCache.put(clientSettings, newClientRef);
            return newClientRef;
        }
    }

    private AmazonS3 buildClient(final S3ClientSettings clientSettings) {
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(buildCredentials(LOGGER, clientSettings));
        builder.withClientConfiguration(buildConfiguration(clientSettings));

        final String endpoint = Strings.hasLength(clientSettings.endpoint)
            ? clientSettings.endpoint
            : Constants.S3_HOSTNAME;
        LOGGER.debug("using endpoint [{}]", endpoint);

        // If the endpoint configuration isn't set on the builder then the default behaviour is to try
        // and work out what region we are in and use an appropriate endpoint - see AwsClientBuilder#setRegion.
        // In contrast, directly-constructed clients use s3.amazonaws.com unless otherwise instructed. We currently
        // use a directly-constructed client, and need to keep the existing behaviour to avoid a breaking change,
        // so to move to using the builder we must set it explicitly to keep the existing behaviour.
        //
        // We do this because directly constructing the client is deprecated (was already deprecated in 1.1.223 too)
        // so this change removes that usage of a deprecated API.
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, null));
        if (clientSettings.pathStyleAccess) {
            builder.enablePathStyleAccess();
        }

        return builder.build();
    }

    // pkg private for tests
    static ClientConfiguration buildConfiguration(S3ClientSettings clientSettings) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setProtocol(clientSettings.protocol);

        if (Strings.hasText(clientSettings.proxyHost)) {
            // TODO: remove this leniency, these settings should exist together and be validated
            clientConfiguration.setProxyHost(clientSettings.proxyHost);
            clientConfiguration.setProxyPort(clientSettings.proxyPort);
            clientConfiguration.setProxyUsername(clientSettings.proxyUsername);
            clientConfiguration.setProxyPassword(clientSettings.proxyPassword);
        }

        clientConfiguration.setMaxErrorRetry(clientSettings.maxRetries);
        clientConfiguration.setUseThrottleRetries(clientSettings.throttleRetries);
        clientConfiguration.setSocketTimeout(clientSettings.readTimeoutMillis);

        return clientConfiguration;
    }

    // pkg private for tests
    static AWSCredentialsProvider buildCredentials(Logger logger, S3ClientSettings clientSettings) {
        final AWSCredentials credentials = clientSettings.credentials;
        if (credentials == null) {
            logger.debug("Using instance profile credentials");
            var ec2ContainerCredentialsProviderWrapper = new EC2ContainerCredentialsProviderWrapper();
            try {
                // Check if credentials are available
                ec2ContainerCredentialsProviderWrapper.getCredentials();
                return ec2ContainerCredentialsProviderWrapper;
            } catch (SdkClientException e) {
                throw new InvalidArgumentException(
                    "Cannot find required credentials to create a repository of type s3. " +
                    "Credentials must be provided either as repository options access_key and secret_key or AWS IAM roles."
                    );
            }
        } else {
            logger.debug("Using basic key/secret credentials");
            return new AWSStaticCredentialsProvider(credentials);
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            // the clients will shutdown when they will not be used anymore
            for (final AmazonS3Reference clientReference : clientsCache.values()) {
                clientReference.decRef();
            }
            clientsCache = new HashMap<>();
            // shutdown IdleConnectionReaper background thread
            // it will be restarted on new client usage
            IdleConnectionReaper.shutdown();
        }
    }
}
