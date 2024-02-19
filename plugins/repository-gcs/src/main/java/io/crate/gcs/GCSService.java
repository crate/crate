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


package io.crate.gcs;

import java.net.URI;
import java.util.HashMap;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import io.crate.common.unit.TimeValue;

/**
 * Based on https://github.com/opensearch-project/OpenSearch/blob/main/plugins/repository-gcs/src/main/java/org/opensearch/repositories/gcs/GoogleCloudStorageService.java
 */
public class GCSService {

    private volatile HashMap<String, Storage> clientCache = new HashMap<>();

    public Storage client(RepositoryMetadata metadata) {
        final Storage storage = clientCache.get(metadata.name());
        if (storage != null) {
            return storage;
        }

        synchronized (this) {
            final Storage existing = clientCache.get(metadata.name());

            if (existing != null) {
                return existing;
            }

            Storage newClient = createClient(metadata.settings());
            clientCache.put(metadata.name(), newClient);
            return newClient;
        }
    }

    synchronized void closeRepositoryClient(String repositoryName) {
        clientCache.remove(repositoryName);
    }

    /**
     * Creates a client that can be used to manage Google Cloud Storage objects. The client is thread-safe.
     *
     * @param settings client settings to use, including secure settings
     * @return a new client storage instance that can be used to manage objects
     * (blobs)
     */
    private Storage createClient(Settings settings) {
        GCSClientSettings clientSettings = GCSClientSettings.fromSettings(settings);
        HttpTransport httpTransport = new NetHttpTransport.Builder().build();

        HttpTransportOptions httpTransportOptions = new HttpTransportOptions(
            HttpTransportOptions.newBuilder()
                .setConnectTimeout(toTimeout(clientSettings.connectTimeout()))
                .setReadTimeout(toTimeout(clientSettings.readTimeout()))
                .setHttpTransportFactory(() -> httpTransport)
        );
        StorageOptions storageOptions = createStorageOptions(clientSettings, httpTransportOptions);
        return storageOptions.getService();
    }

    StorageOptions createStorageOptions(GCSClientSettings clientSettings,
                                        HttpTransportOptions httpTransportOptions) {
        StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder()
            .setTransportOptions(httpTransportOptions);
        if (Strings.hasLength(clientSettings.endpoint())) {
            storageOptionsBuilder.setHost(clientSettings.endpoint());
        }
        if (Strings.hasLength(clientSettings.projectId())) {
            storageOptionsBuilder.setProjectId(clientSettings.projectId());
        }
        ServiceAccountCredentials serviceAccountCredentials = clientSettings.credentials();
        // override token server URI
        final URI tokenServerUri = clientSettings.tokenUri();
        if (Strings.hasLength(tokenServerUri.toString())) {
            // Rebuild the service account credentials in order to use a custom Token url.
            // This is mostly used for testing purpose.
            serviceAccountCredentials = serviceAccountCredentials.toBuilder().setTokenServerUri(tokenServerUri).build();
        }
        storageOptionsBuilder.setCredentials(serviceAccountCredentials);
        return storageOptionsBuilder.build();
    }

    /**
     * Converts timeout values from the settings to a timeout value for the Google
     * Cloud SDK
     **/
    static Integer toTimeout(TimeValue timeout) {
        // Null or zero in settings means the default timeout
        if (timeout == null || TimeValue.ZERO.equals(timeout)) {
            // negative value means using the default value
            return -1;
        }
        // -1 means infinite timeout
        if (TimeValue.MINUS_ONE.equals(timeout)) {
            // 0 is the infinite timeout expected by Google Cloud SDK
            return 0;
        }
        return Math.toIntExact(timeout.millis());
    }
}
