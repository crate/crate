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

import com.amazonaws.util.json.Jackson;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A plugin to add a repository type that writes to and from the AWS S3.
 */
public class S3RepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    static {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                // kick jackson to do some static caching of declared members info
                Jackson.jsonNodeOf("{}");
                // ClientConfiguration clinit has some classloader problems
                // TODO: fix that
                Class.forName("com.amazonaws.ClientConfiguration");
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    protected final S3Service service;

    public S3RepositoryPlugin(final Settings settings) {
        this(settings, new S3Service(settings));
    }

    S3RepositoryPlugin(final Settings settings, final S3Service service) {
        this.service = Objects.requireNonNull(service, "S3 service must not be null");
        // eagerly load client settings so that secure settings are read
        final Map<String, S3ClientSettings> clientsSettings = S3ClientSettings.load(settings);
        this.service.refreshAndClearCache(clientsSettings);
    }

    // proxy method for testing
    protected S3Repository createRepository(final RepositoryMetaData metadata,
                                            final Settings settings,
                                            final NamedXContentRegistry registry) {
        return new S3Repository(metadata, settings, registry, service);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(final Environment env, final NamedXContentRegistry registry) {
        return Collections.singletonMap(S3Repository.TYPE, (metadata) -> createRepository(metadata, env.settings(), registry));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            // named s3 client configuration settings
            S3ClientSettings.ACCESS_KEY_SETTING,
            S3ClientSettings.SECRET_KEY_SETTING,
            S3ClientSettings.SESSION_TOKEN_SETTING,
            S3ClientSettings.ENDPOINT_SETTING,
            S3ClientSettings.PROTOCOL_SETTING,
            S3ClientSettings.PROXY_HOST_SETTING,
            S3ClientSettings.PROXY_PORT_SETTING,
            S3ClientSettings.PROXY_USERNAME_SETTING,
            S3ClientSettings.PROXY_PASSWORD_SETTING,
            S3ClientSettings.READ_TIMEOUT_SETTING,
            S3ClientSettings.MAX_RETRIES_SETTING,
            S3ClientSettings.USE_THROTTLE_RETRIES_SETTING,
            S3Repository.ACCESS_KEY_SETTING,
            S3Repository.SECRET_KEY_SETTING);
    }

    @Override
    public void reload(Settings settings) {
        // secure settings should be readable
        final Map<String, S3ClientSettings> clientsSettings = S3ClientSettings.load(settings);
        service.refreshAndClearCache(clientsSettings);
    }

    @Override
    public void close() throws IOException {
        service.close();
    }
}
