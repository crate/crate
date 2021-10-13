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
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

import io.crate.analyze.repositories.TypeSettings;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.repositories.s3.S3RepositorySettings.ACCESS_KEY_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.SECRET_KEY_SETTING;

/**
 * A plugin to add a repository type that writes to and from the AWS S3.
 */
public class S3RepositoryPlugin extends Plugin implements RepositoryPlugin {

    static {
        try {
            // kick jackson to do some static caching of declared members info
            Jackson.jsonNodeOf("{}");
            // ClientConfiguration clinit has some classloader problems
            // TODO: fix that
            Class.forName("com.amazonaws.ClientConfiguration");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected final S3Service service;

    public S3RepositoryPlugin() {
        this.service = new S3Service();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(ACCESS_KEY_SETTING, SECRET_KEY_SETTING);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(final Environment env, final NamedXContentRegistry registry, ClusterService clusterService) {
        return Collections.singletonMap(
            S3Repository.TYPE,
            new Repository.Factory() {

                @Override
                public TypeSettings settings() {
                    return new TypeSettings(List.of(), S3Repository.optionalSettings());
                }

                @Override
                public Repository create(RepositoryMetadata metadata) throws Exception {
                    return new S3Repository(metadata, registry, service, clusterService);
                }
            }
        );
    }

    @Override
    public void close() {
        service.close();
    }
}
