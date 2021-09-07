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

package org.elasticsearch.repositories;

import io.crate.analyze.repositories.TypeSettings;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.repository.LogicalReplicationRepository;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sets up classes for Snapshot/Restore.
 */
public class RepositoriesModule extends AbstractModule {

    private final RepositoriesService repositoriesService;

    public RepositoriesModule(Environment env,
                              List<RepositoryPlugin> repoPlugins,
                              TransportService transportService,
                              ClusterService clusterService,
                              LogicalReplicationService logicalReplicationService,
                              ThreadPool threadPool,
                              NamedXContentRegistry namedXContentRegistry) {
        Map<String, Repository.Factory> factories = new HashMap<>();
        factories.put(FsRepository.TYPE, new Repository.Factory() {

            @Override
            public TypeSettings settings() {
                return new TypeSettings(FsRepository.mandatorySettings(), FsRepository.optionalSettings());
            }

            @Override
            public Repository create(RepositoryMetadata metadata) throws Exception {
                return new FsRepository(metadata, env, namedXContentRegistry, clusterService);
            }
        });
        factories.put(LogicalReplicationRepository.TYPE,
                      new Repository.Factory() {
                          @Override
                          public Repository create(RepositoryMetadata metadata) throws Exception {
                              return new LogicalReplicationRepository(
                                  clusterService.getSettings(),
                                  clusterService,
                                  logicalReplicationService,
                                  metadata,
                                  threadPool
                              );
                          }

                          @Override
                          public TypeSettings settings() {
                              return new TypeSettings(List.of(), List.of());
                          }
                      }
        );

        for (RepositoryPlugin repoPlugin : repoPlugins) {
            Map<String, Repository.Factory> newRepoTypes = repoPlugin.getRepositories(env, namedXContentRegistry, clusterService);
            for (Map.Entry<String, Repository.Factory> entry : newRepoTypes.entrySet()) {
                if (factories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Repository type [" + entry.getKey() + "] is already registered");
                }
            }
        }

        Map<String, Repository.Factory> repositoryTypes = Collections.unmodifiableMap(factories);
        repositoriesService = new RepositoriesService(env.settings(), clusterService, transportService, repositoryTypes, threadPool);
    }

    @Override
    protected void configure() {
        Map<String, Repository.Factory> repositoryTypes = repositoriesService.typesRegistry();
        MapBinder<String, Repository.Factory> typesBinder = MapBinder.newMapBinder(binder(), String.class, Repository.Factory.class);
        repositoryTypes.forEach((k, v) -> typesBinder.addBinding(k).toInstance(v));

        MapBinder<String, TypeSettings> typeSettingsBinder = MapBinder.newMapBinder(
            binder(),
            String.class,
            TypeSettings.class);
        for (var e : repositoryTypes.entrySet()) {
            String repoScheme = e.getKey();
            var repoSettings = e.getValue().settings();
            typeSettingsBinder.addBinding(repoScheme).toInstance(repoSettings);
        }
    }

    public RepositoriesService repositoryService() {
        return repositoriesService;
    }
}
