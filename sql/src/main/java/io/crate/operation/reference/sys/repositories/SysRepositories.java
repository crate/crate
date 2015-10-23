/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys.repositories;

import io.crate.operation.collect.IterableGetter;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class SysRepositories implements ClusterStateListener, IterableGetter {

    protected Map<String, SysRepository> repositoriesTable = new HashMap<>();

    @Inject
    public SysRepositories(ClusterService clusterService) {
        RepositoriesMetaData repositoriesMetaData = clusterService.state().metaData().custom(RepositoriesMetaData.TYPE);
        addRepositories(repositoriesMetaData);
        clusterService.add(this);
    }

    private void addRepositories(@Nullable RepositoriesMetaData repositoriesMetaData) {
        if (repositoriesMetaData == null) {
            return;
        }
        for (RepositoryMetaData repositoryMetaData : repositoriesMetaData.repositories()) {
            SysRepository repository = new SysRepository(
                    repositoryMetaData.name(),
                    repositoryMetaData.type(),
                    repositoryMetaData.settings().getAsStructuredMap());
            repositoriesTable.put(repositoryMetaData.name(), repository);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (repositoriesChanged(event)) {
            RepositoriesMetaData repositoriesMetaData = event.state().metaData().custom(RepositoriesMetaData.TYPE);
            repositoriesTable = new HashMap<>(repositoriesMetaData.repositories().size());
            addRepositories(repositoriesMetaData);
        }
    }

    private boolean repositoriesChanged(ClusterChangedEvent event) {
        RepositoriesMetaData previousRepositoriesMetaData = event.previousState().metaData().custom(RepositoriesMetaData.TYPE);
        RepositoriesMetaData repositoriesMetaData = event.state().metaData().custom(RepositoriesMetaData.TYPE);
        if (previousRepositoriesMetaData == null && repositoriesMetaData == null) {
            return false;
        } else if (previousRepositoriesMetaData == null || repositoriesMetaData == null) {
            return true;
        }
        return !repositoriesMetaData.repositories().equals(previousRepositoriesMetaData.repositories());
    }

    @Override
    public Iterable<?> getIterable() {
        return Collections.unmodifiableCollection(repositoriesTable.values());
    }
}
