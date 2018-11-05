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

package io.crate.expression.reference.sys.snapshot;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.util.stream.Stream;

@Singleton
public class SysSnapshots {

    private final RepositoriesService repositoriesService;

    @Inject
    public SysSnapshots(RepositoriesService repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    public Iterable<SysSnapshot> currentSnapshots() {
        Stream<SysSnapshot> stream = repositoriesService.getRepositoriesList().stream()
            .flatMap(repository -> repository.getRepositoryData().getSnapshotIds().stream()
                .map(snapshotId -> {
                    SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
                    return new SysSnapshot(
                        snapshotId.getName(),
                        repository.getMetadata().name(),
                        snapshotInfo.indices(),
                        snapshotInfo.startTime(),
                        snapshotInfo.endTime(),
                        snapshotInfo.version().toString(),
                        snapshotInfo.state().name()
                    );
                })
            );
        return stream::iterator;
    }
}
