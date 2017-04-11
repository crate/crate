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

package io.crate.operation.reference.sys.snapshot;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.crate.operation.reference.sys.repositories.SysRepositoriesService;
import io.crate.operation.reference.sys.repositories.SysRepository;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class SysSnapshots {

    private final SysRepositoriesService sysRepositoriesService;
    private final SnapshotsService snapshotsService;
    private final RepositoriesService repositoriesService;

    @Inject
    public SysSnapshots(SysRepositoriesService sysRepositoriesService,
                        SnapshotsService snapshotsService,
                        RepositoriesService repositoriesService) {
        this.sysRepositoriesService = sysRepositoriesService;
        this.snapshotsService = snapshotsService;
        this.repositoriesService = repositoriesService;
    }

    public Iterable<SysSnapshot> snapshotsGetter() {
        List<SysSnapshot> sysSnapshots = new ArrayList<>();
        for (Object entry : sysRepositoriesService.repositoriesGetter()) {
            final String repositoryName = ((SysRepository) entry).name();


            List<SnapshotId> snapshotIds = repositoriesService.repository(repositoryName).getRepositoryData().getSnapshotIds();
            List<SnapshotInfo> snapshots = snapshotsService.snapshots(repositoryName, snapshotIds, true);
            sysSnapshots.addAll(Lists.transform(snapshots, new Function<SnapshotInfo, SysSnapshot>() {
                @Nullable
                @Override
                public SysSnapshot apply(@Nullable SnapshotInfo snapshot) {
                    if (snapshot == null) {
                        return null;
                    }
                    return new SysSnapshot(
                        snapshot.snapshotId().getName(),
                        repositoryName,
                        snapshot.indices(),
                        snapshot.startTime(),
                        snapshot.endTime(),
                        snapshot.version().toString(),
                        snapshot.state().name()
                    );
                }
            }));
        }
        return sysSnapshots;
    }
}
