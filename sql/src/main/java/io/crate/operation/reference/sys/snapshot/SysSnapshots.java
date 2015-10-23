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
import io.crate.operation.collect.IterableGetter;
import io.crate.operation.reference.sys.repositories.SysRepositories;
import io.crate.operation.reference.sys.repositories.SysRepository;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotsService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class SysSnapshots implements IterableGetter {

    private final SysRepositories sysRepositories;
    private final SnapshotsService snapshotsService;

    @Inject
    public SysSnapshots(SysRepositories sysRepositories, SnapshotsService snapshotsService) {
        this.sysRepositories = sysRepositories;
        this.snapshotsService = snapshotsService;
    }

    @Override
    public Iterable<?> getIterable() {
        List<SysSnapshot> sysSnapshots = new ArrayList<>();
        for (Object entry : sysRepositories.getIterable()) {
            final String repositoryName = ((SysRepository) entry).name();
            sysSnapshots.addAll(Lists.transform(snapshotsService.snapshots(repositoryName), new Function<Snapshot, SysSnapshot>() {
                @Nullable
                @Override
                public SysSnapshot apply(@Nullable Snapshot snapshot) {
                    if (snapshot == null) {
                        return null;
                    }
                    return new SysSnapshot(
                            snapshot.name(),
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
