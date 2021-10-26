/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysSnapshotsTest extends ESTestCase {

    @Test
    public void testUnavailableSnapshotsAreFilteredOut() throws Exception {
        HashMap<String, SnapshotId> snapshots = new HashMap<>();
        SnapshotId s1 = new SnapshotId("s1", UUIDs.randomBase64UUID());
        SnapshotId s2 = new SnapshotId("s2", UUIDs.randomBase64UUID());
        snapshots.put(s1.getUUID(), s1);
        snapshots.put(s2.getUUID(), s2);
        RepositoryData repositoryData = new RepositoryData(
            1, snapshots, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), ShardGenerations.EMPTY);

        Repository r1 = mock(Repository.class);
        doAnswer((Answer<Void>) invocation -> {
            ActionListener<RepositoryData> callback = invocation.getArgument(0);
            callback.onResponse(repositoryData);
            return null;
        }).when(r1).getRepositoryData(any());

        when(r1.getMetadata()).thenReturn(new RepositoryMetadata("repo1", "fs", Settings.EMPTY));
        doAnswer((Answer<Void>) invocation -> {
            ActionListener<SnapshotInfo> callback = invocation.getArgument(1);
            callback.onFailure(new SnapshotException("repo1", "s1", "Everything is wrong"));
            return null;
        }).when(r1).getSnapshotInfo(eq(s1), any());
        doAnswer((Answer<Void>) invocation -> {
            ActionListener<SnapshotInfo> callback = invocation.getArgument(1);
            callback.onResponse(new SnapshotInfo(s2, Collections.emptyList(), SnapshotState.SUCCESS));
            return null;
        }).when(r1).getSnapshotInfo(eq(s2), any());

        SysSnapshots sysSnapshots = new SysSnapshots(() -> Collections.singletonList(r1));
        Stream<SysSnapshot> currentSnapshots = StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(sysSnapshots.currentSnapshots().get().iterator(), Spliterator.ORDERED),
            false
        );
        assertThat(
            currentSnapshots.map(SysSnapshot::name).collect(Collectors.toList()),
            containsInAnyOrder("s1", "s2")
        );
    }

    @Test
    public void test_current_snapshot_does_not_fail_if_get_repository_data_raises_exception() throws Exception {
        Repository r1 = mock(Repository.class);
        Mockito
            .doThrow(new IllegalStateException("some error"))
            .when(r1).getRepositoryData(any());

        SysSnapshots sysSnapshots = new SysSnapshots(() -> List.of(r1));
        CompletableFuture<Iterable<SysSnapshot>> currentSnapshots = sysSnapshots.currentSnapshots();
        Iterable<SysSnapshot> iterable = currentSnapshots.get(5, TimeUnit.SECONDS);
        assertThat(iterable.iterator().hasNext(), is(false));
    }
}
