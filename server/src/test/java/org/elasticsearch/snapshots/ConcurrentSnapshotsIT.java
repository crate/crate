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

package org.elasticsearch.snapshots;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.integrationtests.disruption.discovery.AbstractDisruptionTestCase;
import io.crate.testing.UseRandomizedSchema;

@UseRandomizedSchema(random = false)
@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class ConcurrentSnapshotsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockRepository.Plugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(AbstractDisruptionTestCase.DEFAULT_SETTINGS)
            .build();
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testLongRunningSnapshotAllowsConcurrentSnapshot() throws Exception {
        cluster().startMasterOnlyNode();
        final String dataNode = cluster().startDataOnlyNode();
        final String repoName = "repo1";
        execute(
            "CREATE REPOSITORY repo1 TYPE mock WITH (location = ?, compress = ?, chunk_size = ?)",
            new Object[] {
                randomRepoPath().toAbsolutePath().toString(),
                randomBoolean(),
                randomIntBetween(100, 1000)
            }
        );
        execute("""
            create table tbl_slow (
                id string primary key,
                s string
            ) clustered into 1 shards with (
                number_of_replicas = 0
            )
        """);
        execute("insert into tbl_slow (id, s) values ('some_id', 'foo')");
        execute("refresh table tbl_slow");

        CompletableFuture<SnapshotInfo> createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "slow-snapshot",
            repoName,
            dataNode
        );

        final String dataNode2 = cluster().startDataOnlyNode();
        ensureStableCluster(3);

        execute("""
            create table tbl_fast (id string primary key, s string)
            clustered into 1 shards
            with (
                number_of_replicas = 0,
                "routing.allocation.include._name" = ?,
                "routing.allocation.exclude._name" = ?
            )
            """,
            new Object[] {
                dataNode2,
                dataNode
            }
        );
        execute("insert into tbl_fast (id, s) values ('some_id', 'foo')");
        execute("refresh table tbl_fast");

        CompletableFuture<SnapshotInfo> future = startFullSnapshot(repoName, "fast-snapshot", "tbl_fast");
        assertSuccessful(future);

        assertThat(createSlowFuture.isDone()).isFalse();
        unblockNode(repoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    private CompletableFuture<SnapshotInfo> startFullSnapshot(String repoName, String snapshotName, String... indices) {
        logger.info("--> creating full snapshot [{}] to repo [{}]", snapshotName, repoName);
        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(repoName, snapshotName)
            .waitForCompletion(true);
        if (indices != null && indices.length > 0) {
            createSnapshotRequest.indices(indices);
        }
        return cluster().client()
            .execute(CreateSnapshotAction.INSTANCE, createSnapshotRequest)
            .thenApply(x -> x.getSnapshotInfo());
    }


    private void assertSuccessful(CompletableFuture<SnapshotInfo> future) throws Exception {
        assertBusy(() -> {
            assertThat(future).isCompletedWithValueMatching(sInfo -> sInfo.state() == SnapshotState.SUCCESS);
        });
    }

    private CompletableFuture<SnapshotInfo> startFullSnapshotBlockedOnDataNode(String snapshotName,
                                                                               String repoName,
                                                                               String dataNode) throws InterruptedException {
        blockDataNode(repoName, dataNode);
        CompletableFuture<SnapshotInfo> future = startFullSnapshot(repoName, snapshotName);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));
        return future;
    }
}
