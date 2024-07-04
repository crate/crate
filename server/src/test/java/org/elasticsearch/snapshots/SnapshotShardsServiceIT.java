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

package org.elasticsearch.snapshots;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import io.crate.common.unit.TimeValue;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotShardsServiceIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockRepository.Plugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Test
    public void testRetryPostingSnapshotStatusMessages() throws Exception {
        String masterNode = cluster().startMasterOnlyNode();
        String dataNode = cluster().startDataOnlyNode();

        logger.info("-->  creating repository");
        var putRepositoryRequest = new PutRepositoryRequest("repo")
            .type("mock")
            .settings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            );
        assertAcked(client().admin().cluster().execute(PutRepositoryAction.INSTANCE, putRepositoryRequest).get());

        final int shards = between(1, 10);
        execute("create table doc.test(x integer) clustered into ? shards with (number_of_replicas=0)", new Object[]{shards});

        ensureGreen();
        final int numDocs = scaledRandomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            execute("insert into doc.test values(?)", new Object[]{i});
        }

        logger.info("--> blocking repository");
        String blockedNode = blockNodeWithIndex("repo", "test");

        execute("create snapshot repo.snapshot table doc.test with (wait_for_completion = false)");
        waitForBlock(blockedNode, "repo", TimeValue.timeValueSeconds(60));

        SnapshotInfo snapshotInfo = snapshotInfo("repo", "snapshot");
        final SnapshotId snapshotId = snapshotInfo.snapshotId();

        logger.info("--> start disrupting cluster");
        final NetworkDisruption networkDisruption = new NetworkDisruption(new NetworkDisruption.TwoPartitions(masterNode, dataNode),
                                                                          NetworkDisruption.NetworkDelay.random(random()));
        cluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        logger.info("--> unblocking repository");
        unblockNode("repo", blockedNode);

        // Retrieve snapshot status from the data node.
        SnapshotShardsService snapshotShardsService = cluster().getInstance(SnapshotShardsService.class, blockedNode);
        assertBusy(() -> {
            final Snapshot snapshot = new Snapshot("repo", snapshotId);
            List<IndexShardSnapshotStatus.Stage> stages = snapshotShardsService.currentSnapshotShards(snapshot)
                .values().stream().map(status -> status.asCopy().getStage()).collect(Collectors.toList());
            assertThat(stages).hasSize(shards);
            assertThat(stages).allSatisfy(s -> assertThat(s).isEqualTo(IndexShardSnapshotStatus.Stage.DONE));
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> stop disrupting cluster");
        networkDisruption.stopDisrupting();
        cluster().clearDisruptionScheme(true);

        assertBusy(() -> {
            execute("select state, array_length(failures,0) from sys.snapshots where name='snapshot'");
            assertThat(response.rowCount()).isEqualTo(1L);
            assertThat(response.rows()[0][0]).isEqualTo("SUCCESS");
            assertThat(response.rows()[0][1]).isNull();
        }, 30L, TimeUnit.SECONDS);
    }
}
