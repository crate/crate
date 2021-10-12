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

import io.crate.common.unit.TimeValue;
import io.crate.testing.UseJdbc;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for snapshot/restore that require at least 2 threads available
 * in the thread pool (for example, tests that use the mock repository that
 * block on master).
 */
@UseJdbc(0)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MinThreadsSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put("thread_pool.snapshot.core", 2)
            .put("thread_pool.snapshot.max", 2)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockRepository.Plugin.class);
        return plugins;
    }

    public void testConcurrentSnapshotDeletionsNotAllowed() throws Exception {
        logger.info("--> creating repository");

        assertAcked(client().admin().cluster().preparePutRepository("repo").setType("mock").setSettings(
            Settings.builder()
                .put("location", randomRepoPath())
                .put("random", randomAlphaOfLength(10))
                .put("wait_after_unblock", 200)).get());

        logger.info("--> snapshot twice");
        execute("create table doc.test1 (x integer) clustered into 1 shards with (number_of_replicas=0)");
        for (int i = 0; i < 10; i++) {
            execute("insert into doc.test1 values(?)", new Object[]{i});
        }
        refresh();
        execute("create snapshot repo.snapshot1 all WITH (wait_for_completion=true)");

        execute("create table doc.test2(x integer) clustered into 1 shards with (number_of_replicas=0)");

        for (int i = 0; i < 10; i++) {
            execute("insert into doc.test2 values(?)", new Object[]{i});
        }
        refresh();
        execute("create snapshot repo.snapshot2 all WITH (wait_for_completion=true)");

        String blockedNode = internalCluster().getMasterName();
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, blockedNode).repository("repo")).blockOnDataFiles(true);
        logger.info("--> start deletion of second snapshot");
        ActionFuture<AcknowledgedResponse> future =
            client().admin().cluster().prepareDeleteSnapshot("repo", "snapshot2").execute();
        logger.info("--> waiting for block to kick in on node [{}]", blockedNode);
        waitForBlock(blockedNode, "repo", TimeValue.timeValueSeconds(10));

        logger.info("--> try deleting the first snapshot, should fail because there is already a deletion is in progress");
        try {
            execute("drop snapshot repo.snapshot1");
            fail("should not be able to delete snapshots concurrently");
        } catch (ConcurrentSnapshotExecutionException e) {
            assertThat(e.getMessage(), containsString("cannot delete - another snapshot is currently being deleted"));
        }

        logger.info("--> unblocking blocked node [{}]", blockedNode);
        unblockNode("repo", blockedNode);

        logger.info("--> wait until second snapshot deletion is finished");
        assertAcked(future.actionGet());

        logger.info("--> try delete first snapshot again, which should now work");
        execute("drop snapshot repo.snapshot1");
        assertTrue(client().admin().cluster().prepareGetSnapshots("repo").setSnapshots("_all").get().getSnapshots().isEmpty());
    }
}
