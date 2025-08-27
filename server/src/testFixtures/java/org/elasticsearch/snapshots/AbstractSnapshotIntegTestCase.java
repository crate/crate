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
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;

import io.crate.common.unit.TimeValue;
import io.crate.concurrent.FutureActionListener;


public abstract class AbstractSnapshotIntegTestCase extends IntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            // Rebalancing is causing some checks after restore to randomly fail
            // due to https://github.com/elastic/elasticsearch/issues/9421
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockRepository.Plugin.class);
        return plugins;
    }

    @After
    public void assertConsistentHistoryInLuceneIndex() throws Exception {
        cluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
    }

    @After
    public void verifyNoLeakedListeners() throws Exception {
        assertBusy(() -> {
            for (SnapshotsService snapshotsService : cluster().getInstances(SnapshotsService.class)) {
                assertThat(snapshotsService.assertAllListenersResolved()).isTrue();
            }
        }, 30L, TimeUnit.SECONDS);
    }

    private String skipRepoConsistencyCheckReason;

    @After
    public void assertRepoConsistency() {
        if (skipRepoConsistencyCheckReason == null) {
            RepositoriesService repositoriesService = cluster().getCurrentMasterNodeInstance(RepositoriesService.class);
            repositoriesService.getRepositoriesList().forEach(r -> BlobStoreTestUtil.assertRepoConsistency(
                cluster(),
                r.getMetadata().name())
            );
        } else {
            logger.info("--> skipped repo consistency checks because [{}]", skipRepoConsistencyCheckReason);
        }
    }

    protected void createRepo(String repoName, String type) {
        execute(
            "CREATE REPOSITORY \"" + repoName + "\" TYPE \"" + type + "\" WITH (location = ?, compress = ?, chunk_size = ?)",
            new Object[] {
                randomRepoPath().toAbsolutePath().toString(),
                randomBoolean(),
                randomIntBetween(100, 1000)
            }
        );
    }

    protected void disableRepoConsistencyCheck(String reason) {
        assertThat(reason).isNotNull();
        skipRepoConsistencyCheckReason = reason;
    }

    protected RepositoryData getRepositoryData(String repository) throws InterruptedException {
        return getRepositoryData(cluster().getMasterNodeInstance(RepositoriesService.class).repository(repository));
    }

    protected RepositoryData getRepositoryData(Repository repository) throws InterruptedException {
        return FutureUtils.get(repository.getRepositoryData());
    }

    public static void stopNode(final String node) throws IOException {
        cluster().stopRandomNode(settings -> settings.get("node.name").equals(node));
    }

    public static void waitForBlock(String node, String repository, TimeValue timeout) throws InterruptedException {
        long start = System.nanoTime();
        RepositoriesService repositoriesService = cluster().getInstance(RepositoriesService.class, node);
        MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
        while (System.nanoTime() - start < timeout.nanos()) {
            if (mockRepository.blocked()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Timeout waiting for node [" + node + "] to be blocked");
    }

    public static String blockMasterFromFinalizingSnapshotOnIndexFile(final String repositoryName) {
        final String masterName = cluster().getMasterName();
        mockRepo(repositoryName, masterName).setBlockAndFailOnWriteIndexFile();
        return masterName;
    }

    public String blockNodeWithIndex(final String repositoryName, final String indexName) {
        Index index = resolveIndex(indexName);
        String indexUUID = index.getUUID();

        for(String node : cluster().nodesInclude(indexUUID)) {
            mockRepo(repositoryName, node)
                .blockOnDataFiles(true);
            return node;
        }
        fail("No nodes for the index " + indexName + " found");
        return null;
    }

    public static MockRepository mockRepo(String repository, String nodeName) {
        return (MockRepository) cluster().getInstance(RepositoriesService.class, nodeName)
                .repository(repository);
    }

    public static void blockAllDataNodes(String repository) {
        for(RepositoriesService repositoriesService : cluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository)repositoriesService.repository(repository)).blockOnDataFiles(true);
        }
    }

    public static void unblockAllDataNodes(String repository) {
        for(RepositoriesService repositoriesService : cluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository)repositoriesService.repository(repository)).unblock();
        }
    }

    public static void unblockNode(final String repository, final String node) {
        mockRepo(repository, node).unblock();
    }

    protected void awaitClusterState(String viaNode, Predicate<ClusterState> statePredicate) throws Exception {
        ClusterService clusterService = cluster().getInstance(ClusterService.class, viaNode);
        ClusterStateObserver observer = new ClusterStateObserver(
            clusterService,
            TimeValue.timeValueSeconds(30),
            logger
        );
        if (statePredicate.test(observer.setAndGetObservedState()) == false) {
            final FutureActionListener<Void> future = new FutureActionListener<>();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    future.onResponse(null);
                }

                @Override
                public void onClusterServiceClose() {
                    future.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    future.onFailure(new TimeoutException());
                }
            }, statePredicate);
            FutureUtils.get(future, 30L, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    protected SnapshotInfo snapshotInfo(String repo, String snapshot) throws Exception {
        execute(
            "SELECT id, concrete_indices, started, reason, finished, total_shards, failures, include_global_state " +
                "FROM sys.snapshots WHERE repository = ? AND name = ?",
            new Object[]{repo, snapshot}
        );
        return new SnapshotInfo(
            new SnapshotId(snapshot, (String) response.rows()[0][0]),
            (List<String>) response.rows()[0][1],
            (Long) response.rows()[0][2],
            (String) response.rows()[0][3],
            (Long) response.rows()[0][4],
            (int) response.rows()[0][5],
            (List<SnapshotShardFailure>) response.rows()[0][6],
            (boolean) response.rows()[0][7]
        );
    }

}
