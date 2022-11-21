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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import io.crate.common.unit.TimeValue;


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
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
    }

    @After
    public void verifyNoLeakedListeners() throws Exception {
        assertBusy(() -> {
            for (SnapshotsService snapshotsService : internalCluster().getInstances(SnapshotsService.class)) {
                assertThat(snapshotsService.assertAllListenersResolved()).isEqualTo(true);
            }
        }, 30L, TimeUnit.SECONDS);
    }

    private String skipRepoConsistencyCheckReason;

    @After
    public void assertRepoConsistency() {
        if (skipRepoConsistencyCheckReason == null) {
            RepositoriesService repositoriesService = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class);
            repositoriesService.getRepositoriesList().forEach(r -> BlobStoreTestUtil.assertRepoConsistency(
                internalCluster(),
                r.getMetadata().name())
            );
        } else {
            logger.info("--> skipped repo consistency checks because [{}]", skipRepoConsistencyCheckReason);
        }
    }

    protected void disableRepoConsistencyCheck(String reason) {
        assertThat(reason).isNotNull();
        skipRepoConsistencyCheckReason = reason;
    }

    protected RepositoryData getRepositoryData(String repository) throws InterruptedException {
        return getRepositoryData(internalCluster().getMasterNodeInstance(RepositoriesService.class).repository(repository));
    }

    protected RepositoryData getRepositoryData(Repository repository) throws InterruptedException {
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, internalCluster().getMasterName());
        final PlainActionFuture<RepositoryData> repositoryData = PlainActionFuture.newFuture();
        final CountDownLatch latch = new CountDownLatch(1);
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            repository.getRepositoryData(repositoryData);
            latch.countDown();
        });
        latch.await();
        return repositoryData.actionGet();
    }

    public static long getFailureCount(String repository) {
        long failureCount = 0;
        for (RepositoriesService repositoriesService :
            internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class)) {
            MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
            failureCount += mockRepository.getFailureCount();
        }
        return failureCount;
    }

    public static void assertFileCount(Path dir, int expectedCount) throws IOException {
        final List<Path> found = new ArrayList<>();
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                found.add(file);
                return FileVisitResult.CONTINUE;
            }
        });
        assertThat(found.size()).as("Unexpected file count, found: [" + found + "].").isEqualTo(expectedCount);
    }

    public static int numberOfFiles(Path dir) throws IOException {
        final AtomicInteger count = new AtomicInteger();
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                count.incrementAndGet();
                return FileVisitResult.CONTINUE;
            }
        });
        return count.get();
    }

    public static void stopNode(final String node) throws IOException {
        internalCluster().stopRandomNode(settings -> settings.get("node.name").equals(node));
    }

    public static void waitForBlock(String node, String repository, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class, node);
        MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
        while (System.currentTimeMillis() - start < timeout.millis()) {
            if (mockRepository.blocked()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Timeout waiting for node [" + node + "] to be blocked");
    }

    public static String blockMasterFromFinalizingSnapshotOnIndexFile(final String repositoryName) {
        final String masterName = internalCluster().getMasterName();
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, masterName)
            .repository(repositoryName)).setBlockOnWriteIndexFile(true);
        return masterName;
    }

    public static String blockMasterFromFinalizingSnapshotOnSnapFile(final String repositoryName) {
        final String masterName = internalCluster().getMasterName();
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, masterName)
            .repository(repositoryName)).setBlockAndFailOnWriteSnapFiles(true);
        return masterName;
    }

    public static void blockMasterFromDeletingIndexNFile(String repositoryName) {
        final String masterName = internalCluster().getMasterName();
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, masterName)
            .repository(repositoryName)).setBlockOnDeleteIndexFile();
    }

    public static String blockNodeWithIndex(final String repositoryName, final String indexName) {
        for(String node : internalCluster().nodesInclude(indexName)) {
            ((MockRepository)internalCluster().getInstance(RepositoriesService.class, node).repository(repositoryName))
                .blockOnDataFiles(true);
            return node;
        }
        fail("No nodes for the index " + indexName + " found");
        return null;
    }

    public static void blockNodeOnAnyFiles(String repository, String nodeName) {
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, nodeName)
            .repository(repository)).setBlockOnAnyFiles(true);
    }

    public static void blockDataNode(String repository, String nodeName) {
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, nodeName)
            .repository(repository)).blockOnDataFiles(true);
    }

    public static void blockAllDataNodes(String repository) {
        for(RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository)repositoriesService.repository(repository)).blockOnDataFiles(true);
        }
    }

    public static void unblockAllDataNodes(String repository) {
        for(RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository)repositoriesService.repository(repository)).unblock();
        }
    }

    public static void waitForBlockOnAnyDataNode(String repository, TimeValue timeout) throws InterruptedException {
        final boolean blocked = waitUntil(() -> {
            for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
                MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
                if (mockRepository.blocked()) {
                    return true;
                }
            }
            return false;
        }, timeout.millis(), TimeUnit.MILLISECONDS);

        assertThat(blocked).as("No repository is blocked waiting on a data node").isTrue();
    }

    public static void unblockNode(final String repository, final String node) {
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, node).repository(repository)).unblock();
    }

    protected void awaitNoMoreRunningOperations(String viaNode) throws Exception {
        logger.info("--> verify no more operations in the cluster state");
        awaitClusterState(viaNode, state -> state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().isEmpty() &&
            state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).hasDeletionsInProgress() == false);
    }
}
