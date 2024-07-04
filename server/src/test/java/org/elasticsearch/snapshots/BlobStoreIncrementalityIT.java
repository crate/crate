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

import static java.util.Collections.unmodifiableMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.INDEX_SHARD_SNAPSHOT_FORMAT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup;

import io.crate.common.concurrent.CompletableFutures;
import io.crate.testing.UseRandomizedSchema;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakGroup
public class BlobStoreIncrementalityIT extends AbstractSnapshotIntegTestCase {

    @ClassRule
    public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMP_FOLDER.getRoot().getAbsolutePath())
            .build();
    }

    // test needs to be reworked when: https://github.com/crate/crate/issues/12598 is implemented
    // in order to match the original test from es (restore table with different names
    // https://github.com/elastic/elasticsearch/commit/5b9864db2c318b5fd9699075b6e8e3ccab565675
    @UseRandomizedSchema(random = false)
    @Test
    public void testIncrementalBehaviorOnPrimaryFailover() throws Exception {
        cluster().startMasterOnlyNode();
        final String primaryNode = cluster().startDataOnlyNode();
        final String indexName = "test_index";
        execute("CREATE TABLE " + indexName + "(a string) " +
                "CLUSTERED INTO 1 SHARDS WITH(number_of_replicas=1,\"unassigned.node_left.delayed_timeout\"=0)");
        ensureYellow();
        final String newPrimary = cluster().startDataOnlyNode();
        ensureGreen();

        logger.info("--> adding some documents to test index");
        List<Object[]> data = new ArrayList<>();
        for (int j = 0; j < randomIntBetween(1, 10); ++j) {
            for (int i = 0; i < scaledRandomIntBetween(1, 100); ++i) {
                data.add(new Object[]{"foo" + j + "_bar" + i});
            }
        }
        int documentCountOriginal = data.size();
        execute("INSERT INTO " + indexName + "(a) VALUES(?)", data.toArray(new Object[][]{}));
        refresh();

        final String snapshot1 = "snap_1";
        final String repo = "test_repo";
        logger.info("--> creating repository");
        execute("CREATE REPOSITORY " + repo  + " TYPE \"fs\" with (location=?)",
                new Object[]{TEMP_FOLDER.newFolder().getAbsolutePath()});

        logger.info("--> creating snapshot 1");
        execute("CREATE SNAPSHOT " + repo + "." + snapshot1 + " TABLE " + indexName + " WITH (wait_for_completion = true)");

        logger.info("--> Shutting down initial primary node [{}]", primaryNode);
        stopNode(primaryNode);

        ensureYellow();
        final String snapshot2 = "snap_2";
        logger.info("--> creating snapshot 2");
        execute("CREATE SNAPSHOT " + repo + "." + snapshot2 + " TABLE doc." + indexName + " WITH (wait_for_completion = true)");

        assertTwoIdenticalShardSnapshots(repo, snapshot1, snapshot2, newPrimary);
        assertCountInIndexThenDelete(indexName, documentCountOriginal);

        ensureRestoreSingleShardSuccessfully(repo, indexName, snapshot1, documentCountOriginal);
        assertCountInIndexThenDelete(indexName, documentCountOriginal);

        ensureRestoreSingleShardSuccessfully(repo, indexName, snapshot2, documentCountOriginal);
        assertCountInIndexThenDelete(indexName, documentCountOriginal);

        String newNewPrimary = cluster().startDataOnlyNode();
        ensureGreen();

        // Originally the test restores the index with different name, and here it deletes some
        // docs from the index before the next round of snapshot/restore, see comment on the test
        // method declaration.
        // logger.info("--> delete some documents from test index");
        logger.info("--> recreating test index with new set of docs");
        execute("CREATE TABLE " + indexName + "(a string) " +
                "CLUSTERED INTO 1 SHARDS WITH(number_of_replicas=1,\"unassigned.node_left.delayed_timeout\"=0)");
        ensureYellow();
        data = new ArrayList<>();
        for (int j = 0; j < randomIntBetween(10, 20); ++j) {
            for (int i = 0; i < scaledRandomIntBetween(100, 200); ++i) {
                data.add(new Object[]{"foo" + j + "_bar" + i});
            }
        }
        execute("INSERT INTO " + indexName + "(a) VALUES(?)", data.toArray(new Object[][]{}));
        int countAfterRecreation = data.size();
        refresh();

        final String snapshot3 = "snap_3";
        logger.info("--> creating snapshot 3");
        execute("CREATE SNAPSHOT " + repo + "." + snapshot3 + " TABLE " + indexName + " WITH (wait_for_completion = true)");

        logger.info("--> Shutting down new primary node [{}]", newPrimary);
        stopNode(newPrimary);
        ensureYellow();

        final String snapshot4 = "snap_4";
        logger.info("--> creating snapshot 4");
        execute("CREATE SNAPSHOT " + repo + "." + snapshot4 + " TABLE " + indexName + " WITH (wait_for_completion = true)");

        assertTwoIdenticalShardSnapshots(repo, snapshot3, snapshot4, newNewPrimary);
        assertCountInIndexThenDelete(indexName, countAfterRecreation);

        ensureRestoreSingleShardSuccessfully(repo, indexName, snapshot3, countAfterRecreation);
        assertCountInIndexThenDelete(indexName, countAfterRecreation);

        ensureRestoreSingleShardSuccessfully(repo, indexName, snapshot4, countAfterRecreation);
        assertCountInIndexThenDelete(indexName, countAfterRecreation);
    }

    @UseRandomizedSchema(random = false)
    @Test
    public void testForceMergeCausesFullSnapshot() throws Exception {
        cluster().startMasterOnlyNode();
        cluster().ensureAtLeastNumDataNodes(2);
        final String indexName = "test_index";
        execute("CREATE TABLE " + indexName + "(a string) CLUSTERED INTO 1 SHARDS");
        ensureGreen();

        logger.info("--> adding some documents to test index and flush in between to get at least two segments");
        for (int j = 0; j < 5; j++) {
            List<Object[]> data = new ArrayList<>();
            for (int i = 0; i < scaledRandomIntBetween(1, 100); ++i) {
                data.add(new Object[]{"foo" + j + "_bar" + i});
            }
            execute("INSERT INTO " + indexName + "(a) VALUES(?)", data.toArray(new Object[][]{}));
            refresh();
        }
        execute("SELECT count(*) FROM sys.segments WHERE primary=true AND table_name=?", new Object[]{indexName});
        assertThat((long) response.rows()[0][0]).isGreaterThan(1);

        final String snapshot1 = "snap1";
        final String repo = "test_repo";
        logger.info("--> creating repository");
        execute("CREATE REPOSITORY " + repo  + " TYPE \"fs\" with (location=?)",
                new Object[]{TEMP_FOLDER.newFolder().getAbsolutePath()});

        logger.info("--> creating snapshot 1");
        execute("CREATE SNAPSHOT " + repo + "." + snapshot1 + " TABLE " + indexName + " WITH (wait_for_completion = true)");

        logger.info("--> force merging down to a single segment");
        execute("OPTIMIZE TABLE " + indexName + " WITH(max_num_segments=1)");
        refresh();
        execute("SELECT count(*) FROM sys.segments WHERE primary=true AND table_name=?", new Object[]{indexName});
        assertThat((long) response.rows()[0][0]).isEqualTo(1);

        final String snapshot2 = "snap2";
        logger.info("--> creating snapshot 2");
        execute("CREATE SNAPSHOT " + repo + "." + snapshot2 + " TABLE " + indexName + " WITH (wait_for_completion = true)");

        logger.info("--> asserting that the two snapshots refer to different files in the repository");
        final RepositoriesService repositoriesService = cluster().getInstance(RepositoriesService.class);
        final SnapshotInfo snapshotInfo2 = snapshotInfo(repo, snapshot2);

        assertBusy(() -> {
            Map<ShardId, IndexShardSnapshotStatus> snapshotShards = getIndexShardSnapShotStates(
                repositoriesService,
                repo,
                snapshotInfo2
            );
            assertThat(snapshotShards).isNotNull();
            List<IndexShardSnapshotStatus.Copy> statusesSnapshot = snapshotShards
                .values().stream().map(IndexShardSnapshotStatus::asCopy).toList();
            assertThat(statusesSnapshot).hasSize(1);
            IndexShardSnapshotStatus.Copy statusSnap = statusesSnapshot.get(0);

            assertThat(statusSnap.getStage()).isEqualTo(IndexShardSnapshotStatus.Stage.DONE);
            assertThat(statusSnap.getTotalFileCount()).isGreaterThan(0);
            assertThat(statusSnap.getIncrementalFileCount()).isGreaterThan(0);
        }, 30L, TimeUnit.SECONDS);
    }

    private void assertCountInIndexThenDelete(String index, long expectedCount) {
        logger.info("--> asserting that index [{}] contains [{}] documents", index, expectedCount);
        assertThat(getCountForIndex(index)).isEqualTo(expectedCount);
        logger.info("--> deleting index [{}]", index);
        execute("DROP TABLE " + index);
    }

    private void assertTwoIdenticalShardSnapshots(String repo, String snapshot1, String snapshot2, String dataNode)
        throws Exception {
        logger.info(
            "--> asserting that snapshots [{}] and [{}] are referring to the same files in the repository", snapshot1, snapshot2);

        final RepositoriesService repositoriesService = cluster().getInstance(RepositoriesService.class, dataNode);
        final SnapshotInfo snapshotInfo1 = snapshotInfo(repo, snapshot1);
        final int[] fileCount = new int[1];

        assertBusy(() -> {
            Map<ShardId, IndexShardSnapshotStatus> snapshotShards = getIndexShardSnapShotStates(
                repositoriesService,
                repo,
                snapshotInfo1
            );
            assertThat(snapshotShards).isNotNull();
            List<IndexShardSnapshotStatus.Copy> statusesSnapshot = snapshotShards
                    .values().stream().map(IndexShardSnapshotStatus::asCopy).toList();
            assertThat(statusesSnapshot).hasSize(1);
            IndexShardSnapshotStatus.Copy statusSnap = statusesSnapshot.get(0);

            assertThat(statusSnap.getStage()).isEqualTo(IndexShardSnapshotStatus.Stage.DONE);
            assertThat(statusSnap.getTotalFileCount()).isGreaterThan(0);
            fileCount[0] = statusSnap.getTotalFileCount();
            assertThat(statusSnap.getIncrementalFileCount()).isEqualTo(statusSnap.getTotalFileCount());
        }, 30L, TimeUnit.SECONDS);

        final SnapshotInfo snapshotInfo2 = snapshotInfo(repo, snapshot2);

        assertBusy(() -> {
            Map<ShardId, IndexShardSnapshotStatus> snapshotShards = getIndexShardSnapShotStates(
                repositoriesService,
                repo,
                snapshotInfo2
            );
            assertThat(snapshotShards).isNotNull();
            List<IndexShardSnapshotStatus.Copy> statusesSnapshot = snapshotShards
                .values().stream().map(IndexShardSnapshotStatus::asCopy).toList();
            assertThat(statusesSnapshot).hasSize(1);
            IndexShardSnapshotStatus.Copy statusSnap = statusesSnapshot.get(0);

            assertThat(statusSnap.getStage()).isEqualTo(IndexShardSnapshotStatus.Stage.DONE);
            assertThat(statusSnap.getTotalFileCount()).isEqualTo(fileCount[0]);
            assertThat(statusSnap.getIncrementalFileCount()).isZero();
        }, 30L, TimeUnit.SECONDS);
    }

    private void ensureRestoreSingleShardSuccessfully(String repo, String indexName, String snapshot, long numDocs) {
        logger.info("--> restoring [{}]", snapshot);
        execute("RESTORE SNAPSHOT " + repo + "." + snapshot + " TABLE " + indexName + " WITH (wait_for_completion=true)");
        execute("SELECT count(*) FROM sys.shards WHERE table_name=?", new Object[]{indexName});
        assertThat(response.rows()[0][0]).isEqualTo(2L);
        execute("SELECT num_docs FROM sys.shards WHERE primary=true AND table_name=?", new Object[]{indexName});
        assertThat(response.rows()[0][0]).isEqualTo(numDocs);
    }

    private long getCountForIndex(String indexName) {
        execute("SELECT COUNT(*) FROM " + indexName);
        return (long) response.rows()[0][0];
    }

    /**
     * Wrapper method to avoid assertions at the {@link BlobStoreRepository#blobStore()} which requires the current
     * thread to be part of a defined ES thread pool.
     */
    private Map<ShardId, IndexShardSnapshotStatus> getIndexShardSnapShotStates(RepositoriesService repositoriesService,
                                                                               final String repositoryName,
                                                                               final SnapshotInfo snapshotInfo) throws Exception {
        CompletableFuture<Map<ShardId, IndexShardSnapshotStatus>> future = new CompletableFuture<>();
        var thread = EsExecutors.daemonThreadFactory(ThreadPool.Names.GENERIC).newThread(
            () -> {
                try {
                    future.complete(snapshotShards(repositoriesService, repositoryName, snapshotInfo));
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );
        thread.start();
        return future.get(10, TimeUnit.SECONDS);
    }

    /**
     * Derived from Elasticsearch's TransportSnapshotsStatusAction.snapshotShards. The TransportSnapshotsStatusAction
     * is not included at CrateDB as it's functionality is not exposed/used.
     * https://github.com/elastic/elasticsearch/blob/7.10/server/src/main/java/org/elasticsearch/action/admin/cluster/snapshots/status/TransportSnapshotsStatusAction.java#L341
     */
    private Map<ShardId, IndexShardSnapshotStatus> snapshotShards(RepositoriesService repositoriesService,
                                                                  final String repositoryName,
                                                                  final SnapshotInfo snapshotInfo) throws Exception {
        BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repositoryName);
        RepositoryData repositoryData = getRepositoryData(repository);

        List<CompletableFuture<IndexMetadata>> listeners = new ArrayList<>(snapshotInfo.indices().size());
        for (String index : snapshotInfo.indices()) {
            IndexId indexId = repositoryData.resolveIndexId(index);
            listeners.add(repository.getSnapshotIndexMetadata(repositoryData, snapshotInfo.snapshotId(), indexId));
        }
        final Map<ShardId, IndexShardSnapshotStatus> shardStatus = new HashMap<>();
        List<IndexMetadata> indexMetadataList = CompletableFutures.allAsList(listeners).get(10, TimeUnit.SECONDS);
        for (IndexMetadata indexMetadata : indexMetadataList) {
            if (indexMetadata != null) {
                int numberOfShards = indexMetadata.getNumberOfShards();
                for (int i = 0; i < numberOfShards; i++) {
                    ShardId shardId = new ShardId(indexMetadata.getIndex(), i);

                    SnapshotShardFailure shardFailure = findShardFailure(snapshotInfo.shardFailures(), shardId);
                    if (shardFailure != null) {
                        shardStatus.put(shardId, IndexShardSnapshotStatus.newFailed(shardFailure.reason()));
                    } else {
                        final IndexShardSnapshotStatus shardSnapshotStatus;
                        if (snapshotInfo.state() == SnapshotState.FAILED) {
                            // If the snapshot failed, but the shard's snapshot does
                            // not have an exception, it means that partial snapshots
                            // were disabled and in this case, the shard snapshot will
                            // *not* have any metadata, so attempting to read the shard
                            // snapshot status will throw an exception.  Instead, we create
                            // a status for the shard to indicate that the shard snapshot
                            // could not be taken due to partial being set to false.
                            shardSnapshotStatus = IndexShardSnapshotStatus.newFailed("skipped");
                        } else {
                            shardSnapshotStatus = getShardSnapshotStatus(
                                repository,
                                snapshotInfo.snapshotId(),
                                repositoryData.resolveIndexId(shardId.getIndexName()),
                                shardId);
                        }
                        shardStatus.put(shardId, shardSnapshotStatus);
                    }
                }
            }
        }
        return unmodifiableMap(shardStatus);
    }

    /**
     * Derived from ES's BlobStoreRepository.getShardSnapshotStatus. This method is removed at CrateDB as it's
     * functionality is not exposed/used.
     * See https://github.com/elastic/elasticsearch/blob/6492b9c9aa1cfb044952aaa194c08825fb63e2f4/server/src/main/java/org/elasticsearch/repositories/blobstore/BlobStoreRepository.java#L2252
     */
    private IndexShardSnapshotStatus getShardSnapshotStatus(BlobStoreRepository repository,
                                                            SnapshotId snapshotId,
                                                            IndexId indexId,
                                                            ShardId shardId) throws IOException {
        var indicesPath = repository.basePath().add("indices");
        var shardContainer = repository.blobStore()
            .blobContainer(indicesPath.add(indexId.getId()).add(Integer.toString(shardId.id())));

        BlobStoreIndexShardSnapshot snapshot = INDEX_SHARD_SNAPSHOT_FORMAT.read(shardContainer, snapshotId.getUUID(), xContentRegistry());
        return IndexShardSnapshotStatus.newDone(snapshot.startTime(), snapshot.time(),
            snapshot.incrementalFileCount(), snapshot.totalFileCount(),
            snapshot.incrementalSize(), snapshot.totalSize(), null); // Not adding a real generation here as it doesn't matter to callers
    }

    private static SnapshotShardFailure findShardFailure(List<SnapshotShardFailure> shardFailures, ShardId shardId) {
        for (SnapshotShardFailure shardFailure : shardFailures) {
            if (shardId.getIndexName().equals(shardFailure.index()) && shardId.id() == shardFailure.shardId()) {
                return shardFailure;
            }
        }
        return null;
    }
}
