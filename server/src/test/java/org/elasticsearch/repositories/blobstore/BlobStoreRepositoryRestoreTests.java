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

package org.elasticsearch.repositories.blobstore;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.TestFutureUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.junit.Test;

import io.crate.common.io.IOUtils;

/**
 * This class tests the behavior of {@link BlobStoreRepository} when it
 * restores a shard from a snapshot but some files with same names already
 * exist on disc.
 */
public class BlobStoreRepositoryRestoreTests extends IndexShardTestCase {

    /**
     * Restoring a snapshot that contains multiple files must succeed even when
     * some files already exist in the shard's store.
     */
    @Test
    public void testRestoreSnapshotWithExistingFiles() throws Exception {
        final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);

        IndexShard shard = newShard(shardId, true);
        try {
            // index documents in the shards
            final int numDocs = scaledRandomIntBetween(1, 500);
            recoverShardFromStore(shard);
            for (int i = 0; i < numDocs; i++) {
                indexDoc(shard, Integer.toString(i));
                if (rarely()) {
                    flushShard(shard, false);
                }
            }
            assertDocCount(shard, numDocs);

            // snapshot the shard
            final Repository repository = createRepository();
            final Snapshot snapshot = new Snapshot(repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "_uuid"));
            snapshotShard(shard, snapshot, repository);

            // capture current store files
            final Store.MetadataSnapshot storeFiles = shard.snapshotStoreMetadata();
            assertFalse(storeFiles.asMap().isEmpty());

            // close the shard
            closeShards(shard);

            // delete some random files in the store
            List<String> deletedFiles = randomSubsetOf(randomIntBetween(1, storeFiles.size() - 1), storeFiles.asMap().keySet());
            for (String deletedFile : deletedFiles) {
                Files.delete(shard.shardPath().resolveIndex().resolve(deletedFile));
            }

            // build a new shard using the same store directory as the closed shard
            ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(shard.routingEntry(),
                                                                          RecoverySource.ExistingStoreRecoverySource.INSTANCE);
            shard = newShard(
                shardRouting,
                shard.shardPath(),
                shard.indexSettings().getIndexMetadata(),
                null,
                List.of(),
                () -> {},
                RetentionLeaseSyncer.EMPTY,
                EMPTY_EVENT_LISTENER
            );

            // restore the shard
            recoverShardFromSnapshot(shard, snapshot, repository);

            // check that the shard is not corrupted
            TestUtil.checkIndex(shard.store().directory());

            // check that all files have been restored
            final Directory directory = shard.store().directory();
            final List<String> directoryFiles = Arrays.asList(directory.listAll());

            for (StoreFileMetadata storeFile : storeFiles) {
                String fileName = storeFile.name();
                assertTrue("File [" + fileName + "] does not exist in store directory", directoryFiles.contains(fileName));
                assertEquals(storeFile.length(), shard.store().directory().fileLength(fileName));
            }
        } finally {
            if (shard != null && shard.state() != IndexShardState.CLOSED) {
                try {
                    shard.close("test", false);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    public void testSnapshotWithConflictingName() throws Exception {
        final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);

        IndexShard shard = newShard(shardId, true);
        try {
            // index documents in the shards
            final int numDocs = scaledRandomIntBetween(1, 500);
            recoverShardFromStore(shard);
            for (int i = 0; i < numDocs; i++) {
                indexDoc(shard, Integer.toString(i));
                if (rarely()) {
                    flushShard(shard, false);
                }
            }
            assertDocCount(shard, numDocs);

            // snapshot the shard
            final Repository repository = createRepository();
            final Snapshot snapshot = new Snapshot(repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "_uuid"));
            final String shardGen = snapshotShard(shard, snapshot, repository);
            assertNotNull(shardGen);
            final Snapshot snapshotWithSameName = new Snapshot(repository.getMetadata().name(), new SnapshotId(
                snapshot.getSnapshotId().getName(), "_uuid2"));
            final ShardGenerations shardGenerations = ShardGenerations.builder().put(indexId, 0, shardGen).build();
            TestFutureUtils.<RepositoryData, Exception>get(f ->
                repository.finalizeSnapshot(
                    shardGenerations,
                    RepositoryData.EMPTY_REPO_GEN,
                    Metadata.builder().put(shard.indexSettings().getIndexMetadata(), false).build(),
                    new SnapshotInfo(snapshot.getSnapshotId(), shardGenerations.indices().stream()
                        .map(IndexId::getName).collect(Collectors.toList()), 0L, null, 1L, 6,
                        Collections.emptyList(), true),
                    Version.CURRENT, Function.identity(), f));
            assertThatThrownBy(() -> snapshotShard(shard, snapshotWithSameName, repository))
                .isExactlyInstanceOf(IndexShardSnapshotFailedException.class)
                .hasMessageContaining("Duplicate snapshot name");
        } finally {
            if (shard != null && shard.state() != IndexShardState.CLOSED) {
                try {
                    shard.close("test", false);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    /** Create a {@link Repository} with a random name **/
    private Repository createRepository() {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(repositoryMetadata);
        final FsRepository repository = new FsRepository(repositoryMetadata, createEnvironment(), xContentRegistry(), clusterService,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually
            }
        };
        clusterService.addStateApplier(event -> repository.updateState(event.state()));
        // Apply state once to initialize repo properly like RepositoriesService would
        repository.updateState(clusterService.state());
        repository.start();
        return repository;
    }

    /** Create a {@link Environment} with random path.home and path.repo **/
    private Environment createEnvironment() {
        Path home = createTempDir();
        return TestEnvironment.newEnvironment(Settings.builder()
                                                  .put(Environment.PATH_HOME_SETTING.getKey(), home.toAbsolutePath())
                                                  .put(Environment.PATH_REPO_SETTING.getKey(), home.resolve("repo").toAbsolutePath())
                                                  .build());
    }
}
