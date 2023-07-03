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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.lucene.util.SameThreadExecutorService;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.action.FutureActionListener;
import io.crate.common.unit.TimeValue;
import io.crate.server.xcontent.LoggingDeprecationHandler;

public final class BlobStoreTestUtil {

    public static void assertRepoConsistency(TestCluster testCluster, String repoName) {
        final BlobStoreRepository repo =
            (BlobStoreRepository) testCluster.getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);
        BlobStoreTestUtil.assertConsistency(repo, repo.threadPool().executor(ThreadPool.Names.GENERIC));
    }

    public static boolean blobExists(BlobContainer container, String blobName) throws IOException {
        try (InputStream ignored = container.readBlob(blobName)) {
            return true;
        } catch (NoSuchFileException e) {
            return false;
        }
    }

    /**
     * Assert that there are no unreferenced indices or unreferenced root-level metadata blobs in any repository.
     * TODO: Expand the logic here to also check for unreferenced segment blobs and shard level metadata
     * @param repository BlobStoreRepository to check
     * @param executor Executor to run all repository calls on. This is needed since the production {@link BlobStoreRepository}
     *                 implementations assert that all IO inducing calls happen on the generic or snapshot thread-pools and hence callers
     *                 of this assertion must pass an executor on those when using such an implementation.
     */
    public static void assertConsistency(BlobStoreRepository repository, Executor executor) {
        final FutureActionListener<AssertionError, AssertionError> listener = FutureActionListener.newInstance();
        executor.execute(ActionRunnable.supply(listener, () -> {
            try {
                final BlobContainer blobContainer = repository.blobContainer();
                final long latestGen;
                try (DataInputStream inputStream = new DataInputStream(blobContainer.readBlob("index.latest"))) {
                    latestGen = inputStream.readLong();
                } catch (NoSuchFileException e) {
                    throw new AssertionError("Could not find index.latest blob for repo [" + repository + "]");
                }
                assertIndexGenerations(blobContainer, latestGen);
                final RepositoryData repositoryData;
                try (InputStream blob = blobContainer.readBlob(BlobStoreRepository.INDEX_FILE_PREFIX + latestGen);
                     XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                                                                                       LoggingDeprecationHandler.INSTANCE, blob)) {
                    repositoryData = RepositoryData.snapshotsFromXContent(parser, latestGen, false);
                }
                assertIndexUUIDs(repository, repositoryData);
                assertSnapshotUUIDs(repository, repositoryData);
                assertShardIndexGenerations(blobContainer, repositoryData.shardGenerations());
                return null;
            } catch (AssertionError e) {
                return e;
            }
        }));
        final AssertionError err = FutureUtils.get(listener, TimeValue.timeValueMinutes(1L));
        if (err != null) {
            throw new AssertionError(err);
        }
    }

    private static void assertIndexGenerations(BlobContainer repoRoot, long latestGen) throws IOException {
        final long[] indexGenerations = repoRoot.listBlobsByPrefix(BlobStoreRepository.INDEX_FILE_PREFIX).keySet().stream()
            .map(s -> s.replace(BlobStoreRepository.INDEX_FILE_PREFIX, ""))
            .mapToLong(Long::parseLong).sorted().toArray();
        assertEquals(latestGen, indexGenerations[indexGenerations.length - 1]);
        assertTrue(indexGenerations.length <= 2);
    }

    private static void assertShardIndexGenerations(BlobContainer repoRoot, ShardGenerations shardGenerations) throws IOException {
        final BlobContainer indicesContainer = repoRoot.children().get("indices");
        for (IndexId index : shardGenerations.indices()) {
            final List<String> gens = shardGenerations.getGens(index);
            if (gens.isEmpty() == false) {
                final BlobContainer indexContainer = indicesContainer.children().get(index.getId());
                final Map<String, BlobContainer> shardContainers = indexContainer.children();
                for (int i = 0; i < gens.size(); i++) {
                    final String generation = gens.get(i);
                    assertThat(generation, not(ShardGenerations.DELETED_SHARD_GEN));
                    if (generation != null && generation.equals(ShardGenerations.NEW_SHARD_GEN) == false) {
                        final String shardId = Integer.toString(i);
                        assertThat(shardContainers, hasKey(shardId));
                        assertThat(shardContainers.get(shardId).listBlobsByPrefix(BlobStoreRepository.INDEX_FILE_PREFIX),
                                   hasKey(BlobStoreRepository.INDEX_FILE_PREFIX + generation));
                    }
                }
            }
        }
    }

    private static void assertIndexUUIDs(BlobStoreRepository repository, RepositoryData repositoryData) throws IOException {
        final List<String> expectedIndexUUIDs =
            repositoryData.getIndices().values().stream().map(IndexId::getId).collect(Collectors.toList());
        final BlobContainer indicesContainer = repository.blobContainer().children().get("indices");
        final List<String> foundIndexUUIDs;
        if (indicesContainer == null) {
            foundIndexUUIDs = Collections.emptyList();
        } else {
            // Skip Lucene MockFS extraN directory
            foundIndexUUIDs = indicesContainer.children().keySet().stream().filter(
                s -> s.startsWith("extra") == false).collect(Collectors.toList());
        }
        assertThat(foundIndexUUIDs, containsInAnyOrder(expectedIndexUUIDs.toArray(Strings.EMPTY_ARRAY)));
        for (String indexId : foundIndexUUIDs) {
            final Set<String> indexMetaGenerationsFound = indicesContainer.children().get(indexId)
                .listBlobsByPrefix(BlobStoreRepository.METADATA_PREFIX).keySet().stream()
                .map(p -> p.replace(BlobStoreRepository.METADATA_PREFIX, "").replace(".dat", ""))
                .collect(Collectors.toSet());
            final Set<String> indexMetaGenerationsExpected = new HashSet<>();
            final IndexId idx =
                repositoryData.getIndices().values().stream().filter(i -> i.getId().equals(indexId)).findFirst().get();
            for (SnapshotId snapshotId : repositoryData.getSnapshots(idx)) {
                indexMetaGenerationsExpected.add(repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, idx));
            }
            // TODO: assertEquals(indexMetaGenerationsExpected, indexMetaGenerationsFound); requires cleanup functionality for
            //       index meta generations blobs
            assertTrue(indexMetaGenerationsFound.containsAll(indexMetaGenerationsExpected));
        }
    }

    private static void assertSnapshotUUIDs(BlobStoreRepository repository, RepositoryData repositoryData) throws Exception {
        final BlobContainer repoRoot = repository.blobContainer();
        final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        final List<String> expectedSnapshotUUIDs = snapshotIds.stream().map(SnapshotId::getUUID).collect(Collectors.toList());
        for (String prefix : new String[]{BlobStoreRepository.SNAPSHOT_PREFIX, BlobStoreRepository.METADATA_PREFIX}) {
            final Collection<String> foundSnapshotUUIDs = repoRoot.listBlobs().keySet().stream().filter(p -> p.startsWith(prefix))
                .map(p -> p.replace(prefix, "").replace(".dat", ""))
                .collect(Collectors.toSet());
            assertThat(foundSnapshotUUIDs, containsInAnyOrder(expectedSnapshotUUIDs.toArray(Strings.EMPTY_ARRAY)));
        }

        final BlobContainer indicesContainer = repository.blobContainer().children().get("indices");
        final Map<String, BlobContainer> indices;
        if (indicesContainer == null) {
            indices = Collections.emptyMap();
        } else {
            indices = indicesContainer.children();
        }
        final Map<IndexId, Integer> maxShardCountsExpected = new HashMap<>();
        final Map<IndexId, Integer> maxShardCountsSeen = new HashMap<>();
        // Assert that for each snapshot, the relevant metadata was written to index and shard folders
        for (SnapshotId snapshotId: snapshotIds) {
            SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId).get();
            for (String index : snapshotInfo.indices()) {
                final IndexId indexId = repositoryData.resolveIndexId(index);
                assertThat(indices, hasKey(indexId.getId()));
                final BlobContainer indexContainer = indices.get(indexId.getId());
                assertThat(indexContainer.listBlobs(),
                    hasKey(String.format(Locale.ROOT, BlobStoreRepository.METADATA_NAME_FORMAT,
                        repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId))));
                IndexMetadata indexMetadata = repository.getSnapshotIndexMetadata(repositoryData, snapshotId, indexId).get();
                for (Map.Entry<String, BlobContainer> entry : indexContainer.children().entrySet()) {
                    // Skip Lucene MockFS extraN directory
                    if (entry.getKey().startsWith("extra")) {
                        continue;
                    }
                    final int shardId = Integer.parseInt(entry.getKey());
                    final int shardCount = indexMetadata.getNumberOfShards();
                    maxShardCountsExpected.compute(
                        indexId, (i, existing) -> existing == null || existing < shardCount ? shardCount : existing);
                    final BlobContainer shardContainer = entry.getValue();
                    // TODO: we shouldn't be leaking empty shard directories when a shard (but not all of the index it belongs to)
                    //       becomes unreferenced. We should fix that and remove this conditional once its fixed.
                    if (shardContainer.listBlobs().keySet().stream().anyMatch(blob -> blob.startsWith("extra") == false)) {
                        final int impliedCount = shardId - 1;
                        maxShardCountsSeen.compute(
                            indexId, (i, existing) -> existing == null || existing < impliedCount ? impliedCount : existing);
                    }
                    if (shardId < shardCount && snapshotInfo.shardFailures().stream().noneMatch(
                        shardFailure -> shardFailure.index().equals(index) && shardFailure.shardId() == shardId)) {
                        final Map<String, BlobMetadata> shardPathContents = shardContainer.listBlobs();
                        assertThat(shardPathContents,
                            hasKey(String.format(Locale.ROOT, BlobStoreRepository.SNAPSHOT_NAME_FORMAT, snapshotId.getUUID())));
                        assertThat(shardPathContents.keySet().stream()
                            .filter(name -> name.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX)).count(), lessThanOrEqualTo(2L));
                    }
                }
            }
        }
        maxShardCountsSeen.forEach(((indexId, count) -> assertThat("Found unreferenced shard paths for index [" + indexId + "]",
                                                                   count, lessThanOrEqualTo(maxShardCountsExpected.get(indexId)))));
    }


    /**
     * Creates a mocked {@link ClusterService} for use in {@link BlobStoreRepository} related tests that mocks out all the necessary
     * functionality to make {@link BlobStoreRepository} work. Initializes the cluster state as {@link ClusterState#EMPTY_STATE}.
     *
     * @return Mock ClusterService
     */
    public static ClusterService mockClusterService() {
        return mockClusterService(ClusterState.EMPTY_STATE);
    }

    /**
     * Creates a mocked {@link ClusterService} for use in {@link BlobStoreRepository} related tests that mocks out all the necessary
     * functionality to make {@link BlobStoreRepository} work. Initializes the cluster state with a {@link RepositoriesMetadata} instance
     * that contains the given {@code metadata}.
     *
     * @param metadata RepositoryMetadata to initialize the cluster state with
     * @return Mock ClusterService
     */
    public static ClusterService mockClusterService(RepositoryMetadata metadata) {
        return mockClusterService(ClusterState.builder(ClusterState.EMPTY_STATE).metadata(
            Metadata.builder().putCustom(RepositoriesMetadata.TYPE,
                                         new RepositoriesMetadata(Collections.singletonList(metadata))).build()).build());
    }

    private static ClusterService mockClusterService(ClusterState initialState) {
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(ThreadPool.Names.SNAPSHOT)).thenReturn(new SameThreadExecutorService());
        when(threadPool.generic()).thenReturn(new SameThreadExecutorService());
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        final AtomicReference<ClusterState> currentState = new AtomicReference<>(initialState);
        when(clusterService.state()).then(invocationOnMock -> currentState.get());
        final List<ClusterStateApplier> appliers = new CopyOnWriteArrayList<>();
        doAnswer(invocation -> {
            final ClusterStateUpdateTask task = ((ClusterStateUpdateTask) invocation.getArguments()[1]);
            final ClusterState current = currentState.get();
            final ClusterState next = task.execute(current);
            currentState.set(next);
            appliers.forEach(applier -> applier.applyClusterState(
                new ClusterChangedEvent((String) invocation.getArguments()[0], next, current)));
            task.clusterStateProcessed((String) invocation.getArguments()[0], current, next);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        doAnswer(invocation -> {
            appliers.add((ClusterStateApplier) invocation.getArguments()[0]);
            return null;
        }).when(clusterService).addStateApplier(any(ClusterStateApplier.class));
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        return clusterService;
    }
}
