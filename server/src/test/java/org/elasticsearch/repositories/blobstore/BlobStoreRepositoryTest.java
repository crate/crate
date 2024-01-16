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

package org.elasticsearch.repositories.blobstore;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.repositories.RepositoryDataTests.generateRandomRepoData;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.TestFutureUtils;
import org.elasticsearch.repositories.ESBlobStoreTestCase;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.action.FutureActionListener;

public class BlobStoreRepositoryTest extends IntegTestCase {

    private static final String REPOSITORY_NAME = "my_repo";

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private File defaultRepositoryLocation;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMPORARY_FOLDER.getRoot().getAbsolutePath())
            .build();
    }

    @Before
    public void createRepository() throws Exception {
        // BlobStoreRepository operations must run on specific threads
        Thread.currentThread().setName(ThreadPool.Names.GENERIC);
        defaultRepositoryLocation = TEMPORARY_FOLDER.newFolder();
        execute("CREATE REPOSITORY " + REPOSITORY_NAME + " TYPE \"fs\" with (location=?, compress=True)",
                new Object[]{defaultRepositoryLocation.getAbsolutePath()});
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testListChildren() throws Exception {
        final BlobStoreRepository repo = getRepository();
        final FutureActionListener<Void> future = new FutureActionListener<>();
        final Executor genericExec = repo.threadPool().generic();
        final int testBlobLen = randomIntBetween(1, 100);
        genericExec.execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repo.blobStore();
                blobStore.blobContainer(repo.basePath().add("foo"))
                    .writeBlob("nested-blob", new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)), testBlobLen, false);
                blobStore.blobContainer(repo.basePath().add("foo").add("nested"))
                    .writeBlob("bar", new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)), testBlobLen, false);
                blobStore.blobContainer(repo.basePath().add("foo").add("nested2"))
                    .writeBlob("blub", new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)), testBlobLen, false);
                future.onResponse(null);
            }
        });
        future.get();
        assertChildren(repo.basePath(), Collections.singleton("foo"));
        assertBlobsByPrefix(repo.basePath(), "fo", Collections.emptyMap());
        assertChildren(repo.basePath().add("foo"), List.of("nested", "nested2"));
        assertBlobsByPrefix(repo.basePath().add("foo"), "nest",
                            Collections.singletonMap("nested-blob", new PlainBlobMetadata("nested-blob", testBlobLen)));
        assertChildren(repo.basePath().add("foo").add("nested"), Collections.emptyList());
    }

    void assertBlobsByPrefix(BlobPath path, String prefix, Map<String, BlobMetadata> blobs) throws Exception {
        final FutureActionListener<Map<String, BlobMetadata>> future = new FutureActionListener<>();
        final BlobStoreRepository repository = getRepository();
        repository.threadPool().generic().execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repository.blobStore();
                future.onResponse(blobStore.blobContainer(path).listBlobsByPrefix(prefix));
            }
        });
        Map<String, BlobMetadata> foundBlobs = future.get();
        if (blobs.isEmpty()) {
            assertThat(foundBlobs.keySet(), empty());
        } else {
            assertThat(foundBlobs.keySet(), containsInAnyOrder(blobs.keySet().toArray(Strings.EMPTY_ARRAY)));
            for (Map.Entry<String, BlobMetadata> entry : foundBlobs.entrySet()) {
                assertEquals(entry.getValue().length(), blobs.get(entry.getKey()).length());
            }
        }
    }

    void assertChildren(BlobPath path, Collection<String> children) throws Exception {
        final FutureActionListener<Set<String>> future = new FutureActionListener<>();
        final BlobStoreRepository repository = getRepository();
        repository.threadPool().generic().execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repository.blobStore();
                future.onResponse(blobStore.blobContainer(path).children().keySet());
            }
        });
        Set<String> foundChildren = future.get();
        if (children.isEmpty()) {
            assertThat(foundChildren, empty());
        } else {
            assertThat(foundChildren, containsInAnyOrder(children.toArray(Strings.EMPTY_ARRAY)));
        }
    }

    @Test
    public void testReadAndWriteSnapshotsThroughIndexFile() throws Exception {
        final BlobStoreRepository repository = getRepository();

        // write to and read from a index file with no entries
        assertThat(ESBlobStoreTestCase.getRepositoryData(repository).getSnapshotIds().size(), equalTo(0));
        final RepositoryData emptyData = RepositoryData.EMPTY;
        writeIndexGen(repository, emptyData, emptyData.getGenId());
        RepositoryData repoData = ESBlobStoreTestCase.getRepositoryData(repository);
        assertEquals(repoData, emptyData);
        assertEquals(repoData.getIndices().size(), 0);
        assertEquals(repoData.getSnapshotIds().size(), 0);
        assertEquals(0L, repoData.getGenId());

        // write to and read from an index file with snapshots but no indices
        repoData = addRandomSnapshotsToRepoData(repoData, false);
        writeIndexGen(repository, repoData, repoData.getGenId());
        assertEquals(repoData, ESBlobStoreTestCase.getRepositoryData(repository));

        // write to and read from an index file with random repository data
        repoData = addRandomSnapshotsToRepoData(ESBlobStoreTestCase.getRepositoryData(repository), true);
        writeIndexGen(repository, repoData, repoData.getGenId());
        RepositoryData actual = ESBlobStoreTestCase.getRepositoryData(repository);
        assertEquals(repoData, actual);
    }

    private static void writeIndexGen(BlobStoreRepository repository, RepositoryData repositoryData, long generation) throws Exception {
        TestFutureUtils.<RepositoryData, Exception>get(
            f -> repository.writeIndexGen(repositoryData, generation, Version.CURRENT, UnaryOperator.identity(), f));
    }


    @Test
    public void testIndexGenerationalFiles() throws Exception {
        final BlobStoreRepository repository = getRepository();

        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        writeIndexGen(repository, repositoryData, repositoryData.getGenId());
        assertThat(ESBlobStoreTestCase.getRepositoryData(repository), equalTo(repositoryData));
        assertThat(repository.latestIndexBlobId(), equalTo(0L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(0L));

        // adding more and writing to a new index generational file
        repositoryData = addRandomSnapshotsToRepoData(ESBlobStoreTestCase.getRepositoryData(repository), true);
        writeIndexGen(repository, repositoryData, repositoryData.getGenId());
        assertEquals(ESBlobStoreTestCase.getRepositoryData(repository), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(1L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(1L));

        // removing a snapshot and writing to a new index generational file
        repositoryData = ESBlobStoreTestCase.getRepositoryData(repository).removeSnapshots(
            repositoryData.getSnapshotIds(), ShardGenerations.EMPTY);
        writeIndexGen(repository, repositoryData, repositoryData.getGenId());
        assertEquals(ESBlobStoreTestCase.getRepositoryData(repository), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(2L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(2L));
    }

    @Test
    public void testRepositoryDataConcurrentModificationNotAllowed() throws Exception {
        final BlobStoreRepository repository = getRepository();
        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        final long startingGeneration = repositoryData.getGenId();
        writeIndexGen(repository, repositoryData, startingGeneration);

        // write repo data again to index generational file, errors because we already wrote to the
        // N+1 generation from which this repository data instance was created
        assertThatThrownBy(() -> writeIndexGen(repository, repositoryData.withGenId(startingGeneration + 1), repositoryData.getGenId()))
            .isExactlyInstanceOf(RepositoryException.class);
    }

    protected BlobStoreRepository getRepository() throws Exception {
        RepositoriesService service = cluster().getInstance(RepositoriesService.class, cluster().getMasterName());
        return (BlobStoreRepository) service.repository(REPOSITORY_NAME);
    }

    private RepositoryData addRandomSnapshotsToRepoData(RepositoryData repoData, boolean inclIndices) {
        int numSnapshots = randomIntBetween(1, 20);
        for (int i = 0; i < numSnapshots; i++) {
            SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            int numIndices = inclIndices ? randomIntBetween(0, 20) : 0;
            final ShardGenerations.Builder builder = ShardGenerations.builder();
            for (int j = 0; j < numIndices; j++) {
                builder.put(new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID()), 0, "1");
            }
            final ShardGenerations shardGenerations = builder.build();
            final Map<IndexId, String> indexLookup = shardGenerations.indices().stream().collect(Collectors.toMap(UnaryOperator.identity(), ind -> randomAlphaOfLength(256)));
            final Map<String, String> newIdentifiers = indexLookup.values().stream().collect(Collectors.toMap(UnaryOperator.identity(), ignored -> UUIDs.randomBase64UUID(random())));
            repoData = repoData.addSnapshot(snapshotId,
                                            randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL, SnapshotState.FAILED), Version.CURRENT, shardGenerations,
                                            indexLookup,
                                            newIdentifiers);
        }
        return repoData;
    }
}
