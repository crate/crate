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

import io.crate.integrationtests.SQLIntegrationTestCase;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.elasticsearch.repositories.RepositoryDataTests.generateRandomRepoData;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class BlobStoreRepositoryTest extends SQLIntegrationTestCase {

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
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
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
        future.actionGet();
        assertChildren(repo.basePath(), Collections.singleton("foo"));
        assertBlobsByPrefix(repo.basePath(), "fo", Collections.emptyMap());
        assertChildren(repo.basePath().add("foo"), List.of("nested", "nested2"));
        assertBlobsByPrefix(repo.basePath().add("foo"), "nest",
                            Collections.singletonMap("nested-blob", new PlainBlobMetadata("nested-blob", testBlobLen)));
        assertChildren(repo.basePath().add("foo").add("nested"), Collections.emptyList());
    }

    void assertBlobsByPrefix(BlobPath path, String prefix, Map<String, BlobMetadata> blobs) throws Exception {
        final PlainActionFuture<Map<String, BlobMetadata>> future = PlainActionFuture.newFuture();
        final BlobStoreRepository repository = getRepository();
        repository.threadPool().generic().execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repository.blobStore();
                future.onResponse(blobStore.blobContainer(path).listBlobsByPrefix(prefix));
            }
        });
        Map<String, BlobMetadata> foundBlobs = future.actionGet();
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
        final PlainActionFuture<Set<String>> future = PlainActionFuture.newFuture();
        final BlobStoreRepository repository = getRepository();
        repository.threadPool().generic().execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repository.blobStore();
                future.onResponse(blobStore.blobContainer(path).children().keySet());
            }
        });
        Set<String> foundChildren = future.actionGet();
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
        assertThat(repository.getRepositoryData().getSnapshotIds().size(), equalTo(0));
        final RepositoryData emptyData = RepositoryData.EMPTY;
        repository.writeIndexGen(emptyData, emptyData.getGenId(), true);
        RepositoryData repoData = repository.getRepositoryData();
        assertEquals(repoData, emptyData);
        assertEquals(repoData.getIndices().size(), 0);
        assertEquals(repoData.getSnapshotIds().size(), 0);
        assertEquals(0L, repoData.getGenId());

        // write to and read from an index file with snapshots but no indices
        repoData = addRandomSnapshotsToRepoData(repoData, false);
        repository.writeIndexGen(repoData, repoData.getGenId(), true);
        assertEquals(repoData, repository.getRepositoryData());

        // write to and read from a index file with random repository data
        repoData = addRandomSnapshotsToRepoData(repository.getRepositoryData(), true);
        repository.writeIndexGen(repoData, repoData.getGenId(), true);
        assertEquals(repoData, repository.getRepositoryData());
    }

    @Test
    public void testIndexGenerationalFiles() throws Exception {
        final BlobStoreRepository repository = getRepository();

        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        repository.writeIndexGen(repositoryData, repositoryData.getGenId(), true);
        assertThat(repository.getRepositoryData(), equalTo(repositoryData));
        assertThat(repository.latestIndexBlobId(), equalTo(0L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(0L));

        // adding more and writing to a new index generational file
        repositoryData = addRandomSnapshotsToRepoData(repository.getRepositoryData(), true);
        repository.writeIndexGen(repositoryData, repositoryData.getGenId(), true);
        assertEquals(repository.getRepositoryData(), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(1L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(1L));

        // removing a snapshot and writing to a new index generational file
        repositoryData = repository.getRepositoryData().removeSnapshot(
            repositoryData.getSnapshotIds().iterator().next(), ShardGenerations.EMPTY);
        repository.writeIndexGen(repositoryData, repositoryData.getGenId(), true);
        assertEquals(repository.getRepositoryData(), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(2L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(2L));
    }

    @Test
    public void testRepositoryDataConcurrentModificationNotAllowed() throws Exception {
        final BlobStoreRepository repository = getRepository();
        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        final long startingGeneration = repositoryData.getGenId();
        repository.writeIndexGen(repositoryData, startingGeneration, true);

        // write repo data again to index generational file, errors because we already wrote to the
        // N+1 generation from which this repository data instance was created
        expectThrows(RepositoryException.class, () -> repository.writeIndexGen(
            repositoryData.withGenId(startingGeneration + 1), repositoryData.getGenId(), true));
    }

    protected BlobStoreRepository getRepository() throws Exception {
        RepositoriesService service = internalCluster().getInstance(RepositoriesService.class, internalCluster().getMasterName());
        return (BlobStoreRepository) service.repository(REPOSITORY_NAME);
    }

    private static RepositoryData addRandomSnapshotsToRepoData(RepositoryData repoData, boolean inclIndices) {
        int numSnapshots = randomIntBetween(1, 20);
        for (int i = 0; i < numSnapshots; i++) {
            SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            int numIndices = inclIndices ? randomIntBetween(0, 20) : 0;
            final ShardGenerations.Builder builder = ShardGenerations.builder();
            for (int j = 0; j < numIndices; j++) {
                builder.put(new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID()), 0, "1");
            }
            repoData = repoData.addSnapshot(snapshotId, randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL, SnapshotState.FAILED), builder.build());
        }
        return repoData;
    }
}
