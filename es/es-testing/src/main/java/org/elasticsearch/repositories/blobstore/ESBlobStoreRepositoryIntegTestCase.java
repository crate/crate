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

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Basic integration tests for blob-based repository validation.
 */
public abstract class ESBlobStoreRepositoryIntegTestCase extends ESIntegTestCase {

    protected abstract void createTestRepository(String name, boolean verify);

    protected void afterCreationCheck(Repository repository) {

    }

    protected void createAndCheckTestRepository(String name) {
        final boolean verify = randomBoolean();
        createTestRepository(name, verify);

        final Iterable<RepositoriesService> repositoriesServices =
            internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class);

        for (RepositoriesService repositoriesService : repositoriesServices) {
            final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(name);

            afterCreationCheck(repository);
            assertThat("blob store has to be lazy initialized",
                repository.getBlobStore(), verify ? is(notNullValue()) : is(nullValue()));
        }

    }

    public void testIndicesDeletedFromRepository() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        final String repoName = "test-repo";
        createAndCheckTestRepository(repoName);

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 20; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();

        logger.info("--> take a snapshot");
        CreateSnapshotResponse createSnapshotResponse =
            client.admin().cluster().prepareCreateSnapshot(repoName, "test-snap").setWaitForCompletion(true).get();
        assertEquals(createSnapshotResponse.getSnapshotInfo().successfulShards(), createSnapshotResponse.getSnapshotInfo().totalShards());

        logger.info("--> indexing more data");
        for (int i = 20; i < 40; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }

        logger.info("--> take another snapshot with only 2 of the 3 indices");
        createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, "test-snap2")
                                     .setWaitForCompletion(true)
                                     .setIndices("test-idx-1", "test-idx-2")
                                     .get();
        assertEquals(createSnapshotResponse.getSnapshotInfo().successfulShards(), createSnapshotResponse.getSnapshotInfo().totalShards());

        logger.info("--> delete a snapshot");
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, "test-snap").get());

        logger.info("--> verify index folder deleted from blob container");
        RepositoriesService repositoriesSvc = internalCluster().getInstance(RepositoriesService.class, internalCluster().getMasterName());
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, internalCluster().getMasterName());
        @SuppressWarnings("unchecked") BlobStoreRepository repository = (BlobStoreRepository) repositoriesSvc.repository(repoName);

        final SetOnce<BlobContainer> indicesBlobContainer = new SetOnce<>();
        final SetOnce<RepositoryData> repositoryData = new SetOnce<>();
        final CountDownLatch latch = new CountDownLatch(1);
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            indicesBlobContainer.set(repository.blobStore().blobContainer(repository.basePath().add("indices")));
            repositoryData.set(repository.getRepositoryData());
            latch.countDown();
        });

        latch.await();
        for (IndexId indexId : repositoryData.get().getIndices().values()) {
            if (indexId.getName().equals("test-idx-3")) {
                assertFalse(indicesBlobContainer.get().blobExists(indexId.getId())); // deleted index
            }
        }
    }

    protected String[] generateRandomNames(int num) {
        Set<String> names = new HashSet<>();
        for (int i = 0; i < num; i++) {
            String name;
            do {
                name = randomAsciiName();
            } while (names.contains(name));
            names.add(name);
        }
        return names.toArray(new String[num]);
    }

    public static CreateSnapshotResponse assertSuccessfulSnapshot(CreateSnapshotRequestBuilder requestBuilder) {
        CreateSnapshotResponse response = requestBuilder.get();
        assertSuccessfulSnapshot(response);
        return response;
    }

    public static void assertSuccessfulSnapshot(CreateSnapshotResponse response) {
        assertThat(response.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(response.getSnapshotInfo().successfulShards(), equalTo(response.getSnapshotInfo().totalShards()));
    }

    public static RestoreSnapshotResponse assertSuccessfulRestore(RestoreSnapshotRequestBuilder requestBuilder) {
        RestoreSnapshotResponse response = requestBuilder.get();
        assertSuccessfulRestore(response);
        return response;
    }

    public static void assertSuccessfulRestore(RestoreSnapshotResponse response) {
        assertThat(response.getRestoreInfo().successfulShards(), greaterThan(0));
        assertThat(response.getRestoreInfo().successfulShards(), equalTo(response.getRestoreInfo().totalShards()));
    }

    public static String randomAsciiName() {
        return randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
    }
}
