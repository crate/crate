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

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CorruptedBlobStoreRepositoryIT extends AbstractSnapshotIntegTestCase {

    public void testConcurrentlyChangeRepositoryContents() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        final String repoName = "test";

        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        execute("CREATE REPOSITORY test TYPE fs with (location=?, compress=false, chunk_size=?)",
                new Object[]{repo.toAbsolutePath().toString(), randomIntBetween(100, 1000) + ByteSizeUnit.BYTES.getSuffix()});

        execute("create table doc.test1(x integer)");
        execute("create table doc.test2(x integer)");

        logger.info("--> indexing some data");

        execute("insert into doc.test1 values(1),(2)");
        execute("insert into doc.test2 values(1),(2)");

        final String snapshot = "snapshot1";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true).setIndices("test*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                   equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData =
            getRepositoryData(internalCluster().getMasterNodeInstance(RepositoriesService.class).repository(repoName));
        Files.move(repo.resolve("index-" + repositoryData.getGenId()), repo.resolve("index-" + (repositoryData.getGenId() + 1)));

        assertRepositoryBlocked(client, repoName, snapshot);

        if (randomBoolean()) {
            logger.info("--> move index-N blob back to initial generation");
            Files.move(repo.resolve("index-" + (repositoryData.getGenId() + 1)), repo.resolve("index-" + repositoryData.getGenId()));

            logger.info("--> verify repository remains blocked");
            assertRepositoryBlocked(client, repoName, snapshot);
        }

        logger.info("--> remove repository");
        execute("drop REPOSITORY test");

        logger.info("--> recreate repository");

        execute("CREATE REPOSITORY test TYPE fs with (location=?, compress=false, chunk_size=?)",
                new Object[]{repo.toAbsolutePath().toString(), randomIntBetween(100, 1000) + ByteSizeUnit.BYTES.getSuffix()});

        logger.info("--> delete snapshot");
        execute("drop snapshot test.snapshot1");

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(SnapshotMissingException.class, () -> client.admin().cluster().prepareGetSnapshots(repoName)
            .addSnapshots(snapshot).get().getSnapshots());
    }

    private void assertRepositoryBlocked(Client client, String repo, String existingSnapshot) {
        logger.info("--> try to delete snapshot");
        final RepositoryException repositoryException3 = expectThrows(RepositoryException.class,
                                                                      () -> client.admin().cluster().prepareDeleteSnapshot(repo, existingSnapshot).execute().actionGet());
        assertThat(repositoryException3.getMessage(),
                   containsString("Could not read repository data because the contents of the repository do not match its expected state."));

        logger.info("--> try to create snapshot");
        final RepositoryException repositoryException4 = expectThrows(RepositoryException.class,
                                                                      () -> client.admin().cluster().prepareCreateSnapshot(repo, existingSnapshot).execute().actionGet());
        assertThat(repositoryException4.getMessage(),
                   containsString("Could not read repository data because the contents of the repository do not match its expected state."));
    }
}
