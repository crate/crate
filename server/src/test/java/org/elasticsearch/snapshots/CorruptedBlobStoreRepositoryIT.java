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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.testing.Asserts;
import io.crate.testing.SQLErrorMatcher;
import io.crate.testing.TestingHelpers;
import io.netty.handler.codec.http.HttpResponseStatus;

public class CorruptedBlobStoreRepositoryIT extends AbstractSnapshotIntegTestCase {

    @Test
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
        execute("create snapshot test.snapshot1 table doc.test1, doc.test2 with (wait_for_completion = true)");

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
        execute("select * from sys.snapshots where repository = 'test' and name = 'snapshot1'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testFindDanglingLatestGeneration() throws Exception {
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
        execute("create snapshot test.snapshot1 table doc.test1, doc.test2 with (wait_for_completion = true)");

        final Repository repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData = getRepositoryData(repository);
        final long beforeMoveGen = repositoryData.getGenId();
        Files.move(repo.resolve("index-" + beforeMoveGen), repo.resolve("index-" + (beforeMoveGen + 1)));

        logger.info("--> set next generation as pending in the cluster state");
        final PlainActionFuture<Void> csUpdateFuture = PlainActionFuture.newFuture();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).submitStateUpdateTask("set pending generation",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).metadata(
                    Metadata.builder(currentState.getMetadata())
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        currentState.metadata().<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE).withUpdatedGeneration(
                        repository.getMetadata().name(), beforeMoveGen, beforeMoveGen + 1)).build()).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    csUpdateFuture.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    csUpdateFuture.onResponse(null);
                }
        }
        );
        csUpdateFuture.get();

        logger.info("--> full cluster restart");
        internalCluster().fullRestart();
        ensureGreen();

        Repository repositoryAfterRestart = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);

        logger.info("--> verify index-N blob is found at the new location");
        assertThat(getRepositoryData(repositoryAfterRestart).getGenId(), is(beforeMoveGen + 1));

        logger.info("--> delete snapshot");
        execute("drop snapshot test.snapshot1");

        logger.info("--> verify index-N blob is found at the expected location");
        assertThat(getRepositoryData(repositoryAfterRestart).getGenId(), is(beforeMoveGen + 2));

        logger.info("--> make sure snapshot doesn't exist");
        execute("select * from sys.snapshots where repository = 'test' and name = 'snapshot1'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testHandlingMissingRootLevelSnapshotMetadata() throws Exception {
        Path repo = randomRepoPath();
        final String repoName = "test";
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());

        execute("CREATE REPOSITORY test TYPE fs with (location=?, compress=false, chunk_size=?)",
                new Object[]{repo.toAbsolutePath().toString(), randomIntBetween(100, 1000) + ByteSizeUnit.BYTES.getSuffix()});

        final String snapshotPrefix = "test-snap-";
        final int snapshots = randomIntBetween(1, 2);
        logger.info("--> creating [{}] snapshots", snapshots);
        for (int i = 0; i < snapshots; ++i) {
            // Workaround to simulate BwC situation: taking a snapshot without indices here so that we don't create any new version shard
            // generations (the existence of which would short-circuit checks for the repo containing old version snapshots)
            var createSnapshot = new CreateSnapshotRequest(repoName, snapshotPrefix + i).waitForCompletion(true);
            var createSnapshotResponse = client().admin().cluster().execute(CreateSnapshotAction.INSTANCE, createSnapshot).get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), is(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        }
        final Repository repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repository);

        final SnapshotId snapshotToCorrupt = randomFrom(repositoryData.getSnapshotIds());
        logger.info("--> delete root level snapshot metadata blob for snapshot [{}]", snapshotToCorrupt);
        Files.delete(repo.resolve(String.format(Locale.ROOT, BlobStoreRepository.SNAPSHOT_NAME_FORMAT, snapshotToCorrupt.getUUID())));

        logger.info("--> strip version information from index-N blob");
        final RepositoryData withoutVersions = new RepositoryData(repositoryData.getGenId(),
            repositoryData.getSnapshotIds().stream().collect(Collectors.toMap(
                SnapshotId::getUUID, Function.identity())),
            repositoryData.getSnapshotIds().stream().collect(Collectors.toMap(
                SnapshotId::getUUID, repositoryData::getSnapshotState)),
            Collections.emptyMap(), Collections.emptyMap(), ShardGenerations.EMPTY);

        Files.write(repo.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + withoutVersions.getGenId()),
            BytesReference.toBytes(BytesReference.bytes(withoutVersions.snapshotsToXContent(XContentFactory.jsonBuilder(),
                true))), StandardOpenOption.TRUNCATE_EXISTING);

        logger.info("--> verify that repo is assumed in old metadata format");
        final SnapshotsService snapshotsService = internalCluster().getCurrentMasterNodeInstance(SnapshotsService.class);
        final ThreadPool threadPool = internalCluster().getCurrentMasterNodeInstance(ThreadPool.class);
        assertThat(
            FutureUtils.get(threadPool.generic().submit(() ->
                snapshotsService.minCompatibleVersion(Version.CURRENT, repoName, getRepositoryData(repository), null)
            )),
            is(SnapshotsService.OLD_SNAPSHOT_FORMAT)
        );

        logger.info("--> verify that snapshot with missing root level metadata can be deleted");
        execute("drop snapshot test.\"" + snapshotToCorrupt.getName() + "\"");

        logger.info("--> verify that repository is assumed in new metadata format after removing corrupted snapshot");
        assertThat(
            FutureUtils.get(threadPool.generic().submit(() ->
                snapshotsService.minCompatibleVersion(Version.CURRENT, repoName, getRepositoryData(repository), null)
            )),
            is(Version.CURRENT)
        );
        final RepositoryData finalRepositoryData = getRepositoryData(repository);
        for (SnapshotId snapshotId : finalRepositoryData.getSnapshotIds()) {
            assertThat(finalRepositoryData.getVersion(snapshotId), is(Version.CURRENT));
        }
    }

    @Test
    public void testCorruptedSnapshotIsIgnored() throws Exception {
        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());

        execute("CREATE REPOSITORY repo1 TYPE fs with (location=?, compress=false, chunk_size=?)",
                new Object[]{repo.toAbsolutePath().toString(), randomIntBetween(100, 1000) + ByteSizeUnit.BYTES.getSuffix()});

        final String snapshotPrefix = "test-snap-";
        final int snapshots = 2;
        logger.info("--> creating [{}] snapshots", snapshots);
        for (int i = 0; i < snapshots; ++i) {
            var createSnapshot = new CreateSnapshotRequest("repo1", snapshotPrefix + i).waitForCompletion(true);
            var createSnapshotResponse = client().admin().cluster().execute(CreateSnapshotAction.INSTANCE, createSnapshot).get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), is(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                       equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        }
        final Repository repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository("repo1");
        final RepositoryData repositoryData = getRepositoryData(repository);

        final SnapshotId snapshotToCorrupt = randomFrom(repositoryData.getSnapshotIds());
        logger.info("--> delete root level snapshot metadata blob for snapshot [{}]", snapshotToCorrupt);
        Files.delete(repo.resolve(String.format(Locale.ROOT, BlobStoreRepository.SNAPSHOT_NAME_FORMAT, snapshotToCorrupt.getUUID())));

        logger.info("--> ensure snapshots can be retrieved without any error");
        execute("select state from sys.snapshots where repository = 'repo1' order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "FAILED\n" +
            "SUCCESS\n"
        ));

        execute("drop snapshot repo1.\"" + snapshotToCorrupt.getName() + "\"");
    }

    @Test
    public void testMountCorruptedRepositoryData() throws Exception {
        disableRepoConsistencyCheck("This test intentionally corrupts the repository contents");
        Client client = client();

        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        execute("CREATE REPOSITORY \"test-repo\" TYPE fs WITH (location = ?, compress = false)", new Object[] { repo.toAbsolutePath().toString() });

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        var createSnapshot = new CreateSnapshotRequest(repoName, snapshot)
            .waitForCompletion(true)
            .indices("test-idx-*");
        var createSnapshotResponse = client.admin().cluster().execute(CreateSnapshotAction.INSTANCE, createSnapshot).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> corrupt index-N blob");
        final Repository repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repository);
        Files.write(repo.resolve("index-" + repositoryData.getGenId()), randomByteArrayOfLength(randomIntBetween(1, 100)));

        logger.info("--> verify loading repository data throws RepositoryException");
        expectThrows(RepositoryException.class, () -> getRepositoryData(repository));

        logger.info("--> mount repository path in a new repository");
        final String otherRepoName = "other-repo";
        execute("CREATE REPOSITORY \"other-repo\" TYPE fs WITH (location = ?, compress = false)", new Object[] { repo.toAbsolutePath().toString() });
        final Repository otherRepo = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(otherRepoName);

        logger.info("--> verify loading repository data from newly mounted repository throws RepositoryException");
        expectThrows(RepositoryException.class, () -> getRepositoryData(otherRepo));
    }

    private void assertRepositoryBlocked(Client client, String repo, String existingSnapshot) {
        logger.info("--> try to delete snapshot");
        Asserts.assertThrowsMatches(
            () -> execute("drop snapshot \"" + repo + "\".\"" + existingSnapshot + "\""),
            SQLErrorMatcher.isSQLError(
                containsString("Could not read repository data because the contents of the repository do not match its expected state."),
                PGErrorStatus.INTERNAL_ERROR,
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                5000
            )
        );

        logger.info("--> try to create snapshot");
        Asserts.assertThrowsMatches(
            () -> execute("create snapshot \"" + repo + "\".\"" + existingSnapshot + "\" ALL"),
            SQLErrorMatcher.isSQLError(
                containsString("Could not read repository data because the contents of the repository do not match its expected state."),
                PGErrorStatus.INTERNAL_ERROR,
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                5000
            )
        );
    }
}
