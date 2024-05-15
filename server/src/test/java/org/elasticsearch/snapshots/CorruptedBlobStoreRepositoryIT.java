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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import io.crate.action.FutureActionListener;
import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.testing.Asserts;
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
            getRepositoryData(cluster().getMasterNodeInstance(RepositoriesService.class).repository(repoName));
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

        logger.info("--> creating snapshot");
        execute("create snapshot test.snapshot1 table doc.test1, doc.test2 with (wait_for_completion = true)");

        final Repository repository = cluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData = getRepositoryData(repository);
        final long beforeMoveGen = repositoryData.getGenId();
        Files.move(repo.resolve("index-" + beforeMoveGen), repo.resolve("index-" + (beforeMoveGen + 1)));

        logger.info("--> set next generation as pending in the cluster state");
        final FutureActionListener<Void> csUpdateFuture = new FutureActionListener<>();
        cluster().getCurrentMasterNodeInstance(ClusterService.class).submitStateUpdateTask("set pending generation",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).metadata(
                    Metadata.builder(currentState.metadata())
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
        cluster().fullRestart();
        ensureGreen();

        Repository repositoryAfterRestart = cluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);

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
        final Repository repository = cluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);
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
                Collections.emptyMap(), Collections.emptyMap(), ShardGenerations.EMPTY, IndexMetaDataGenerations.EMPTY);

        Files.write(repo.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + withoutVersions.getGenId()),
            BytesReference.toBytes(BytesReference.bytes(withoutVersions.snapshotsToXContent(JsonXContent.builder(),
                Version.CURRENT))), StandardOpenOption.TRUNCATE_EXISTING);

        logger.info("--> verify that repo is assumed in old metadata format");
        final SnapshotsService snapshotsService = cluster().getCurrentMasterNodeInstance(SnapshotsService.class);
        final ThreadPool threadPool = cluster().getCurrentMasterNodeInstance(ThreadPool.class);
        assertThat(
            FutureUtils.get(threadPool.generic().submit(() ->
                snapshotsService.minCompatibleVersion(Version.CURRENT, getRepositoryData(repository), null)
            )),
            is(SnapshotsService.OLD_SNAPSHOT_FORMAT)
        );

        logger.info("--> verify that snapshot with missing root level metadata can be deleted");
        execute("drop snapshot test.\"" + snapshotToCorrupt.getName() + "\"");

        logger.info("--> verify that repository is assumed in new metadata format after removing corrupted snapshot");
        assertThat(
            FutureUtils.get(threadPool.generic().submit(() ->
                snapshotsService.minCompatibleVersion(Version.CURRENT, getRepositoryData(repository), null)
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
        final Repository repository = cluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository("repo1");
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
        final Repository repository = cluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repository);
        Files.write(repo.resolve("index-" + repositoryData.getGenId()), randomByteArrayOfLength(randomIntBetween(1, 100)));

        logger.info("--> verify loading repository data throws RepositoryException");
        assertThatThrownBy(() -> getRepositoryData(repository))
            .isExactlyInstanceOf(RepositoryException.class);

        logger.info("--> mount repository path in a new repository");
        final String otherRepoName = "other-repo";
        execute("CREATE REPOSITORY \"other-repo\" TYPE fs WITH (location = ?, compress = false)", new Object[] { repo.toAbsolutePath().toString() });
        final Repository otherRepo = cluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(otherRepoName);

        logger.info("--> verify loading repository data from newly mounted repository throws RepositoryException");
        assertThatThrownBy(() -> getRepositoryData(otherRepo))
            .isExactlyInstanceOf(RepositoryException.class);
    }

    @Test
    public void testHandleSnapshotErrorWithBwCFormat() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        execute("CREATE REPOSITORY \"" + repoName + "\" TYPE fs WITH (location = ?)",
                new Object[] { repoPath.toAbsolutePath().toString() });

        // Workaround to simulate BwC situation: taking a snapshot without indices here so that we don't create any new version shard
        // generations (the existence of which would short-circuit checks for the repo containing old version snapshots)
        final String oldVersionSnapshot = "old-version-snapshot";
        final Client client = client();
        var createSnapshot = new CreateSnapshotRequest(repoName, oldVersionSnapshot)
            .waitForCompletion(true);
        var createSnapshotResponse = client.admin().cluster().execute(CreateSnapshotAction.INSTANCE, createSnapshot).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), is(0));

        logger.info("--> writing downgraded RepositoryData");
        final RepositoryData repositoryData = getRepositoryData(repoName);
        final XContentBuilder jsonBuilder = JsonXContent.builder();
        repositoryData.snapshotsToXContent(jsonBuilder, Version.V_4_1_0);
        final RepositoryData downgradedRepoData = RepositoryData.snapshotsFromXContent(JsonXContent.JSON_XCONTENT.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                Strings.toString(jsonBuilder).replace(Version.CURRENT.toString(), SnapshotsService.OLD_SNAPSHOT_FORMAT.toString())),
                repositoryData.getGenId(), randomBoolean());
        Files.write(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + repositoryData.getGenId()),
                BytesReference.toBytes(BytesReference.bytes(
                        downgradedRepoData.snapshotsToXContent(JsonXContent.builder(), Version.V_4_1_0))),
                StandardOpenOption.TRUNCATE_EXISTING);

        logger.info("--> recreating repository to clear caches");
        execute("DROP REPOSITORY \"" + repoName + "\"");
        execute("CREATE REPOSITORY \"" + repoName + "\" TYPE fs WITH (location = ?, compress = false)",
                new Object[] { repoPath.toAbsolutePath().toString() });

        final String indexName = "test-index";
        execute("create table doc.\"test-index\" (x int) with (number_of_replicas = 0)");

        assertCreateSnapshotSuccess(repoName, "snapshot-1");

        // In the old metadata version the shard level metadata could be moved to the next generation for all sorts of reasons, this should
        // not break subsequent repository operations
        logger.info("--> move shard level metadata to new generation");
        final IndexId indexId = getRepositoryData(repoName).resolveIndexId(indexName);
        final Path shardPath = repoPath.resolve("indices").resolve(indexId.getId()).resolve("0");
        final Path initialShardMetaPath = shardPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + "0");
        assertFileExists(initialShardMetaPath);
        Files.move(initialShardMetaPath, shardPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + "1"));

        logger.info("--> delete old version snapshot");
        execute("DROP SNAPSHOT \"" + repoName + "\".\"" + oldVersionSnapshot + "\"");

        assertCreateSnapshotSuccess(repoName, "snapshot-2");
    }

    @Test
    public void testRepairBrokenShardGenerations() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        execute("CREATE REPOSITORY \"" + repoName + "\" TYPE fs WITH (location = ?)",
                new Object[] { repoPath.toAbsolutePath().toString() });

        // Workaround to simulate BwC situation: taking a snapshot without indices here so that we don't create any new version shard
        // generations (the existence of which would short-circuit checks for the repo containing old version snapshots)
        final String oldVersionSnapshot = "old-version-snapshot";
        var createSnapshot = new CreateSnapshotRequest(repoName, oldVersionSnapshot)
            .waitForCompletion(true);
        final var createSnapshotResponse = client().admin().cluster().execute(CreateSnapshotAction.INSTANCE, createSnapshot).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), is(0));

        logger.info("--> writing downgraded RepositoryData");
        final RepositoryData repositoryData = getRepositoryData(repoName);
        final XContentBuilder jsonBuilder = JsonXContent.builder();
        repositoryData.snapshotsToXContent(jsonBuilder, Version.V_4_1_0);
        final RepositoryData downgradedRepoData = RepositoryData.snapshotsFromXContent(JsonXContent.JSON_XCONTENT.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                Strings.toString(jsonBuilder).replace(Version.CURRENT.toString(), SnapshotsService.OLD_SNAPSHOT_FORMAT.toString())),
                repositoryData.getGenId(), randomBoolean());
        Files.write(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + repositoryData.getGenId()),
                BytesReference.toBytes(BytesReference.bytes(
                        downgradedRepoData.snapshotsToXContent(JsonXContent.builder(), Version.V_4_1_0))),
                StandardOpenOption.TRUNCATE_EXISTING);

        logger.info("--> recreating repository to clear caches");
        execute("DROP REPOSITORY \"" + repoName + "\"");
        execute("CREATE REPOSITORY \"" + repoName + "\" TYPE fs WITH (location = ?)",
                new Object[] { repoPath.toAbsolutePath().toString() });

        final String indexName = "test-index";
        execute("create table doc.\"test-index\" (x int) with (number_of_replicas = 0)");

        assertCreateSnapshotSuccess(repoName, "snapshot-1");

        logger.info("--> delete old version snapshot");
        execute("DROP SNAPSHOT \"" + repoName + "\".\"" + oldVersionSnapshot + "\"");

        logger.info("--> move shard level metadata to new generation and make RepositoryData point at an older generation");
        final IndexId indexId = getRepositoryData(repoName).resolveIndexId(indexName);
        final Path shardPath = repoPath.resolve("indices").resolve(indexId.getId()).resolve("0");
        final Path initialShardMetaPath = shardPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + "0");
        assertFileExists(initialShardMetaPath);
        Files.move(initialShardMetaPath, shardPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + randomIntBetween(1, 1000)));

        final RepositoryData repositoryData1 = getRepositoryData(repoName);
        final Map<String, SnapshotId> snapshotIds =
                repositoryData1.getSnapshotIds().stream().collect(Collectors.toMap(SnapshotId::getUUID, Function.identity()));
        final RepositoryData brokenRepoData = new RepositoryData(
                repositoryData1.getGenId(), snapshotIds, snapshotIds.values().stream().collect(
                Collectors.toMap(SnapshotId::getUUID, repositoryData1::getSnapshotState)),
                snapshotIds.values().stream().collect(
                        Collectors.toMap(SnapshotId::getUUID, repositoryData1::getVersion)),
                repositoryData1.getIndices().values().stream().collect(
                        Collectors.toMap(Function.identity(), repositoryData1::getSnapshots)
                ),  ShardGenerations.builder().putAll(repositoryData1.shardGenerations()).put(indexId, 0, "0").build(),
                repositoryData1.indexMetaDataGenerations()
        );
        Files.write(repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + repositoryData1.getGenId()),
                BytesReference.toBytes(BytesReference.bytes(
                        brokenRepoData.snapshotsToXContent(JsonXContent.builder(), Version.CURRENT))),
                StandardOpenOption.TRUNCATE_EXISTING);

        logger.info("--> recreating repository to clear caches");
        execute("DROP REPOSITORY \"" + repoName + "\"");
        execute("CREATE REPOSITORY \"" + repoName + "\" TYPE fs WITH (location = ?)",
                new Object[] { repoPath.toAbsolutePath().toString() });

        assertCreateSnapshotSuccess(repoName, "snapshot-2");
    }

    private void assertCreateSnapshotSuccess(String repoName, String snapshotName) throws Exception {
        logger.info("--> create another snapshot");
        var createSnapshot = new CreateSnapshotRequest(repoName, snapshotName)
            .waitForCompletion(true);
        final SnapshotInfo snapshotInfo = client().admin().cluster().execute(CreateSnapshotAction.INSTANCE, createSnapshot)
                .get().getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        final int successfulShards = snapshotInfo.successfulShards();
        assertThat(successfulShards, greaterThan(0));
        assertThat(successfulShards, equalTo(snapshotInfo.totalShards()));
    }

    private void assertRepositoryBlocked(Client client, String repo, String existingSnapshot) {
        logger.info("--> try to delete snapshot");
        Asserts.assertSQLError(() -> execute("drop snapshot \"" + repo + "\".\"" + existingSnapshot + "\""))
                .hasPGError(PGErrorStatus.INTERNAL_ERROR)
                .hasHTTPError(HttpResponseStatus.INTERNAL_SERVER_ERROR, 5000)
                .hasMessageContaining(
                        "Could not read repository data because the contents of the repository do not match its expected state.");

        logger.info("--> try to create snapshot");
        Asserts.assertSQLError(() -> execute("create snapshot \"" + repo + "\".\"" + existingSnapshot + "\" ALL"))
                .hasPGError(PGErrorStatus.INTERNAL_ERROR)
                .hasHTTPError(HttpResponseStatus.INTERNAL_SERVER_ERROR, 5000)
                .hasMessageContaining(
                        "Could not read repository data because the contents of the repository do not match its expected state.");
    }
}
