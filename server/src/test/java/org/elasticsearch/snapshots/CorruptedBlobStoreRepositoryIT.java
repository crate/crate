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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

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
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true).setIndices("test*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                   equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

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
        expectThrows(SnapshotMissingException.class, () -> client().admin().cluster().prepareGetSnapshots(repoName)
            .addSnapshots(snapshot).get().getSnapshots());
    }

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
            CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoName, snapshotPrefix + i)
                .setIndices().setWaitForCompletion(true).get();
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
        assertThat(PlainActionFuture.get(f -> threadPool.generic().execute(
            ActionRunnable.wrap(f, (d) -> snapshotsService.hasOldVersionSnapshots(repoName, getRepositoryData(repository), d, null)))),
            is(true));

        logger.info("--> verify that snapshot with missing root level metadata can be deleted");
        client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotToCorrupt.getName()).get();

        logger.info("--> verify that repository is assumed in new metadata format after removing corrupted snapshot");
        assertThat(PlainActionFuture.get(f -> threadPool.generic().execute(
            ActionRunnable.wrap(f, (d) -> snapshotsService.hasOldVersionSnapshots(repoName, getRepositoryData(repository), d,null)))),
            is(false));
        final RepositoryData finalRepositoryData = getRepositoryData(repository);
        for (SnapshotId snapshotId : finalRepositoryData.getSnapshotIds()) {
            assertThat(finalRepositoryData.getVersion(snapshotId), is(Version.CURRENT));
        }
    }

    public void testCorruptedSnapshotIsIgnored() throws Exception {
        Path repo = randomRepoPath();
        final String repoName = "test";
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());

        execute("CREATE REPOSITORY test TYPE fs with (location=?, compress=false, chunk_size=?)",
                new Object[]{repo.toAbsolutePath().toString(), randomIntBetween(100, 1000) + ByteSizeUnit.BYTES.getSuffix()});

        final String snapshotPrefix = "test-snap-";
        final int snapshots = randomIntBetween(2, 2);
        logger.info("--> creating [{}] snapshots", snapshots);
        for (int i = 0; i < snapshots; ++i) {
            CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoName, snapshotPrefix + i)
                .setIndices().setWaitForCompletion(true).get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), is(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                       equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        }
        final Repository repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repository);

        final SnapshotId snapshotToCorrupt = randomFrom(repositoryData.getSnapshotIds());
        logger.info("--> delete root level snapshot metadata blob for snapshot [{}]", snapshotToCorrupt);
        Files.delete(repo.resolve(String.format(Locale.ROOT, BlobStoreRepository.SNAPSHOT_NAME_FORMAT, snapshotToCorrupt.getUUID())));

        logger.info("--> ensure snapshot list can be retrieved without any error if ignoreUnavailable is set");
        var resp = client().admin().cluster().prepareGetSnapshots(repoName).setIgnoreUnavailable(true).get();
        assertThat(resp.getSnapshots().size(), is(snapshots - 1));

        client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotToCorrupt.getName()).get();
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
