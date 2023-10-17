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
package org.elasticsearch.snapshots.mockstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.TestFutureUtils;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.action.FutureActionListener;

public class MockEventuallyConsistentRepositoryTests extends ESTestCase {

    private final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));

    @Test
    public void testReadAfterWriteConsistently() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (BlobStoreRepository repository = new MockEventuallyConsistentRepository(
            new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY),
            xContentRegistry(), BlobStoreTestUtil.mockClusterService(), recoverySettings, blobStoreContext, random())) {
            repository.start();
            final BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath());
            final String blobName = randomAlphaOfLength(10);
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            blobContainer.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, true);
            try (InputStream in = blobContainer.readBlob(blobName)) {
                final byte[] readBytes = new byte[lengthWritten + 1];
                final int lengthSeen = in.read(readBytes);
                assertThat(lengthSeen).isEqualTo(lengthWritten);
                assertArrayEquals(blobData, Arrays.copyOf(readBytes, lengthWritten));
            }
        }
    }

    @Test
    public void testReadAfterWriteAfterReadThrows() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (BlobStoreRepository repository = new MockEventuallyConsistentRepository(
            new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY),
            xContentRegistry(), BlobStoreTestUtil.mockClusterService(), recoverySettings, blobStoreContext, random())) {
            repository.start();
            final BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath());
            final String blobName = randomAlphaOfLength(10);
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            assertThatThrownBy(() -> blobContainer.readBlob(blobName))
                .isExactlyInstanceOf(NoSuchFileException.class);
            blobContainer.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, true);
            assertThrowsOnInconsistentRead(blobContainer, blobName);
        }
    }

    @Test
    public void testReadAfterDeleteAfterWriteThrows() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (BlobStoreRepository repository = new MockEventuallyConsistentRepository(
            new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY),
            xContentRegistry(), BlobStoreTestUtil.mockClusterService(), recoverySettings, blobStoreContext, random())) {
            repository.start();
            final BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath());
            final String blobName = randomAlphaOfLength(10);
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            blobContainer.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, true);
            blobContainer.deleteBlobsIgnoringIfNotExists(Collections.singletonList(blobName));
            assertThrowsOnInconsistentRead(blobContainer, blobName);
            blobStoreContext.forceConsistent();
            assertThatThrownBy(() -> blobContainer.readBlob(blobName))
                .isExactlyInstanceOf(NoSuchFileException.class);
        }
    }

    @Test
    public void testOverwriteRandomBlobFails() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (BlobStoreRepository repository = new MockEventuallyConsistentRepository(
            new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY),
            xContentRegistry(), BlobStoreTestUtil.mockClusterService(), recoverySettings, blobStoreContext, random())) {
            repository.start();
            final BlobContainer container = repository.blobStore().blobContainer(repository.basePath());
            final String blobName = randomAlphaOfLength(10);
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            container.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, false);
            assertThatThrownBy(() -> container.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten - 1, false))
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("Tried to overwrite blob [" + blobName +"]");
        }
    }

    @Test
    public void testOverwriteShardSnapBlobFails() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (BlobStoreRepository repository = new MockEventuallyConsistentRepository(
            new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY),
            xContentRegistry(), BlobStoreTestUtil.mockClusterService(), recoverySettings, blobStoreContext, random())) {
            repository.start();
            final BlobContainer container =
                repository.blobStore().blobContainer(repository.basePath().add("indices").add("someindex").add("0"));
            final String blobName = BlobStoreRepository.SNAPSHOT_PREFIX + UUIDs.randomBase64UUID();
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            container.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, false);
            assertThatThrownBy(() -> container.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, false))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Shard level snap-{uuid} blobs should never be overwritten");
        }
    }

    @Test
    public void testOverwriteSnapshotInfoBlob() throws Exception {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        final RepositoryMetadata metaData = new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(metaData);
        try (BlobStoreRepository repository =
                 new MockEventuallyConsistentRepository(metaData, xContentRegistry(), clusterService, recoverySettings, blobStoreContext, random())) {
            clusterService.addStateApplier(event -> repository.updateState(event.state()));
            // Apply state once to initialize repo properly like RepositoriesService would
            repository.updateState(clusterService.state());
            repository.start();

            // We create a snap- blob for snapshot "foo" in the first generation
            final SnapshotId snapshotId = new SnapshotId("foo", UUIDs.randomBase64UUID());
            TestFutureUtils.<RepositoryData, Exception>get(f ->
                // We try to write another snap- blob for "foo" in the next generation. It fails because the content differs.
                repository.finalizeSnapshot(
                    ShardGenerations.EMPTY,
                    RepositoryData.EMPTY_REPO_GEN,
                    Metadata.EMPTY_METADATA,
                    new SnapshotInfo(snapshotId, Collections.emptyList(), 0L, null, 1L, 5, Collections.emptyList(), true),
                    Version.CURRENT,
                    Function.identity(),
                    f
                ));

            // We try to write another snap- blob for "foo" in the next generation. It fails because the content differs.
            FutureActionListener<RepositoryData, RepositoryData> fut = FutureActionListener.newInstance();
            repository.finalizeSnapshot(
                ShardGenerations.EMPTY,
                0L,
                Metadata.EMPTY_METADATA,
                new SnapshotInfo(snapshotId, Collections.emptyList(), 0L, null, 1L, 6, Collections.emptyList(), true),
                Version.CURRENT,
                Function.identity(),
                fut
            );
            assertThat(fut).failsWithin(5, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .havingCause()
                    .isExactlyInstanceOf(AssertionError.class)
                    .withMessage("\nExpected: <6>\n     but: was <5>");

            // We try to write yet another snap- blob for "foo" in the next generation.
            // It passes cleanly because the content of the blob except for the timestamps.
            TestFutureUtils.<RepositoryData, Exception>get(f ->
                repository.finalizeSnapshot(
                    ShardGenerations.EMPTY,
                    0L,
                    Metadata.EMPTY_METADATA,
                    new SnapshotInfo(snapshotId, Collections.emptyList(), 0L, null, 2L, 5, Collections.emptyList(), true),
                    Version.CURRENT,
                    Function.identity(),
                    f
                ));
        }
    }

    private static void assertThrowsOnInconsistentRead(BlobContainer blobContainer, String blobName) {
        assertThatThrownBy(() -> blobContainer.readBlob(blobName))
            .isInstanceOf(AssertionError.class)
            .hasMessage("Inconsistent read on [" + blobName + ']');
    }
}
