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

package org.elasticsearch.repositories.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import com.amazonaws.services.s3.AbstractAmazonS3;


public class S3RepositoryTests extends ESTestCase {

    private static class DummyS3Client extends AbstractAmazonS3 {

        @Override
        public void shutdown() {
            // TODO check is closed
        }
    }

    private static class DummyS3Service extends S3Service {
        @Override
        public AmazonS3Reference client(RepositoryMetadata metadata) {
            return new AmazonS3Reference(new DummyS3Client());
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testCreateRepositoryWithValidChunkBufferSizeSettings() {
        // chunk > buffer should pass
        final Settings s2 = bufferAndChunkSettings(5, 10);
        createS3Repo(getRepositoryMetadata(s2)).close();
        // chunk = buffer should pass
        final Settings s3 = bufferAndChunkSettings(5, 5);
        createS3Repo(getRepositoryMetadata(s3)).close();
    }

    @Test
    public void testCreateRepositoryWithChunkSmallerThanBufferSize() {
        assertThatThrownBy(
            () -> createS3Repo(getRepositoryMetadata(bufferAndChunkSettings(10, 5))))
            .isExactlyInstanceOf(RepositoryException.class)
            .hasMessageEndingWith("chunk_size (5mb) can't be lower than buffer_size (10mb).");
    }

    @Test
    public void testCreateRepositoryWithBufferSizeSmallerThan5mb() {
        assertThatThrownBy(
            () -> createS3Repo(getRepositoryMetadata(bufferAndChunkSettings(4, 10))))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("failed to parse value [4mb] for setting [buffer_size], must be >= [5mb]");
    }

    @Test
    public void testCreateRepositoryWithChunkSizeGreaterThan5tb() {
        assertThatThrownBy(
            () -> createS3Repo(getRepositoryMetadata(bufferAndChunkSettings(5, 6000000))))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("failed to parse value [6000000mb] for setting [chunk_size], must be <= [5tb]");
    }

    private Settings bufferAndChunkSettings(long buffer, long chunk) {
        return Settings.builder()
            .put(S3RepositorySettings.BUFFER_SIZE_SETTING.getKey(),
                 new ByteSizeValue(buffer, ByteSizeUnit.MB).getStringRep())
            .put(S3RepositorySettings.CHUNK_SIZE_SETTING.getKey(),
                 new ByteSizeValue(chunk, ByteSizeUnit.MB).getStringRep())
            .build();
    }

    private RepositoryMetadata getRepositoryMetadata(Settings settings) {
        return new RepositoryMetadata("dummy-repo", "mock", Settings.builder().put(settings).build());
    }

    @Test
    public void testBasePathSetting() {
        final RepositoryMetadata metadata = new RepositoryMetadata("dummy-repo", "mock", Settings.builder()
            .put(S3RepositorySettings.BASE_PATH_SETTING.getKey(), "foo/bar").build());
        try (S3Repository s3repo = createS3Repo(metadata)) {
            assertThat(s3repo.basePath().buildAsString()).isEqualTo("foo/bar/");
        }
    }

    private S3Repository createS3Repo(RepositoryMetadata metadata) {
        return new S3Repository(
            metadata,
            NamedWriteableRegistry.EMPTY,
            NamedXContentRegistry.EMPTY,
            new DummyS3Service(),
            BlobStoreTestUtil.mockClusterService(),
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))) {

            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually on test/main threads
            }
        };
    }
}
