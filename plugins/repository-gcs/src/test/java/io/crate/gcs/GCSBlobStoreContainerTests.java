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

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package io.crate.gcs;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import com.google.cloud.BatchResult;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;

public class GCSBlobStoreContainerTests extends IntegTestCase {

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteBlobsIgnoringIfNotExistsThrowsIOException() throws Exception {
        final List<String> blobs = Arrays.asList("blobA", "blobB");

        final StorageBatch batch = mock(StorageBatch.class);
        if (randomBoolean()) {
            StorageBatchResult<Boolean> result = mock(StorageBatchResult.class);
            when(batch.delete(any(BlobId.class))).thenReturn(result);
            doThrow(new StorageException(new IOException("Batch submit throws a storage exception"))).when(batch).submit();
        } else {
            StorageBatchResult<Boolean> resultA = mock(StorageBatchResult.class);
            doReturn(resultA).when(batch).delete(eq(BlobId.of("bucket", "blobA")));
            doAnswer(invocation -> {
                StorageException storageException = new StorageException(new IOException("Batched delete throws a storage exception"));
                ((BatchResult.Callback) invocation.getArguments()[0]).error(storageException);
                return null;
            }).when(resultA).notify(any(StorageBatchResult.Callback.class));

            StorageBatchResult<Boolean> resultB = mock(StorageBatchResult.class);
            doReturn(resultB).when(batch).delete(eq(BlobId.of("bucket", "blobB")));
            doAnswer(invocation -> {
                if (randomBoolean()) {
                    StorageException storageException = new StorageException(new IOException("Batched delete throws a storage exception"));
                    ((BatchResult.Callback) invocation.getArguments()[0]).error(storageException);
                } else {
                    ((BatchResult.Callback) invocation.getArguments()[0]).success(randomBoolean());
                }
                return null;
            }).when(resultB).notify(any(StorageBatchResult.Callback.class));

            doNothing().when(batch).submit();
        }

        final Storage storage = mock(Storage.class);
        when(storage.get("bucket")).thenReturn(mock(Bucket.class));
        when(storage.batch()).thenReturn(batch);

        final GCSService storageService = mock(GCSService.class);
        when(storageService.client(any(RepositoryMetadata.class))).thenReturn(storage);
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("test", "bla", Settings.EMPTY);
        try (BlobStore store = new GCSBlobStore("bucket", storageService, repositoryMetadata, randomIntBetween(1, 8) * 1024)) {
            final BlobContainer container = store.blobContainer(new BlobPath());
            assertThatThrownBy(() -> container.deleteBlobsIgnoringIfNotExists(blobs)).isInstanceOfAny(IOException.class).hasCauseExactlyInstanceOf(StorageException.class);
        }
    }
}
