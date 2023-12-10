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
import static org.elasticsearch.repositories.s3.S3BlobStoreTests.randomMockS3BlobStore;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.repositories.ESBlobStoreContainerTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import io.crate.common.collections.Tuple;

public class S3BlobStoreContainerTests extends ESBlobStoreContainerTestCase {

    @Rule
    public MockitoRule initRule = MockitoJUnit.rule();

    @Override
    protected BlobStore newBlobStore() {
        return randomMockS3BlobStore();
    }

    @Override
    public void testVerifyOverwriteFails() {
        LuceneTestCase.assumeFalse("not implemented because of S3's weak consistency model", true);
    }

    @Test
    public void testExecuteSingleUploadBlobSizeTooLarge() {
        final long blobSize = ByteSizeUnit.GB.toBytes(randomIntBetween(6, 10));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        assertThatThrownBy(
            () -> blobContainer.executeSingleUpload(blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageEndingWith("can't be larger than 5gb");

    }

    @Test
    public void testExecuteSingleUploadBlobSizeLargerThanBufferSize() {
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bufferSizeInBytes()).thenReturn(ByteSizeUnit.MB.toBytes(1));

        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        assertThatThrownBy(
            () -> blobContainer.executeSingleUpload(blobStore, blobName, new ByteArrayInputStream(new byte[0]), ByteSizeUnit.MB.toBytes(2)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageEndingWith("can't be larger than buffer size");
    }

    @Test
    public void testExecuteSingleUpload() throws IOException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = new BlobPath();
        if (randomBoolean()) {
            IntStream.of(randomIntBetween(1, 5)).forEach(value -> blobPath.add("path_" + value));
        }

        final int bufferSize = randomIntBetween(1024, 2048);
        final int blobSize = randomIntBetween(0, bufferSize);

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn((long) bufferSize);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        final boolean serverSideEncryption = randomBoolean();
        when(blobStore.serverSideEncryption()).thenReturn(serverSideEncryption);

        final StorageClass storageClass = randomFrom(StorageClass.values());
        when(blobStore.getStorageClass()).thenReturn(storageClass);

        final CannedAccessControlList cannedAccessControlList = randomBoolean() ? randomFrom(CannedAccessControlList.values()) : null;
        if (cannedAccessControlList != null) {
            when(blobStore.getCannedACL()).thenReturn(cannedAccessControlList);
        }

        final AmazonS3 client = mock(AmazonS3.class);
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        when(blobStore.clientReference()).thenReturn(clientReference);

        final ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        when(client.putObject(argumentCaptor.capture())).thenReturn(new PutObjectResult());

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[blobSize]);
        blobContainer.executeSingleUpload(blobStore, blobName, inputStream, blobSize);

        final PutObjectRequest request = argumentCaptor.getValue();
        assertThat(bucketName).isEqualTo(request.getBucketName());
        assertThat(blobPath.buildAsString() + blobName).isEqualTo(request.getKey());
        assertThat(inputStream).isEqualTo(request.getInputStream());
        assertThat(blobSize).isEqualTo(request.getMetadata().getContentLength());
        assertThat(storageClass).hasToString(request.getStorageClass());
        assertThat(cannedAccessControlList).isEqualTo(request.getCannedAcl());
        if (serverSideEncryption) {
            assertThat(request.getMetadata().getSSEAlgorithm()).isEqualTo(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }
    }

    @Test
    public void testExecuteMultipartUploadBlobSizeTooLarge() {
        final long blobSize = ByteSizeUnit.TB.toBytes(randomIntBetween(6, 10));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        assertThatThrownBy(
            () -> blobContainer.executeMultipartUpload(blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageEndingWith("can't be larger than 5tb");
    }

    @Test
    public void testExecuteMultipartUploadBlobSizeTooSmall() {
        final long blobSize = ByteSizeUnit.MB.toBytes(randomIntBetween(1, 4));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        assertThatThrownBy(
            () -> blobContainer.executeMultipartUpload(blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageEndingWith("can't be smaller than 5mb");
    }

    @Test
    public void testExecuteMultipartUpload() throws IOException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = new BlobPath();
        if (randomBoolean()) {
            IntStream.of(randomIntBetween(1, 5)).forEach(value -> blobPath.add("path_" + value));
        }

        final long blobSize = ByteSizeUnit.GB.toBytes(randomIntBetween(1, 128));
        final long bufferSize =  ByteSizeUnit.MB.toBytes(randomIntBetween(5, 1024));

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn(bufferSize);

        final boolean serverSideEncryption = randomBoolean();
        when(blobStore.serverSideEncryption()).thenReturn(serverSideEncryption);

        final StorageClass storageClass = randomFrom(StorageClass.values());
        when(blobStore.getStorageClass()).thenReturn(storageClass);

        final CannedAccessControlList cannedAccessControlList = randomBoolean() ? randomFrom(CannedAccessControlList.values()) : null;
        if (cannedAccessControlList != null) {
            when(blobStore.getCannedACL()).thenReturn(cannedAccessControlList);
        }

        final AmazonS3 client = mock(AmazonS3.class);
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        when(blobStore.clientReference()).thenReturn(clientReference);

        final ArgumentCaptor<InitiateMultipartUploadRequest> initArgCaptor = ArgumentCaptor.forClass(InitiateMultipartUploadRequest.class);
        final InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
        initResult.setUploadId(randomAlphaOfLength(10));
        when(client.initiateMultipartUpload(initArgCaptor.capture())).thenReturn(initResult);

        final ArgumentCaptor<UploadPartRequest> uploadArgCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);

        final List<String> expectedEtags = new ArrayList<>();
        final long partSize = Math.min(bufferSize, blobSize);
        long totalBytes = 0;
        do {
            expectedEtags.add(randomAlphaOfLength(50));
            totalBytes += partSize;
        } while (totalBytes < blobSize);

        when(client.uploadPart(uploadArgCaptor.capture())).thenAnswer(invocationOnMock -> {
            final UploadPartRequest request = (UploadPartRequest) invocationOnMock.getArguments()[0];
            final UploadPartResult response = new UploadPartResult();
            response.setPartNumber(request.getPartNumber());
            response.setETag(expectedEtags.get(request.getPartNumber() - 1));
            return response;
        });

        final ArgumentCaptor<CompleteMultipartUploadRequest> compArgCaptor = ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
        when(client.completeMultipartUpload(compArgCaptor.capture())).thenReturn(new CompleteMultipartUploadResult());

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);
        blobContainer.executeMultipartUpload(blobStore, blobName, inputStream, blobSize);

        final InitiateMultipartUploadRequest initRequest = initArgCaptor.getValue();
        assertThat(initRequest.getBucketName()).isEqualTo(bucketName);
        assertThat(initRequest.getKey()).isEqualTo(blobPath.buildAsString() + blobName);
        assertThat(initRequest.getStorageClass()).isEqualTo(storageClass);
        assertThat(initRequest.getCannedACL()).isEqualTo(cannedAccessControlList);
        if (serverSideEncryption) {
            assertThat(initRequest.getObjectMetadata().getSSEAlgorithm())
                .isEqualTo(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }

        final Tuple<Long, Long> numberOfParts = S3BlobContainer.numberOfMultiparts(blobSize, bufferSize);

        final List<UploadPartRequest> uploadRequests = uploadArgCaptor.getAllValues();
        assertThat(uploadRequests).hasSize(numberOfParts.v1().intValue());

        for (int i = 0; i < uploadRequests.size(); i++) {
            final UploadPartRequest uploadRequest = uploadRequests.get(i);

            assertThat(uploadRequest.getBucketName()).isEqualTo(bucketName);
            assertThat(uploadRequest.getKey()).isEqualTo(blobPath.buildAsString() + blobName);
            assertThat(uploadRequest.getUploadId()).isEqualTo(initResult.getUploadId());
            assertThat(uploadRequest.getPartNumber()).isEqualTo(i + 1);
            assertThat(uploadRequest.getInputStream()).isEqualTo(inputStream);

            if (i == (uploadRequests.size() -1)) {
                assertThat(uploadRequest.isLastPart()).isTrue();
                assertThat(uploadRequest.getPartSize()).isEqualTo(numberOfParts.v2().longValue());
            } else {
                assertThat(uploadRequest.isLastPart()).isFalse();
                assertThat(uploadRequest.getPartSize()).isEqualTo(bufferSize);
            }
        }

        final CompleteMultipartUploadRequest compRequest = compArgCaptor.getValue();
        assertThat(compRequest.getBucketName()).isEqualTo(bucketName);
        assertThat(compRequest.getKey()).isEqualTo(blobPath.buildAsString() + blobName);
        assertThat(compRequest.getUploadId()).isEqualTo(initResult.getUploadId());

        final List<String> actualETags = compRequest.getPartETags().stream().map(PartETag::getETag).collect(Collectors.toList());
        assertThat(actualETags).isEqualTo(expectedEtags);
    }

    @Test
    public void testExecuteMultipartUploadAborted() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final BlobPath blobPath = new BlobPath();

        final long blobSize = ByteSizeUnit.MB.toBytes(765);
        final long bufferSize =  ByteSizeUnit.MB.toBytes(150);

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn(bufferSize);
        when(blobStore.getStorageClass()).thenReturn(randomFrom(StorageClass.values()));

        final AmazonS3 client = mock(AmazonS3.class);
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        doAnswer(invocation -> {
            clientReference.incRef();
            return clientReference;
        }).when(blobStore).clientReference();

        final String uploadId = randomAlphaOfLength(25);

        final int stage = randomInt(2);
        final List<AmazonClientException> exceptions = Arrays.asList(
            new AmazonClientException("Expected initialization request to fail"),
            new AmazonClientException("Expected upload part request to fail"),
            new AmazonClientException("Expected completion request to fail")
        );

        if (stage == 0) {
            // Fail the initialization request
            when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
                .thenThrow(exceptions.get(stage));

        } else if (stage == 1) {
            final InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
            initResult.setUploadId(uploadId);
            when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenReturn(initResult);

            // Fail the upload part request
            when(client.uploadPart(any(UploadPartRequest.class)))
                .thenThrow(exceptions.get(stage));

        } else {
            final InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
            initResult.setUploadId(uploadId);
            when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenReturn(initResult);

            when(client.uploadPart(any(UploadPartRequest.class))).thenAnswer(invocationOnMock -> {
                final UploadPartRequest request = (UploadPartRequest) invocationOnMock.getArguments()[0];
                final UploadPartResult response = new UploadPartResult();
                response.setPartNumber(request.getPartNumber());
                response.setETag(randomAlphaOfLength(20));
                return response;
            });

            // Fail the completion request
            when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenThrow(exceptions.get(stage));
        }

        final ArgumentCaptor<AbortMultipartUploadRequest> argumentCaptor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        doNothing().when(client).abortMultipartUpload(argumentCaptor.capture());

        assertThatThrownBy(() -> {
            final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);
            blobContainer.executeMultipartUpload(blobStore, blobName, new ByteArrayInputStream(new byte[0]), blobSize);
        }).isExactlyInstanceOf(IOException.class)
            .hasMessage("Unable to upload object [" + blobName + "] using multipart upload")
            .hasCauseExactlyInstanceOf(AmazonClientException.class)
            .extracting(t -> t.getCause(), Assertions.as(InstanceOfAssertFactories.THROWABLE))
            .hasMessage(exceptions.get(stage).getMessage());

        if (stage == 0) {
            verify(client, times(1)).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
            verify(client, times(0)).uploadPart(any(UploadPartRequest.class));
            verify(client, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            verify(client, times(0)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        } else {
            verify(client, times(1)).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));

            if (stage == 1) {
                verify(client, times(1)).uploadPart(any(UploadPartRequest.class));
                verify(client, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            } else {
                verify(client, times(6)).uploadPart(any(UploadPartRequest.class));
                verify(client, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            }

            verify(client, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

            final AbortMultipartUploadRequest abortRequest = argumentCaptor.getValue();
            assertThat(abortRequest.getBucketName()).isEqualTo(bucketName);
            assertThat(abortRequest.getKey()).isEqualTo(blobName);
            assertThat(abortRequest.getUploadId()).isEqualTo(uploadId);
        }
    }

    @Test
    public void testNumberOfMultipartsWithZeroPartSize() {
        assertThatThrownBy(
            () -> S3BlobContainer.numberOfMultiparts(randomNonNegativeLong(), 0L))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Part size must be greater than zero");
    }

    @Test
    public void testNumberOfMultiparts() {
        final ByteSizeUnit unit = randomFrom(ByteSizeUnit.BYTES, ByteSizeUnit.KB, ByteSizeUnit.MB, ByteSizeUnit.GB);
        final long size = unit.toBytes(randomIntBetween(2, 1000));
        final int factor = randomIntBetween(2, 10);

        // Fits in 1 empty part
        assertNumberOfMultiparts(1, 0L, 0L, size);

        // Fits in 1 part exactly
        assertNumberOfMultiparts(1, size, size, size);
        assertNumberOfMultiparts(1, size, size, size * factor);

        // Fits in N parts exactly
        assertNumberOfMultiparts(factor, size, size * factor, size);

        // Fits in N parts plus a bit more
        final long remaining = randomIntBetween(1, (size > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) size - 1);
        assertNumberOfMultiparts(factor + 1, remaining, (size * factor) + remaining, size);
    }

    private static void assertNumberOfMultiparts(final int expectedParts, final long expectedRemaining, long totalSize, long partSize) {
        final Tuple<Long, Long> result = S3BlobContainer.numberOfMultiparts(totalSize, partSize);

        assertThat((long) result.v1())
            .as("Expected number of parts [" + expectedParts + "] but got [" + result.v1() + "]")
            .isEqualTo(expectedParts);
        assertThat((long) result.v2())
            .as("Expected remaining [" + expectedRemaining + "] but got [" + result.v2() + "]")
            .isEqualTo(expectedRemaining);
    }
}
