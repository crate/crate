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

package io.crate.copy.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.zip.GZIPOutputStream;

import org.jetbrains.annotations.Nullable;

import io.crate.common.annotations.NotThreadSafe;
import io.crate.common.concurrent.CompletableFutures;
import io.crate.copy.s3.common.S3ClientHelper;
import io.crate.copy.s3.common.S3URI;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.engine.export.FileOutput;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

@NotThreadSafe
public class S3FileOutput implements FileOutput {

    @Nullable
    private final String protocolSetting;

    public S3FileOutput(@Nullable String protocol) {
        protocolSetting = protocol;
    }

    @Override
    public OutputStream acquireOutputStream(Executor executor, URI uri, WriterProjection.CompressionType compressionType) throws IOException {
        OutputStream outputStream = new S3OutputStream(executor, S3URI.toS3URI(uri), new S3ClientHelper(), protocolSetting);
        if (compressionType != null) {
            outputStream = new GZIPOutputStream(outputStream);
        }
        return outputStream;
    }


    private static class S3OutputStream extends OutputStream {

        private static final int PART_SIZE = 5 * 1024 * 1024;

        private final S3Client client;
        private final CreateMultipartUploadResponse multipartUpload;
        private final Executor executor;
        private final String bucketName;
        private final String key;
        private final List<CompletableFuture<CompletedPart>> pendingUploads = new ArrayList<>();

        private ByteArrayOutputStream outputStream;
        long currentPartBytes = 0;
        int partNumber = 1;

        private S3OutputStream(Executor executor,
                               S3URI s3URI,
                               S3ClientHelper s3ClientHelper,
                               String protocolSetting) throws IOException {
            this.executor = executor;
            bucketName = s3URI.bucket();
            key = s3URI.key();
            outputStream = new ByteArrayOutputStream();
            client = s3ClientHelper.client(s3URI, protocolSetting);
            multipartUpload = client.createMultipartUpload(
                CreateMultipartUploadRequest.builder().bucket(bucketName).key(key).build());
        }

        @Override
        public void write(byte[] b) throws IOException {
            outputStream.write(b);
            currentPartBytes += b.length;
            doUploadIfNeeded();
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            outputStream.write(b, off, len);
            currentPartBytes += len;
            doUploadIfNeeded();
        }

        @Override
        public void write(int b) throws IOException {
            outputStream.write(b);
            currentPartBytes++;
            doUploadIfNeeded();
        }

        private void doUploadIfNeeded() throws IOException {
            if (currentPartBytes >= PART_SIZE) {
                final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
                final int currentPart = partNumber;
                final long currentPartSize = currentPartBytes;

                outputStream.close();
                outputStream = new ByteArrayOutputStream();
                partNumber++;
                pendingUploads.add(CompletableFutures.supplyAsync(() -> {
                    UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .partNumber(currentPart)
                        .contentLength(currentPartSize)
                        .uploadId(multipartUpload.uploadId()).build();
                    var response = client.uploadPart(
                        uploadPartRequest,
                        RequestBody.fromInputStream(inputStream, uploadPartRequest.contentLength()));
                    return CompletedPart.builder().partNumber(currentPart).eTag(response.eTag()).build();
                }, executor));
                currentPartBytes = 0;
            }
        }

        @Override
        public void close() throws IOException {
            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(key)
                .partNumber(partNumber)
                .contentLength((long) outputStream.size())
                .uploadId(multipartUpload.uploadId()).build();
            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            UploadPartResponse uploadPartResult = client.uploadPart(
                uploadPartRequest,
                RequestBody.fromInputStream(inputStream, uploadPartRequest.contentLength()));

            List<CompletedPart> completedParts;
            try {
                completedParts = CompletableFutures.allAsList(pendingUploads).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
            completedParts.add(CompletedPart.builder().partNumber(partNumber).eTag(uploadPartResult.eTag()).build());
            client.completeMultipartUpload(
                CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(multipartUpload.uploadId())
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                    .build());
            super.close();
        }
    }
}
