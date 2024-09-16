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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import io.crate.copy.s3.common.S3ClientHelper;
import io.crate.copy.s3.common.S3URI;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.engine.export.FileOutput;

import org.jetbrains.annotations.Nullable;
import io.crate.common.annotations.NotThreadSafe;
import io.crate.common.concurrent.CompletableFutures;

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

@NotThreadSafe
public class S3FileOutput implements FileOutput {

    private final URI uri;

    @Nullable
    private final String protocolSetting;

    public S3FileOutput(URI uri, String protocol) {
        this.uri = uri;
        protocolSetting = protocol;
    }

    @Override
    public OutputStream acquireOutputStream(Executor executor,WriterProjection.CompressionType compressionType) throws IOException {
        OutputStream outputStream = new S3OutputStream(executor, S3URI.toS3URI(uri), new S3ClientHelper(), protocolSetting);
        if (compressionType != null) {
            outputStream = new GZIPOutputStream(outputStream);
        }
        return outputStream;
    }


    private static class S3OutputStream extends OutputStream {

        private static final int PART_SIZE = 5 * 1024 * 1024;

        private final AmazonS3 client;
        private final InitiateMultipartUploadResult multipartUpload;
        private final Executor executor;
        private final String bucketName;
        private final String key;
        private final List<CompletableFuture<PartETag>> pendingUploads = new ArrayList<>();

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
            multipartUpload = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
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
                    UploadPartRequest uploadPartRequest = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(key)
                        .withPartNumber(currentPart)
                        .withPartSize(currentPartSize)
                        .withUploadId(multipartUpload.getUploadId())
                        .withInputStream(inputStream);
                    return client.uploadPart(uploadPartRequest).getPartETag();
                }, executor));
                currentPartBytes = 0;
            }
        }

        @Override
        public void close() throws IOException {
            UploadPartRequest uploadPartRequest = new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(key)
                .withPartNumber(partNumber)
                .withPartSize(outputStream.size())
                .withUploadId(multipartUpload.getUploadId())
                .withInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
            UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);

            List<PartETag> partETags;
            try {
                partETags = CompletableFutures.allAsList(pendingUploads).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
            partETags.add(uploadPartResult.getPartETag());
            client.completeMultipartUpload(
                new CompleteMultipartUploadRequest(
                    bucketName,
                    key,
                    multipartUpload.getUploadId(),
                    partETags)
            );
            super.close();
        }
    }
}
