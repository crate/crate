/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.projectors.writer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.external.S3ClientHelper;
import io.crate.planner.projection.WriterProjection;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.zip.GZIPOutputStream;

@NotThreadSafe
public class OutputS3 extends Output {

    private final ExecutorService executorService;
    private final URI uri;
    private final boolean compression;

    public OutputS3(ExecutorService executorService, URI uri, WriterProjection.CompressionType compressionType) {
        this.executorService = executorService;
        this.uri = uri;
        compression = compressionType != null;
    }

    @Override
    public OutputStream acquireOutputStream() throws IOException {
        OutputStream outputStream = new S3OutputStream(executorService, uri, new S3ClientHelper());
        if (compression) {
            outputStream = new GZIPOutputStream(outputStream);
        }
        return outputStream;
    }


    private static class S3OutputStream extends OutputStream {

        private static final int PART_SIZE = 5 * 1024 * 1024;

        private final AmazonS3 client;
        private final InitiateMultipartUploadResult multipartUpload;
        private final String bucketName;
        private final String key;
        private final ListeningExecutorService executorService;
        private final List<PartETag> etags = Collections.<PartETag>synchronizedList(new ArrayList<PartETag>());
        private final List<ListenableFuture<?>> pendingUploads = new ArrayList<>();

        private ByteArrayOutputStream outputStream;
        long currentPartBytes = 0;
        int partNumber = 1;

        private S3OutputStream(ExecutorService executor, URI uri, S3ClientHelper s3ClientHelper) throws IOException {
            bucketName = uri.getHost();
            key = uri.getPath().substring(1);
            outputStream = new ByteArrayOutputStream();
            client = s3ClientHelper.client(uri);
            executorService = MoreExecutors.listeningDecorator(executor);
            multipartUpload = client.initiateMultipartUpload(
                new InitiateMultipartUploadRequest(bucketName, key));
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
                ListenableFuture<?> future = executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        UploadPartRequest uploadPartRequest = new UploadPartRequest()
                            .withBucketName(bucketName)
                            .withKey(key)
                            .withPartNumber(currentPart)
                            .withPartSize(currentPartSize)
                            .withUploadId(multipartUpload.getUploadId())
                            .withInputStream(inputStream);
                        UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
                        etags.add(uploadPartResult.getPartETag());
                    }
                });
                pendingUploads.add(future);
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
            etags.add(uploadPartResult.getPartETag());
            ListenableFuture<List<Object>> future = Futures.allAsList(pendingUploads);
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
            client.completeMultipartUpload(
                new CompleteMultipartUploadRequest(
                    bucketName,
                    key,
                    multipartUpload.getUploadId(),
                    etags)
            );
            super.close();
        }
    }
}
