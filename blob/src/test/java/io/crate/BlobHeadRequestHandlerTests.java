/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate;

import io.crate.blob.BlobTransferTarget;
import io.crate.blob.DigestBlob;
import io.crate.blob.transfer.BlobHeadRequestHandler;
import io.crate.blob.transfer.HeadChunkFileTooSmallException;
import io.crate.blob.transfer.PutHeadChunkRunnable;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportFuture;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BlobHeadRequestHandlerTests extends CrateUnitTest {

    @Test
    public void testPutHeadChunkRunnableFileGrowth() throws Exception {
        File file = File.createTempFile("test", "");

        try (final FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(new byte[]{0x65});

            UUID transferId = UUID.randomUUID();
            BlobTransferTarget blobTransferTarget = mock(BlobTransferTarget.class);
            TransportService transportService = mock(TransportService.class);
            DiscoveryNode discoveryNode = mock(DiscoveryNode.class);
            DigestBlob digestBlob = mock(DigestBlob.class);
            when(digestBlob.file()).thenReturn(file);

            ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(EsExecutors.daemonThreadFactory("blob-head"));
            try {
                scheduledExecutor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            outputStream.write(new byte[]{0x66, 0x67, 0x68, 0x69});
                        } catch (IOException ex) {
                            //pass
                        }
                    }
                }, 800, TimeUnit.MILLISECONDS);
                PutHeadChunkRunnable runnable = new PutHeadChunkRunnable(
                    digestBlob, 5, transportService, blobTransferTarget, discoveryNode, transferId
                );

                @SuppressWarnings("unchecked")
                TransportFuture<TransportResponse.Empty> result = mock(TransportFuture.class);

                when(transportService.submitRequest(
                    eq(discoveryNode),
                    eq(BlobHeadRequestHandler.Actions.PUT_BLOB_HEAD_CHUNK),
                    any(TransportRequest.class),
                    any(TransportRequestOptions.class),
                    eq(EmptyTransportResponseHandler.INSTANCE_SAME)
                )).thenReturn(result);

                runnable.run();

                verify(blobTransferTarget).putHeadChunkTransferFinished(transferId);
                verify(transportService, times(2)).submitRequest(
                    eq(discoveryNode),
                    eq(BlobHeadRequestHandler.Actions.PUT_BLOB_HEAD_CHUNK),
                    any(TransportRequest.class),
                    any(TransportRequestOptions.class),
                    eq(EmptyTransportResponseHandler.INSTANCE_SAME)
                );
            } finally {
                scheduledExecutor.awaitTermination(1, TimeUnit.SECONDS);
                scheduledExecutor.shutdownNow();
            }
        }
    }

    @Test
    public void testPutHeadChunkRunnableFileDoesntGrow() throws Exception {
        // this test is rather slow, tune wait time in PutHeadChunkRunnable?
        expectedException.expect(HeadChunkFileTooSmallException.class);

        File file = File.createTempFile("test", "");
        File notExisting = new File("./does/not/exist");
        try (final FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(new byte[]{0x65});
        }
        UUID transferId = UUID.randomUUID();
        BlobTransferTarget transferTarget = mock(BlobTransferTarget.class);
        TransportService transportService = mock(TransportService.class);
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);

        DigestBlob digestBlob = mock(DigestBlob.class);
        when(digestBlob.file()).thenReturn(notExisting);
        when(digestBlob.getContainerFile()).thenReturn(file);
        PutHeadChunkRunnable runnable = new PutHeadChunkRunnable(
            digestBlob, 5, transportService, transferTarget, discoveryNode, transferId
        );

        @SuppressWarnings("unchecked")
        TransportFuture<TransportResponse.Empty> result = mock(TransportFuture.class);
        when(transportService.submitRequest(
            eq(discoveryNode),
            eq(BlobHeadRequestHandler.Actions.PUT_BLOB_HEAD_CHUNK),
            any(TransportRequest.class),
            any(TransportRequestOptions.class),
            eq(EmptyTransportResponseHandler.INSTANCE_SAME)
        )).thenReturn(result);

        runnable.run();
        verify(digestBlob).getContainerFile();
    }
}
