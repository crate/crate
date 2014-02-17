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

package org.cratedb;

import org.cratedb.blob.BlobTransferTarget;
import org.cratedb.blob.pending_transfer.BlobHeadRequestHandler;
import org.cratedb.blob.pending_transfer.HeadChunkFileTooSmallException;
import org.cratedb.blob.pending_transfer.PutHeadChunkRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class BlobHeadRequestHandlerTests {

    protected ThreadPool threadPool;


    @Before
    public void setUp() throws Exception {
        threadPool = new ThreadPool();
    }

    @After
    public void tearDown() {
        threadPool.shutdown();
    }

    @Test
    public void testPutHeadChunkRunnableFileGrowth() throws Exception {

        File file = File.createTempFile("test", "");
        final FileOutputStream outputStream = new FileOutputStream(file);
        outputStream.write(new byte[] { 0x65 });

        UUID transferId = UUID.randomUUID();
        BlobTransferTarget blobTransferTarget = mock(BlobTransferTarget.class);
        TransportService transportService = mock(TransportService.class);
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);

        threadPool.schedule(TimeValue.timeValueMillis(800), ThreadPool.Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                try {
                    outputStream.write(new byte[] { 0x66, 0x67, 0x68, 0x69 });
                } catch (IOException ex) {
                    //pass
                }
            }
        });

        PutHeadChunkRunnable runnable = new PutHeadChunkRunnable(
            file, 5, transportService, blobTransferTarget, discoveryNode, transferId
        );

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
    }

    @Test(expected = HeadChunkFileTooSmallException.class)
    public void testPutHeadChunkRunnableFileDoesntGrow() throws Exception {
        // this test is rather slow, tune wait time in PutHeadChunkRunnable?

        File file = File.createTempFile("test", "");
        final FileOutputStream outputStream = new FileOutputStream(file);
        outputStream.write(new byte[] { 0x65 });

        UUID transferId = UUID.randomUUID();
        BlobTransferTarget transferTarget = mock(BlobTransferTarget.class);
        TransportService transportService = mock(TransportService.class);
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);

        PutHeadChunkRunnable runnable = new PutHeadChunkRunnable(
            file, 5, transportService, transferTarget, discoveryNode, transferId
        );

        TransportFuture<TransportResponse.Empty> result = mock(TransportFuture.class);
        when(transportService.submitRequest(
            eq(discoveryNode),
            eq(BlobHeadRequestHandler.Actions.PUT_BLOB_HEAD_CHUNK),
            any(TransportRequest.class),
            any(TransportRequestOptions.class),
            eq(EmptyTransportResponseHandler.INSTANCE_SAME)
        )).thenReturn(result);

        runnable.run();
    }
}
