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

package io.crate;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.blob.BlobTransferTarget;
import io.crate.blob.DigestBlob;
import io.crate.blob.transfer.HeadChunkFileTooSmallException;
import io.crate.blob.transfer.PutHeadChunkRunnable;

public class BlobHeadRequestHandlerTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;

    @Before
    public void setUpResources() throws Exception {
        threadPool = new TestThreadPool(getClass().getName());
        var transport = new CapturingTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                super.onSendRequest(requestId, action, request, node);
                handleResponse(requestId, TransportResponse.Empty.INSTANCE);
            }
        };
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            boundAddress -> clusterService.localNode(),
            null
        );
        transportService.start();
        transportService.acceptIncomingRequests();
    }

    @After
    public void cleanUp() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPutHeadChunkRunnableFileGrowth() throws Exception {
        File file = File.createTempFile("test", "");

        try (final FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(new byte[]{0x65});

            UUID transferId = UUID.randomUUID();
            BlobTransferTarget blobTransferTarget = mock(BlobTransferTarget.class);
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
                DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
                PutHeadChunkRunnable runnable = new PutHeadChunkRunnable(
                    digestBlob, 5, transportService, blobTransferTarget, node1, transferId
                );

                runnable.run();

                verify(blobTransferTarget).putHeadChunkTransferFinished(transferId);
            } finally {
                scheduledExecutor.awaitTermination(1, TimeUnit.SECONDS);
                scheduledExecutor.shutdownNow();
            }
        }
    }

    @Test
    public void testPutHeadChunkRunnableFileDoesntGrow() throws Exception {
        // this test is rather slow, tune wait time in PutHeadChunkRunnable?

        File file = File.createTempFile("test", "");
        File notExisting = new File("./does/not/exist");
        try (final FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(new byte[]{0x65});
        }
        UUID transferId = UUID.randomUUID();
        BlobTransferTarget transferTarget = mock(BlobTransferTarget.class);

        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);

        DigestBlob digestBlob = mock(DigestBlob.class);
        when(digestBlob.file()).thenReturn(notExisting);
        when(digestBlob.getContainerFile()).thenReturn(file);
        PutHeadChunkRunnable runnable = new PutHeadChunkRunnable(
            digestBlob, 5, transportService, transferTarget, node1, transferId
        );

        assertThatThrownBy(() -> runnable.run())
            .isExactlyInstanceOf(HeadChunkFileTooSmallException.class);
        verify(digestBlob).getContainerFile();
    }
}
