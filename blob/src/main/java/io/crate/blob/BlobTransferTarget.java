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

package io.crate.blob;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.blob.exceptions.DigestMismatchException;
import io.crate.blob.pending_transfer.BlobHeadRequestHandler;
import io.crate.blob.pending_transfer.BlobInfoRequest;
import io.crate.blob.pending_transfer.BlobTransferInfoResponse;
import io.crate.blob.pending_transfer.GetBlobHeadRequest;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

public class BlobTransferTarget extends AbstractComponent {

    private final ConcurrentMap<UUID, BlobTransferStatus> activeTransfers =
        ConcurrentCollections.newConcurrentMap();

    private final BlobIndicesService blobIndicesService;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private CountDownLatch getHeadRequestLatch;
    private SettableFuture<CountDownLatch> getHeadRequestLatchFuture;
    private final ConcurrentLinkedQueue<UUID> activePutHeadChunkTransfers;
    private CountDownLatch activePutHeadChunkTransfersLatch;
    private volatile boolean recoveryActive = false;
    private final Object lock = new Object();
    private final List<UUID> finishedUploads = new ArrayList<>();
    private final TimeValue STATE_REMOVAL_DELAY;

    @Inject
    public BlobTransferTarget(Settings settings,
                              BlobIndicesService blobIndicesService,
                              ThreadPool threadPool,
                              TransportService transportService,
                              ClusterService clusterService) {
        super(settings);
        String property = System.getProperty("tests.short_timeouts");
        if (property == null) {
            STATE_REMOVAL_DELAY = new TimeValue(40, TimeUnit.SECONDS);
        } else {
            STATE_REMOVAL_DELAY = new TimeValue(2, TimeUnit.SECONDS);
        }
        this.blobIndicesService = blobIndicesService;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.getHeadRequestLatchFuture = SettableFuture.create();
        this.activePutHeadChunkTransfers = new ConcurrentLinkedQueue<>();
    }

    public BlobTransferStatus getActiveTransfer(UUID transferId) {
        return activeTransfers.get(transferId);
    }

    public void startTransfer(StartBlobRequest request, StartBlobResponse response) {
        logger.debug("startTransfer {} {}", request.transferId(), request.isLast());

        BlobShard blobShard = blobIndicesService.blobShardSafe(request.shardId());
        File existing = blobShard.blobContainer().getFile(request.id());
        long size = existing.length();
        if (existing.exists()) {
            // the file exists
            response.status(RemoteDigestBlob.Status.EXISTS);
            response.size(size);
            return;
        }

        DigestBlob digestBlob = blobShard.blobContainer().createBlob(request.id(), request.transferId());
        digestBlob.addContent(request.content(), request.isLast());

        response.size(digestBlob.size());
        if (request.isLast()) {
            try {
                digestBlob.commit();
                response.status(RemoteDigestBlob.Status.FULL);
            } catch (DigestMismatchException e) {
                response.status(RemoteDigestBlob.Status.MISMATCH);
            }
        } else {
            BlobTransferStatus status = new BlobTransferStatus(
                request.index(), request.transferId(), digestBlob
            );
            activeTransfers.put(request.transferId(), status);
            response.status(RemoteDigestBlob.Status.PARTIAL);
        }
        logger.debug("startTransfer finished {} {}", response.status(), response.size());
    }

    public void continueTransfer(PutChunkReplicaRequest request, PutChunkResponse response) {
        BlobTransferStatus status = activeTransfers.get(request.transferId);
        if (status == null) {
            status = restoreTransferStatus(request);
        }

        addContent(request, response, status);
    }

    public void continueTransfer(PutChunkRequest request, PutChunkResponse response) {
        BlobTransferStatus status = activeTransfers.get(request.transferId());
        if (status == null) {
            logger.error("No context for transfer: {} Dropping request", request.transferId());
            response.status(RemoteDigestBlob.Status.PARTIAL);
            return;
        }

        addContent(request, response, status);
    }

    private BlobTransferStatus restoreTransferStatus(PutChunkReplicaRequest request) {
        logger.trace("Restoring transferContext for PutChunkReplicaRequest with transferId {}",
            request.transferId);

        DiscoveryNodes nodes = clusterService.state().getNodes();
        DiscoveryNode recipientNodeId = nodes.get(request.sourceNodeId);
        String senderNodeId = nodes.getLocalNodeId();

        BlobTransferInfoResponse transferInfoResponse =
            (BlobTransferInfoResponse) transportService.submitRequest(
                recipientNodeId,
                BlobHeadRequestHandler.Actions.GET_TRANSFER_INFO,
                new BlobInfoRequest(senderNodeId, request.transferId),
                TransportRequestOptions.EMPTY,
                new FutureTransportResponseHandler<TransportResponse>() {
                    @Override
                    public TransportResponse newInstance() {
                        return new BlobTransferInfoResponse();
                    }
                }
            ).txGet();

        BlobShard blobShard = blobIndicesService.blobShardSafe(request.shardId());

        DigestBlob digestBlob = DigestBlob.resumeTransfer(
            blobShard.blobContainer(), transferInfoResponse.digest, request.transferId, request.currentPos
        );

        assert digestBlob != null : "DigestBlob couldn't be restored";

        BlobTransferStatus status;
        status = new BlobTransferStatus(transferInfoResponse.index, request.transferId, digestBlob);
        activeTransfers.put(request.transferId, status);
        logger.trace("Restored transferStatus for digest {} transferId: {}",
            transferInfoResponse.digest, request.transferId
        );

        transportService.submitRequest(
            recipientNodeId,
            BlobHeadRequestHandler.Actions.GET_BLOB_HEAD,
            new GetBlobHeadRequest(senderNodeId, request.transferId(), request.currentPos),
            TransportRequestOptions.EMPTY,
            EmptyTransportResponseHandler.INSTANCE_SAME
        ).txGet();
        return status;
    }

    private void addContent(IPutChunkRequest request, PutChunkResponse response, BlobTransferStatus status) {
        DigestBlob digestBlob = status.digestBlob();
        try {
            digestBlob.addContent(request.content(), request.isLast());
        } catch (BlobWriteException e) {
            IOUtils.closeWhileHandlingException(activeTransfers.remove(status.transferId()));
            throw e;
        }

        response.size(digestBlob.size());
        if (request.isLast()) {
            digestBlob.waitForHead();
            try {
                digestBlob.commit();
                response.status(RemoteDigestBlob.Status.FULL);
            } catch (DigestMismatchException e) {
                response.status(RemoteDigestBlob.Status.MISMATCH);
            } finally {
                removeTransferAfterRecovery(status.transferId());
            }
            logger.debug("transfer finished digest:{} status:{} size:{} chunks:{}",
                status.transferId(), response.status(), response.size(), digestBlob.chunks());
        } else {
            response.status(RemoteDigestBlob.Status.PARTIAL);
        }
    }

    private void removeTransferAfterRecovery(UUID transferId) {
        boolean toSchedule = false;
        synchronized (lock) {
            if (recoveryActive) {
                /**
                 * the recovery target node might request the transfer context. So it is
                 * necessary to keep the state until the recovery is done.
                 */
                finishedUploads.add(transferId);
            } else {
                toSchedule = true;
            }
        }
        if (toSchedule) {
            logger.debug("finished transfer {}, removing state", transferId);

            /**
             * there might be a race condition that the recoveryActive flag is still false although a
             * recovery has been started.
             *
             * delay the state removal a bit and re-check the recoveryActive flag in order to not remove
             * states which might still be needed.
             */
            threadPool.schedule(STATE_REMOVAL_DELAY, ThreadPool.Names.GENERIC, new StateRemoval(transferId));
        }
    }

    private class StateRemoval implements Runnable {

        private final UUID transferId;

        private StateRemoval(UUID transferId) {
            this.transferId = transferId;
        }

        @Override
        public void run() {
            synchronized (lock) {
                if (recoveryActive) {
                    finishedUploads.add(transferId);
                } else {
                    BlobTransferStatus transferStatus = activeTransfers.remove(transferId);
                    IOUtils.closeWhileHandlingException(transferStatus);
                }
            }
        }
    }

    /**
     * creates a countDownLatch from the activeTransfers.
     * This is the number of "GetHeadRequests" that are expected from the target node
     * The actual number of GetHeadRequests that will be received might be lower
     * than the number that is expected.
     */
    public void createActiveTransfersSnapshot() {
        getHeadRequestLatch = new CountDownLatch(activeTransfers.size());

        /**
         * the future is used because {@link #gotAGetBlobHeadRequest(org.elasticsearch.common.UUID)}
         * might be called before this method und there is a .get() call that blocks and waits
         */
        getHeadRequestLatchFuture.set(getHeadRequestLatch);
    }

    /**
     * wait until the expected number of GetHeadRequests was received or at most
     * num / timeUnit.
     * <p>
     * The number of GetHeadRequests that are expected is  set when
     * {@link #createActiveTransfersSnapshot()} is called
     */
    public void waitForGetHeadRequests(int num, TimeUnit timeUnit) {
        try {
            getHeadRequestLatch.await(num, timeUnit);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    public void waitUntilPutHeadChunksAreFinished() {
        try {
            activePutHeadChunkTransfersLatch.await();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    public void gotAGetBlobHeadRequest(UUID transferId) {
        /**
         * this method might be called before the getHeadRequestLatch is initialized
         * the future is used here to wait until it is initialized.
         */
        if (getHeadRequestLatch == null) {
            try {
                getHeadRequestLatch = getHeadRequestLatchFuture.get();
                activePutHeadChunkTransfers.add(transferId);
                getHeadRequestLatch.countDown();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("can't retrieve getHeadRequestLatch", e);
            }
        }
    }

    public void createActivePutHeadChunkTransfersSnapshot() {
        activePutHeadChunkTransfersLatch = new CountDownLatch(activePutHeadChunkTransfers.size());
    }

    public void putHeadChunkTransferFinished(UUID transferId) {
        activePutHeadChunkTransfers.remove(transferId);
        if (activePutHeadChunkTransfersLatch != null) {
            activePutHeadChunkTransfersLatch.countDown();
        }
    }

    public void startRecovery() {
        recoveryActive = true;
    }

    public void stopRecovery() {
        synchronized (lock) {
            recoveryActive = false;
            for (UUID finishedUpload : finishedUploads) {
                logger.debug("finished transfer and recovery for {}, removing state", finishedUpload);
                BlobTransferStatus transferStatus = activeTransfers.remove(finishedUpload);
                IOUtils.closeWhileHandlingException(transferStatus);
            }
        }
    }
}
