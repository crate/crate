package crate.elasticsearch.blob;

import crate.elasticsearch.blob.exceptions.DigestMismatchException;
import crate.elasticsearch.blob.pending_transfer.*;
import crate.elasticsearch.blob.v2.BlobIndices;
import crate.elasticsearch.blob.v2.BlobShard;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.*;

import java.io.File;
import java.util.concurrent.*;

public class BlobTransferTarget extends AbstractComponent {

    private final ConcurrentMap<UUID, BlobTransferStatus> activeTransfers =
        ConcurrentCollections.newConcurrentMap();

    private final BlobIndices blobIndices;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private CountDownLatch getHeadRequestLatch;
    private GenericBaseFuture<CountDownLatch> getHeadRequestLatchFuture;
    private final ConcurrentLinkedQueue activePutHeadChunkTransfers;
    private CountDownLatch activePutHeadChunkTransfersLatch;

    @Inject
    public BlobTransferTarget(Settings settings, BlobIndices blobIndices,
                              TransportService transportService, ClusterService clusterService) {
        super(settings);
        this.blobIndices = blobIndices;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.getHeadRequestLatchFuture = new GenericBaseFuture<CountDownLatch>();
        this.activePutHeadChunkTransfers = new ConcurrentLinkedQueue();
    }

    public BlobTransferStatus getActiveTransfer(UUID transferId) {
        return activeTransfers.get(transferId);
    }

    public void startTransfer(int shardId, StartBlobRequest request, StartBlobResponse response) {
        logger.info("startTransfer {} {}", request.transferId(), request.isLast());

        BlobShard blobShard = blobIndices.blobShardSafe(request.index(), shardId);
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
        logger.info("startTransfer finished {} {}", response.status(), response.size());
    }

    public void continueTransfer(PutChunkReplicaRequest request, PutChunkResponse response, int shardId) {
        BlobTransferStatus status = activeTransfers.get(request.transferId);
        if (status == null) {
            status = restoreTransferStatus(request, shardId);
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

    private BlobTransferStatus restoreTransferStatus(PutChunkReplicaRequest request, int shardId) {
        logger.trace("Restoring transferContext for PutChunkReplicaRequest with transferId {}",
            request.transferId);

        DiscoveryNode recipientNodeId = clusterService.state().getNodes().get(request.sourceNodeId);
        String senderNodeId = clusterService.localNode().getId();

        BlobTransferInfoResponse transferInfoResponse =
            (BlobTransferInfoResponse)transportService.submitRequest(
                recipientNodeId,
                BlobHeadRequestHandler.Actions.GET_TRANSFER_INFO,
                new BlobInfoRequest(senderNodeId, request.transferId),
                TransportRequestOptions.options(),
                new FutureTransportResponseHandler<TransportResponse>() {
                    @Override
                    public TransportResponse newInstance() {
                        return new BlobTransferInfoResponse();
                    }
                }
            ).txGet();

        BlobShard blobShard;
        try {
            blobShard = blobIndices.blobShardFuture(transferInfoResponse.index, shardId).get();
        } catch (InterruptedException e) {
            throw new TransferRestoreException("failure loading blobShard", request.transferId, e);
        } catch (ExecutionException e) {
            throw new TransferRestoreException("failure loading blobShard", request.transferId, e);
        }

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
            TransportRequestOptions.options(),
            EmptyTransportResponseHandler.INSTANCE_SAME
        ).txGet();
        return status;
    }

    private void addContent(IPutChunkRequest request, PutChunkResponse response, BlobTransferStatus status) {
        DigestBlob digestBlob = status.digestBlob();
        try {
            digestBlob.addContent(request.content(), request.isLast());
        } catch (BlobWriteException e) {
            activeTransfers.remove(status.transferId());
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
                activeTransfers.remove(status.transferId());
            }
            logger.info("transfer finished digest:{} status:{} size:{} chunks:{}",
                status.transferId(), response.status(), response.size(), digestBlob.chunks());
        } else {
            response.status(RemoteDigestBlob.Status.PARTIAL);
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
     *
     * The number of GetHeadRequests that are expected is  set when
     * {@link #createActiveTransfersSnapshot()} is called
     *
     * @param num
     * @param timeUnit
     */
    public void waitForGetHeadRequests(int num, TimeUnit timeUnit) {
        try {
            getHeadRequestLatch.await(num, timeUnit);
        } catch (InterruptedException e) {
            // pass
        }
    }

    public void waitUntilPutHeadChunksAreFinished() {
        try {
            activePutHeadChunkTransfersLatch.await();
        } catch (InterruptedException e) {
            // pass
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
            } catch (InterruptedException e) {
                logger.error("can't retrieve getHeadRequestLatch", e, null);
            } catch (ExecutionException e) {
                logger.error("can't retrieve getHeadRequestLatch", e, null);
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
}
