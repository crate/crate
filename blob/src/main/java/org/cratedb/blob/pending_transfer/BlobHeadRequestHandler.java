package org.cratedb.blob.pending_transfer;

import org.cratedb.blob.BlobTransferStatus;
import org.cratedb.blob.BlobTransferTarget;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.File;

public class BlobHeadRequestHandler {

    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final BlobTransferTarget blobTransferTarget;
    private final ClusterService clusterService;

    /**
     * this request handler contains handlers for both the source and target nodes
     */
    public static class Actions {
        // handlers called on the source node
        public static final String GET_BLOB_HEAD = "crate/blob/shard/tmp_transfer/get_head";
        public static final String GET_TRANSFER_INFO = "crate/blob/shard/tmp_transfer/get_info";

        // handlers called on the target node
        public static final String PUT_BLOB_HEAD_CHUNK = "crate/blob/shard/tmp_transfer/put_head_chunk";
    }

    @Inject
    public BlobHeadRequestHandler(TransportService transportService, ClusterService clusterService,
                                  BlobTransferTarget blobTransferTarget, ThreadPool threadPool) {
        this.blobTransferTarget = blobTransferTarget;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;
    }

    public void registerHandler() {
        transportService.registerHandler(Actions.GET_BLOB_HEAD, new GetBlobHeadHandler());
        transportService.registerHandler(Actions.GET_TRANSFER_INFO, new GetTransferInfoHandler());
        transportService.registerHandler(Actions.PUT_BLOB_HEAD_CHUNK, new PutBlobHeadChunkHandler());
    }

    private class GetBlobHeadHandler extends BaseTransportRequestHandler<GetBlobHeadRequest> {
        @Override
        public GetBlobHeadRequest newInstance() {
            return new GetBlobHeadRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

        /**
         * this is method is called on the recovery source node
         * the target is requesting the head of a file it got a PutReplicaChunkRequest for.
         */
        @Override
        public void messageReceived(final GetBlobHeadRequest request, TransportChannel channel) throws Exception {

            final BlobTransferStatus transferStatus = blobTransferTarget.getActiveTransfer(request.transferId);
            assert transferStatus != null :
                "Received GetBlobHeadRequest for transfer" + request.transferId.toString() + "but don't have an activeTransfer with that id";

            final DiscoveryNode recipientNode = clusterService.state().getNodes().get(request.senderNodeId);
            final File pendingFile = transferStatus.digestBlob().file();
            final long bytesToSend = request.endPos;

            blobTransferTarget.gotAGetBlobHeadRequest(request.transferId);

            channel.sendResponse(TransportResponse.Empty.INSTANCE);

            threadPool.generic().execute(
                new PutHeadChunkRunnable(
                    pendingFile, bytesToSend, transportService, blobTransferTarget,
                    recipientNode, request.transferId)
            );
        }
    }



    class PutBlobHeadChunkHandler extends BaseTransportRequestHandler<PutBlobHeadChunkRequest> {
        @Override
        public PutBlobHeadChunkRequest newInstance() {
            return new PutBlobHeadChunkRequest();
        }

        /**
         * called when the target node in a recovery receives a PutBlobHeadChunkRequest
         */
        @Override
        public void messageReceived(PutBlobHeadChunkRequest request, TransportChannel channel) throws Exception {
            BlobTransferStatus transferStatus = blobTransferTarget.getActiveTransfer(request.transferId);
            assert transferStatus != null;
            transferStatus.digestBlob().addToHead(request.content);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }
    }

    class GetTransferInfoHandler extends BaseTransportRequestHandler<BlobInfoRequest> {
        @Override
        public BlobInfoRequest newInstance() {
            return new BlobInfoRequest();
        }

        @Override
        public void messageReceived(BlobInfoRequest request, TransportChannel channel) throws Exception {
            final BlobTransferStatus transferStatus = blobTransferTarget.getActiveTransfer(request.transferId);
            assert transferStatus != null :
                "Received GetBlobHeadRequest for transfer" + request.transferId.toString() + "but don't have an activeTransfer with that id";

            BlobTransferInfoResponse response = new BlobTransferInfoResponse(
                transferStatus.index(),
                transferStatus.digestBlob().getDigest()
            );
            channel.sendResponse(response);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }
    }
}
