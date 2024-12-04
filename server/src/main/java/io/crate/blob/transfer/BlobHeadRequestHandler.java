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

package io.crate.blob.transfer;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import io.crate.blob.BlobTransferStatus;
import io.crate.blob.BlobTransferTarget;

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
        public static final String GET_BLOB_HEAD = "internal:crate:blob/shard/tmp_transfer/get_head";
        public static final String GET_TRANSFER_INFO = "internal:crate:blob/shard/tmp_transfer/get_info";

        // handlers called on the target node
        public static final String PUT_BLOB_HEAD_CHUNK = "internal:crate:blob/shard/tmp_transfer/put_head_chunk";
    }

    public BlobHeadRequestHandler(TransportService transportService,
                                  ClusterService clusterService,
                                  BlobTransferTarget blobTransferTarget,
                                  ThreadPool threadPool) {
        this.blobTransferTarget = blobTransferTarget;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;
    }

    public void registerHandler() {
        transportService.registerRequestHandler(
            Actions.GET_BLOB_HEAD,
            ThreadPool.Names.GENERIC,
            GetBlobHeadRequest::new,
            new GetBlobHeadHandler()
        );
        transportService.registerRequestHandler(
            Actions.GET_TRANSFER_INFO,
            ThreadPool.Names.GENERIC,
            BlobInfoRequest::new,
            new GetTransferInfoHandler()
        );
        transportService.registerRequestHandler(
            Actions.PUT_BLOB_HEAD_CHUNK,
            ThreadPool.Names.GENERIC,
            PutBlobHeadChunkRequest::new,
            new PutBlobHeadChunkHandler()
        );
    }

    private class GetBlobHeadHandler implements TransportRequestHandler<GetBlobHeadRequest> {
        /**
         * this is method is called on the recovery source node
         * the target is requesting the head of a file it got a PutReplicaChunkRequest for.
         */
        @Override
        public void messageReceived(final GetBlobHeadRequest request, TransportChannel channel) throws Exception {

            final BlobTransferStatus transferStatus = blobTransferTarget.getActiveTransfer(request.transferId);
            assert transferStatus != null :
                "Received GetBlobHeadRequest for transfer" + request.transferId.toString() +
                "but don't have an activeTransfer with that id";

            final DiscoveryNode recipientNode = clusterService.state().nodes().get(request.senderNodeId);
            final long bytesToSend = request.endPos;

            blobTransferTarget.gotAGetBlobHeadRequest(request.transferId);

            channel.sendResponse(TransportResponse.Empty.INSTANCE);

            threadPool.generic().execute(
                new PutHeadChunkRunnable(
                    transferStatus.digestBlob(), bytesToSend, transportService, blobTransferTarget,
                    recipientNode, request.transferId)
            );
        }
    }


    private class PutBlobHeadChunkHandler implements TransportRequestHandler<PutBlobHeadChunkRequest> {
        /**
         * called when the target node in a recovery receives a PutBlobHeadChunkRequest
         */
        @Override
        public void messageReceived(PutBlobHeadChunkRequest request, TransportChannel channel) throws Exception {
            BlobTransferStatus transferStatus = blobTransferTarget.getActiveTransfer(request.transferId);
            assert transferStatus != null : "transferStatus should not be null";
            transferStatus.digestBlob().addToHead(request.content);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    private class GetTransferInfoHandler implements TransportRequestHandler<BlobInfoRequest> {
        @Override
        public void messageReceived(BlobInfoRequest request, TransportChannel channel) throws Exception {
            final BlobTransferStatus transferStatus = blobTransferTarget.getActiveTransfer(request.transferId);
            assert transferStatus != null :
                "Received GetBlobHeadRequest for transfer " + request.transferId.toString() +
                " but don't have an activeTransfer with that id";

            BlobTransferInfoResponse response = new BlobTransferInfoResponse(
                transferStatus.index(),
                transferStatus.digestBlob().getDigest()
            );
            channel.sendResponse(response);
        }
    }
}
