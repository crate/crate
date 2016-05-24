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

package org.elasticsearch.indices.recovery;

import io.crate.blob.BlobWriteException;
import io.crate.blob.exceptions.IllegalBlobRecoveryStateException;
import io.crate.blob.v2.BlobShard;
import io.crate.common.Hex;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.File;
import java.io.FileOutputStream;


public class BlobRecoveryTarget extends AbstractComponent {

    /*
    * @startuml
    * title Sync Sequence
    * actor SourceNode as s
    * actor TargetNode as t
    *
    * group for every two char prefix
    * s -> t:StartPrefixSync(prefix)
    * t -> t:getDigests for prefix
    * t --> s:found digests
    * s -> s: get missing digests
    * group for every missing digest
    *  s -> t:BlobSyncStartRequest(transferId, digest, contents, totalsize)
    *  t -> s: ack
    *  s -> s: BlobSyncChunkRequest(transferid, contents, isLast)
    *  t -> t: if isLast move to final
    *  t -> s: ack
    * end
    * s -> t:FinishPrefixSync(deletableDigests)
    * t -> t: delete deletable digests
    * s -> t: ack
    * end
    *
    *
    * @enduml
    *
    * */

    private final ConcurrentMapLong<BlobRecoveryStatus> onGoingRecoveries = ConcurrentCollections.newConcurrentMapLong();
    private final RecoveryTarget indexRecoveryTarget;
    private final IndicesService indicesService;

    public static class Actions {
        public static final String FINALIZE_RECOVERY = "crate/blob/shard/recovery/finalize_recovery";
        public static final String DELETE_FILE = "crate/blob/shard/recovery/delete_file";
        public static final String START_RECOVERY = "crate/blob/shard/recovery/start";
        public static final String START_PREFIX = "crate/blob/shard/recovery/start_prefix";
        public static final String TRANSFER_CHUNK = "crate/blob/shard/recovery/transfer_chunk";
        public static final String START_TRANSFER = "crate/blob/shard/recovery/start_transfer";
    }

    @Inject
    public BlobRecoveryTarget(Settings settings, IndicesLifecycle indicesLifecycle, RecoveryTarget indexRecoveryTarget,
            IndicesService indicesService, TransportService transportService) {
        super(settings);
        this.indexRecoveryTarget = indexRecoveryTarget;
        this.indicesService = indicesService;

        transportService.registerRequestHandler(Actions.START_RECOVERY, BlobStartRecoveryRequest.class, ThreadPool.Names.GENERIC, new StartRecoveryRequestHandler());
        transportService.registerRequestHandler(Actions.START_PREFIX, BlobStartPrefixSyncRequest.class, ThreadPool.Names.GENERIC, new StartPrefixSyncRequestHandler());
        transportService.registerRequestHandler(Actions.TRANSFER_CHUNK, BlobRecoveryChunkRequest.class, ThreadPool.Names.GENERIC, new TransferChunkRequestHandler());
        transportService.registerRequestHandler(Actions.START_TRANSFER, BlobRecoveryStartTransferRequest.class, ThreadPool.Names.GENERIC, new StartTransferRequestHandler());
        transportService.registerRequestHandler(Actions.DELETE_FILE, BlobRecoveryDeleteRequest.class, ThreadPool.Names.GENERIC, new DeleteFileRequestHandler());
        transportService.registerRequestHandler(Actions.FINALIZE_RECOVERY, BlobFinalizeRecoveryRequest.class, ThreadPool.Names.GENERIC, new FinalizeRecoveryRequestHandler());
    }

    class StartRecoveryRequestHandler extends TransportRequestHandler<BlobStartRecoveryRequest> {
        @Override
        public void messageReceived(BlobStartRecoveryRequest request, TransportChannel channel) throws Exception {

            logger.info("[{}] StartRecoveryRequestHandler start recovery with recoveryId {}",
                request.shardId().getId(), request.recoveryId);

            try (RecoveriesCollection.StatusRef statusSafe = indexRecoveryTarget.onGoingRecoveries.getStatusSafe(
                    request.recoveryId(), request.shardId())) {
                RecoveryStatus onGoingIndexRecovery = statusSafe.status();

                if (onGoingIndexRecovery.CancellableThreads().isCancelled()) {
                    throw new IndexShardClosedException(request.shardId());
                }

                BlobShard blobShard = indicesService.indexServiceSafe(
                        onGoingIndexRecovery.shardId().getIndex()).shardInjectorSafe(
                        onGoingIndexRecovery.shardId().id()).getInstance(BlobShard.class);

                BlobRecoveryStatus status = new BlobRecoveryStatus(onGoingIndexRecovery, blobShard);
                onGoingRecoveries.put(request.recoveryId(), status);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }


    class TransferChunkRequestHandler extends TransportRequestHandler<BlobRecoveryChunkRequest> {
        @Override
        public void messageReceived(BlobRecoveryChunkRequest request, TransportChannel channel) throws Exception {

            BlobRecoveryStatus onGoingRecovery = onGoingRecoveries.get(request.recoveryId());
            if (onGoingRecovery == null) {
                 // shard is getting closed on us
                throw new IllegalBlobRecoveryStateException("Could not retrieve onGoingRecoveryStatus");
            }

            BlobRecoveryTransferStatus transferStatus = onGoingRecovery.onGoingTransfers().get(request.transferId());
            BlobShard shard = onGoingRecovery.blobShard;
            if (onGoingRecovery.canceled()) {
                onGoingRecovery.sentCanceledToSource();
                 throw new IndexShardClosedException(onGoingRecovery.shardId());
            }

            if (transferStatus == null) {
                throw new IndexShardClosedException(onGoingRecovery.shardId());
            }

            BytesReference content = request.content();
            if (!content.hasArray()) {
                content = content.toBytesArray();
            }
            transferStatus.outputStream().write(
                content.array(), content.arrayOffset(), content.length()
            );

            if (request.isLast()) {
                transferStatus.outputStream().close();
                File source = new File(shard.blobContainer().getBaseDirectory(),
                    transferStatus.sourcePath()
                );
                File target = new File(shard.blobContainer().getBaseDirectory(),
                    transferStatus.targetPath()
                );

                if (target.exists()) {
                    logger.info("target file {} exists already.", target.getName());
                    // this might happen on bad timing while recovering/relocating.
                    // noop
                } else {
                    if (!source.renameTo(target)) {
                        throw new BlobWriteException(target.getName(), target.length(), null);
                    }
                }

                onGoingRecovery.onGoingTransfers().remove(request.transferId());
            }

            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }


    class StartPrefixSyncRequestHandler extends TransportRequestHandler<BlobStartPrefixSyncRequest> {
        @Override
        public void messageReceived(BlobStartPrefixSyncRequest request, TransportChannel channel) throws Exception {
            BlobRecoveryStatus status = onGoingRecoveries.get(request.recoveryId());
            if (status == null) {
                throw new IllegalBlobRecoveryStateException(
                    "could not retrieve BlobRecoveryStatus"
                );
            }
            if (status.canceled()) {
                throw new IndexShardClosedException(status.shardId());
            }
            BlobStartPrefixResponse response = new BlobStartPrefixResponse();
            response.existingDigests = status.blobShard.currentDigests(request.prefix());
            channel.sendResponse(response);
        }
    }


    private class StartTransferRequestHandler extends TransportRequestHandler<BlobRecoveryStartTransferRequest> {
        @Override
        public void messageReceived(BlobRecoveryStartTransferRequest request, TransportChannel channel) throws Exception {
            BlobRecoveryStatus status = onGoingRecoveries.get(request.recoveryId());
            logger.debug("received BlobRecoveryStartTransferRequest for file {} with size {}",
                request.path(), request.size());
            if (status == null) {
                throw new IllegalBlobRecoveryStateException("Could not retrieve onGoingRecoveryStatus");
            }
            if (status.canceled()) {
                throw new IndexShardClosedException(status.shardId());
            }

            BlobShard shard = status.blobShard;
            String tmpPath = request.path() + "." + request.transferId();
            FileOutputStream outputStream = new FileOutputStream(
                new File(shard.blobContainer().getBaseDirectory(), tmpPath)
            );

            BytesReference content = request.content();
            if (!content.hasArray()) {
                content = content.toBytesArray();
            }
            outputStream.write(content.array(), content.arrayOffset(), content.length());

            if (request.size() == request.content().length()) {  // start request contains the whole file.
                outputStream.close();
                File source = new File(shard.blobContainer().getBaseDirectory(), tmpPath);
                File target = new File(shard.blobContainer().getBaseDirectory(), request.path());
                if (!target.exists()) {
                    if (!source.renameTo(target)) {
                        throw new IllegalBlobRecoveryStateException(
                            "couldn't rename file to " + request.path()
                        );
                    }
                }
            } else {
                BlobRecoveryTransferStatus transferStatus= new BlobRecoveryTransferStatus(
                    request.transferId(), outputStream, tmpPath, request.path()
                );
                status.onGoingTransfers().put(request.transferId(), transferStatus);
            }

            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    private class DeleteFileRequestHandler extends TransportRequestHandler<BlobRecoveryDeleteRequest> {
        @Override
        public void messageReceived(BlobRecoveryDeleteRequest request, TransportChannel channel) throws Exception {
            BlobRecoveryStatus status = onGoingRecoveries.get(request.recoveryId());
            if (status.canceled()) {
                throw new IndexShardClosedException(status.shardId());
            }
            for (BytesReference digest : request.digests) {
                status.blobShard.delete(Hex.encodeHexString(digest.toBytes()));
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    private class FinalizeRecoveryRequestHandler extends TransportRequestHandler<BlobFinalizeRecoveryRequest> {
        @Override
        public void messageReceived(BlobFinalizeRecoveryRequest request, TransportChannel channel) throws Exception {

            BlobRecoveryStatus status = onGoingRecoveries.get(request.recoveryId);

            for (BlobRecoveryTransferStatus transferStatus : status.onGoingTransfers().values()) {
                if (transferStatus.outputStream().getChannel().isOpen()) {
                    throw new IllegalBlobRecoveryStateException(
                        "File channel was left open for "
                    );
                }
            }
            onGoingRecoveries.remove(request.recoveryId);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }
}
