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

package io.crate.blob.recovery;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.PlainFuture;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.recovery.BlobFinalizeRecoveryRequest;
import org.elasticsearch.indices.recovery.BlobRecoveryChunkRequest;
import org.elasticsearch.indices.recovery.BlobRecoveryDeleteRequest;
import org.elasticsearch.indices.recovery.BlobRecoveryStartTransferRequest;
import org.elasticsearch.indices.recovery.BlobRecoveryTarget;
import org.elasticsearch.indices.recovery.BlobStartPrefixResponse;
import org.elasticsearch.indices.recovery.BlobStartPrefixSyncRequest;
import org.elasticsearch.indices.recovery.BlobStartRecoveryRequest;
import org.elasticsearch.indices.recovery.RecoverySourceHandler;
import org.elasticsearch.indices.recovery.RecoveryTargetHandler;
import org.elasticsearch.indices.recovery.StartRecoveryRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import io.crate.blob.BlobContainer;
import io.crate.blob.BlobTransferTarget;
import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;
import io.crate.common.Hex;

public class BlobRecoveryHandler extends RecoverySourceHandler {

    private static final Logger LOGGER = LogManager.getLogger(BlobRecoveryHandler.class);
    private final StartRecoveryRequest request;
    private final TransportService transportService;
    private final BlobShard blobShard;
    private final IndexShard shard;
    private final BlobTransferTarget blobTransferTarget;
    private final int GET_HEAD_TIMEOUT;


    public BlobRecoveryHandler(IndexShard shard,
                               RecoveryTargetHandler recoveryTarget,
                               StartRecoveryRequest request,
                               int fileChunkSizeInBytes,
                               int maxConcurrentFileChunks,
                               int maxConcurrentOperations,
                               final TransportService transportService,
                               BlobTransferTarget blobTransferTarget,
                               BlobIndicesService blobIndicesService) {
        super(
            shard,
            recoveryTarget,
            shard.getThreadPool(),
            request,
            fileChunkSizeInBytes,
            maxConcurrentFileChunks,
            maxConcurrentOperations
        );
        assert BlobIndex.isBlobIndex(shard.shardId().getIndexName()) : "Shard must belong to a blob index";
        this.blobShard = blobIndicesService.blobShardSafe(request.shardId());
        this.request = request;
        this.transportService = transportService;
        this.blobTransferTarget = blobTransferTarget;
        this.shard = shard;
        String property = System.getProperty("tests.short_timeouts");
        if (property == null) {
            GET_HEAD_TIMEOUT = 30;
        } else {
            GET_HEAD_TIMEOUT = 2;
        }
    }

    private Set<BytesArray> getExistingDigestsFromTarget(byte prefix) {
        var listener = new PlainFuture<BlobStartPrefixResponse>();
        transportService.sendRequest(
            request.targetNode(),
            BlobRecoveryTarget.Actions.START_PREFIX,
            new BlobStartPrefixSyncRequest(request.recoveryId(), request.shardId(), prefix),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, BlobStartPrefixResponse::new)
        );
        BlobStartPrefixResponse response = FutureUtils.get(listener);

        Set<BytesArray> result = new HashSet<>();
        for (byte[] digests : response.existingDigests) {
            result.add(new BytesArray(digests));
        }
        return result;
    }

    @Override
    protected void blobRecoveryHook() throws Exception {
        LOGGER.debug("[{}][{}] recovery [phase1] to {}: start",
                     request.shardId().getIndexName(), request.shardId().id(), request.targetNode().getName());
        final StopWatch stopWatch = new StopWatch().start();
        blobTransferTarget.startRecovery();
        blobTransferTarget.createActiveTransfersSnapshot();
        sendStartRecoveryRequest();

        final AtomicReference<Exception> lastException = new AtomicReference<>();
        try {
            syncVarFiles(lastException);
        } catch (InterruptedException ex) {
            throw new ElasticsearchException("blob recovery phase1 failed", ex);
        }

        Exception exception = lastException.get();
        if (exception != null) {
            throw exception;
        }

        /*
          as soon as the recovery starts the target node will receive PutChunkReplicaRequests
          the target node will then request the bytes it is missing from the source node
          (it is missing bytes from PutChunk/StartBlob requests that happened before the recovery)
          here we need to block so that the target node has enough time to request the head chunks

          e.g.
               Target Node receives Chunk X with bytes 10-19
               Target Node requests bytes 0-9 from Source Node
               Source Node sends bytes 0-9
               Source Node sets transferTakenOver
         */

        blobTransferTarget.waitForGetHeadRequests(GET_HEAD_TIMEOUT, TimeUnit.SECONDS);
        blobTransferTarget.createActivePutHeadChunkTransfersSnapshot();

        /*
          After receiving a getHeadRequest the source node starts to send HeadChunks to the target
          wait for all PutHeadChunk-Runnables to finish before ending the recovery.
         */
        blobTransferTarget.waitUntilPutHeadChunksAreFinished();
        sendFinalizeRecoveryRequest();

        blobTransferTarget.stopRecovery();
        stopWatch.stop();
        LOGGER.debug("[{}][{}] recovery [phase1] to {}: took [{}]",
                     request.shardId().getIndexName(), request.shardId().id(), request.targetNode().getName(),
                     stopWatch.totalTime());
    }

    private void syncVarFiles(AtomicReference<Exception> lastException) throws InterruptedException, IOException {
        for (byte prefix : BlobContainer.PREFIXES) {
            // byte[1] and byte[1] have different hashCodes
            // so setA.removeAll(setB) wouldn't work with byte[], that's why BytesArray is used here
            Set<BytesArray> remoteDigests = getExistingDigestsFromTarget(prefix);
            Set<BytesArray> localDigests = new HashSet<>();
            for (byte[] digest : blobShard.currentDigests(prefix)) {
                localDigests.add(new BytesArray(digest));
            }

            Set<BytesArray> localButNotRemoteDigests = new HashSet<>(localDigests);
            localButNotRemoteDigests.removeAll(remoteDigests);

            final CountDownLatch latch = new CountDownLatch(localButNotRemoteDigests.size());
            for (BytesArray digestBytes : localButNotRemoteDigests) {
                final String digest = Hex.encodeHexString(BytesReference.toBytes(digestBytes));
                LOGGER.trace("[{}][{}] start to transfer file var/{} to {}",
                             request.shardId().getIndexName(), request.shardId().id(), digest,
                             request.targetNode().getName());
                cancellableThreads.execute(
                    new TransferFileRunnable(blobShard.blobContainer().getFile(digest),
                        lastException, latch)
                );
            }
            latch.await();

            remoteDigests.removeAll(localDigests);
            if (!remoteDigests.isEmpty()) {
                deleteFilesRequest(remoteDigests.toArray(new BytesArray[remoteDigests.size()]));
            }
        }
    }

    private void deleteFilesRequest(BytesArray[] digests) {
        var listener = new PlainFuture<TransportResponse>();
        transportService.sendRequest(
            request.targetNode(),
            BlobRecoveryTarget.Actions.DELETE_FILE,
            new BlobRecoveryDeleteRequest(request.recoveryId(), digests),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, in -> TransportResponse.Empty.INSTANCE)
        );
        FutureUtils.get(listener);
    }

    private void sendFinalizeRecoveryRequest() {
        var listener = new PlainFuture<TransportResponse>();
        transportService.sendRequest(
            request.targetNode(),
            BlobRecoveryTarget.Actions.FINALIZE_RECOVERY,
            new BlobFinalizeRecoveryRequest(request.recoveryId()),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, in -> TransportResponse.Empty.INSTANCE)
        );
        FutureUtils.get(listener);
    }

    private void sendStartRecoveryRequest() {
        var listener = new PlainFuture<TransportResponse>();
        transportService.sendRequest(
            request.targetNode(),
            BlobRecoveryTarget.Actions.START_RECOVERY,
            new BlobStartRecoveryRequest(request.recoveryId(), request.shardId()),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, in -> TransportResponse.Empty.INSTANCE)
        );
        FutureUtils.get(listener);
    }

    private class TransferFileRunnable implements CancellableThreads.Interruptable {
        private final AtomicReference<Exception> lastException;
        private final String baseDir;
        private final File file;
        private final CountDownLatch latch;

        TransferFileRunnable(File filePath, AtomicReference<Exception> lastException, CountDownLatch latch) {
            this.file = filePath;
            this.lastException = lastException;
            this.latch = latch;
            this.baseDir = blobShard.blobContainer().getBaseDirectory().toAbsolutePath().toString();
        }

        @Override
        public void run() {

            try {
                final int BUFFER_SIZE = 4 * 4096;

                long fileSize = file.length();

                if (fileSize == 0) {
                    LOGGER.warn("[{}][{}] empty file: {}",
                                request.shardId().getIndexName(), request.shardId().id(), file.getName());
                }

                try (FileInputStream fileStream = new FileInputStream(file)) {
                    String filePath = file.getAbsolutePath();
                    String relPath = filePath.substring(baseDir.length() + 1, filePath.length());
                    assert !relPath.startsWith("/") : "relPath must not start with a /";
                    byte[] buf = new byte[BUFFER_SIZE];
                    int bytesRead = fileStream.read(buf, 0, BUFFER_SIZE);
                    long bytesReadTotal = 0;
                    BytesArray content = new BytesArray(buf, 0, bytesRead);
                    BlobRecoveryStartTransferRequest startTransferRequest =
                        new BlobRecoveryStartTransferRequest(request.recoveryId(), relPath, content,
                            fileSize
                        );

                    if (bytesRead > 0) {
                        bytesReadTotal += bytesRead;

                        LOGGER.trace("[{}][{}] send BlobRecoveryStartTransferRequest to {} for file {} with size {}",
                                     request.shardId().getIndexName(), request.shardId().id(),
                                     request.targetNode().getName(),
                                     relPath,
                                     fileSize
                        );
                        var listener = new PlainFuture<>();
                        transportService.sendRequest(
                            request.targetNode(),
                            BlobRecoveryTarget.Actions.START_TRANSFER,
                            startTransferRequest,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(listener, in -> TransportResponse.Empty.INSTANCE)
                        );
                        FutureUtils.get(listener);

                        boolean isLast = false;
                        boolean sentChunks = false;
                        while ((bytesRead = fileStream.read(buf, 0, BUFFER_SIZE)) > 0) {

                            sentChunks = true;
                            bytesReadTotal += bytesRead;

                            if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                                throw new IndexShardClosedException(shard.shardId());
                            }
                            if (bytesReadTotal == fileSize) {
                                isLast = true;
                            }
                            content = new BytesArray(buf, 0, bytesRead);

                            var transferChunkListener = new PlainFuture<>();
                            transportService.sendRequest(
                                request.targetNode(),
                                BlobRecoveryTarget.Actions.TRANSFER_CHUNK,
                                new BlobRecoveryChunkRequest(request.recoveryId(),
                                    startTransferRequest.transferId(), content, isLast),
                                TransportRequestOptions.EMPTY,
                                new ActionListenerResponseHandler<>(transferChunkListener, in -> TransportResponse.Empty.INSTANCE)
                            );
                            FutureUtils.get(transferChunkListener);
                        }

                        if (!isLast && sentChunks) {
                            LOGGER.error("Sending isLast because it wasn't sent before for {}", relPath);
                            var transferMissingChunkListener = new PlainFuture<>();
                            transportService.sendRequest(
                                request.targetNode(),
                                BlobRecoveryTarget.Actions.TRANSFER_CHUNK,
                                new BlobRecoveryChunkRequest(request.recoveryId(),
                                    startTransferRequest.transferId(), BytesArray.EMPTY, true),
                                TransportRequestOptions.EMPTY,
                                new ActionListenerResponseHandler<>(transferMissingChunkListener, in -> TransportResponse.Empty.INSTANCE)
                            );
                            FutureUtils.get(transferMissingChunkListener);
                        }
                    }

                    LOGGER.trace("[{}][{}] completed to transfer file {} to {}",
                                 request.shardId().getIndexName(), request.shardId().id(), file.getName(),
                                 request.targetNode().getName());
                }
            } catch (IOException ex) {
                LOGGER.error("exception while file transfer", ex);
                lastException.set(ex);
            } finally {
                latch.countDown();
            }
        }
    }
}
