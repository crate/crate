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

package org.elasticsearch.indices.recovery;

import io.crate.blob.BlobTransferTarget;
import io.crate.blob.recovery.BlobRecoveryHandler;
import io.crate.blob.v2.BlobIndices;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.TransportService;
import com.google.common.collect.Iterables;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads.Interruptable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportRequestOptions;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RecoverySourceHandler handles the three phases of shard recovery, which is
 * everything relating to copying the segment files as well as sending translog
 * operations across the wire once the segments have been copied.
 *
 * Copied from RecoverySourceHandler, extends only phase1.
 *
 */
public class BlobRecoverySourceHandler extends RecoverySourceHandler {

    private final BlobRecoveryHandler blobRecoveryHandler;

    public BlobRecoverySourceHandler(final IndexShard shard,
                                     final StartRecoveryRequest request,
                                     final RecoverySettings recoverySettings,
                                     final TransportService transportService,
                                     final ClusterService clusterService,
                                     final IndicesService indicesService,
                                     final MappingUpdatedAction mappingUpdatedAction,
                                     final ESLogger logger,
                                     BlobTransferTarget blobTransferTarget,
                                     BlobIndices blobIndices) {
        super(shard, request, recoverySettings, transportService, clusterService, indicesService, mappingUpdatedAction, logger);

        if (BlobIndices.isBlobIndex(shard.shardId().getIndex())) {
            blobRecoveryHandler = new BlobRecoveryHandler(
                    transportService, recoverySettings, blobTransferTarget, blobIndices, shard, request);
        } else {
            blobRecoveryHandler = null;
        }
    }

    /**
     * Perform phase1 of the recovery operations. Once this {@link SnapshotIndexCommit}
     * snapshot has been performed no commit operations (files being fsync'd)
     * are effectively allowed on this index until all recovery phases are done
     *
     * Phase1 examines the segment files on the target node and copies over the
     * segments that are missing. Only segments that have the same size and
     * checksum can be reused
     *
     * {@code InternalEngine#recover} is responsible for snapshotting the index
     * and releasing the snapshot once all 3 phases of recovery are complete
     */
    @Override
    public void phase1(final SnapshotIndexCommit snapshot) throws ElasticsearchException {
        cancellableThreads.checkForCancel();
        // Total size of segment files that are recovered
        long totalSize = 0;
        // Total size of segment files that were able to be re-used
        long existingTotalSize = 0;
        final Store store = shard.store();
        store.incRef();
        try {
            if (blobRecoveryHandler !=null) {
                blobRecoveryHandler.phase1();
            }
            StopWatch stopWatch = new StopWatch().start();
            final Store.MetadataSnapshot recoverySourceMetadata = store.getMetadata(snapshot);
            for (String name : snapshot.getFiles()) {
                final StoreFileMetaData md = recoverySourceMetadata.get(name);
                if (md == null) {
                    logger.info("Snapshot differs from actual index for file: {} meta: {}", name, recoverySourceMetadata.asMap());
                    throw new CorruptIndexException("Snapshot differs from actual index - maybe index was removed metadata has " +
                            recoverySourceMetadata.asMap().size() + " files");
                }
            }
            // Generate a "diff" of all the identical, different, and missing
            // segment files on the target node, using the existing files on
            // the source node
            final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(new Store.MetadataSnapshot(request.existingFiles()));
            for (StoreFileMetaData md : diff.identical) {
                response.phase1ExistingFileNames.add(md.name());
                response.phase1ExistingFileSizes.add(md.length());
                existingTotalSize += md.length();
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}][{}] recovery [phase1] to {}: not recovering [{}], exists in local store and has checksum [{}], size [{}]",
                            indexName, shardId, request.targetNode(), md.name(), md.checksum(), md.length());
                }
                totalSize += md.length();
            }
            for (StoreFileMetaData md : Iterables.concat(diff.different, diff.missing)) {
                if (request.existingFiles().containsKey(md.name())) {
                    logger.trace("[{}][{}] recovery [phase1] to {}: recovering [{}], exists in local store, but is different: remote [{}], local [{}]",
                            indexName, shardId, request.targetNode(), md.name(), request.existingFiles().get(md.name()), md);
                } else {
                    logger.trace("[{}][{}] recovery [phase1] to {}: recovering [{}], does not exists in remote",
                            indexName, shardId, request.targetNode(), md.name());
                }
                response.phase1FileNames.add(md.name());
                response.phase1FileSizes.add(md.length());
                totalSize += md.length();
            }
            response.phase1TotalSize = totalSize;
            response.phase1ExistingTotalSize = existingTotalSize;

            logger.trace("[{}][{}] recovery [phase1] to {}: recovering_files [{}] with total_size [{}], reusing_files [{}] with total_size [{}]",
                    indexName, shardId, request.targetNode(), response.phase1FileNames.size(),
                    new ByteSizeValue(totalSize), response.phase1ExistingFileNames.size(), new ByteSizeValue(existingTotalSize));
            cancellableThreads.execute(new Interruptable() {
                @Override
                public void run() throws InterruptedException {
                    RecoveryFilesInfoRequest recoveryInfoFilesRequest = new RecoveryFilesInfoRequest(request.recoveryId(), request.shardId(),
                            response.phase1FileNames, response.phase1FileSizes, response.phase1ExistingFileNames, response.phase1ExistingFileSizes,
                            shard.translog().estimatedNumberOfOperations(),
                            response.phase1TotalSize, response.phase1ExistingTotalSize);
                    transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FILES_INFO, recoveryInfoFilesRequest,
                            TransportRequestOptions.options().withTimeout(recoverySettings.internalActionTimeout()),
                            EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                }
            });


            // This latch will be used to wait until all files have been transferred to the target node
            final CountDownLatch latch = new CountDownLatch(response.phase1FileNames.size());
            final CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();
            final AtomicReference<Throwable> corruptedEngine = new AtomicReference<>();
            int fileIndex = 0;
            ThreadPoolExecutor pool;

            // How many bytes we've copied since we last called RateLimiter.pause
            final AtomicLong bytesSinceLastPause = new AtomicLong();

            for (final String name : response.phase1FileNames) {
                long fileSize = response.phase1FileSizes.get(fileIndex);

                // Files are split into two categories, files that are "small"
                // (under 5mb) and other files. Small files are transferred
                // using a separate thread pool dedicated to small files.
                //
                // The idea behind this is that while we are transferring an
                // older, large index, a user may create a new index, but that
                // index will not be able to recover until the large index
                // finishes, by using two different thread pools we can allow
                // tiny files (like segments for a brand new index) to be
                // recovered while ongoing large segment recoveries are
                // happening. It also allows these pools to be configured
                // separately.
                if (fileSize > recoverySettings.SMALL_FILE_CUTOFF_BYTES) {
                    pool = recoverySettings.concurrentStreamPool();
                } else {
                    pool = recoverySettings.concurrentSmallFileStreamPool();
                }

                pool.execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Throwable t) {
                        // we either got rejected or the store can't be incremented / we are canceled
                        logger.debug("Failed to transfer file [" + name + "] on recovery");
                    }

                    public void onAfter() {
                        // Signify this file has completed by decrementing the latch
                        latch.countDown();
                    }

                    @Override
                    protected void doRun() {
                        cancellableThreads.checkForCancel();
                        store.incRef();
                        final StoreFileMetaData md = recoverySourceMetadata.get(name);
                        try (final IndexInput indexInput = store.directory().openInput(name, IOContext.READONCE)) {
                            final int BUFFER_SIZE = (int) recoverySettings.fileChunkSize().bytes();
                            final byte[] buf = new byte[BUFFER_SIZE];
                            boolean shouldCompressRequest = recoverySettings.compress();
                            if (CompressorFactory.isCompressed(indexInput)) {
                                shouldCompressRequest = false;
                            }

                            final long len = indexInput.length();
                            long readCount = 0;
                            final TransportRequestOptions requestOptions = TransportRequestOptions.options()
                                    .withCompress(shouldCompressRequest)
                                    .withType(TransportRequestOptions.Type.RECOVERY)
                                    .withTimeout(recoverySettings.internalActionTimeout());

                            while (readCount < len) {
                                if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                                    throw new IndexShardClosedException(shard.shardId());
                                }
                                int toRead = readCount + BUFFER_SIZE > len ? (int) (len - readCount) : BUFFER_SIZE;
                                final long position = indexInput.getFilePointer();

                                // Pause using the rate limiter, if desired, to throttle the recovery
                                RateLimiter rl = recoverySettings.rateLimiter();
                                long throttleTimeInNanos = 0;
                                if (rl != null) {
                                    long bytes = bytesSinceLastPause.addAndGet(toRead);
                                    if (bytes > rl.getMinPauseCheckBytes()) {
                                        // Time to pause
                                        bytesSinceLastPause.addAndGet(-bytes);
                                        throttleTimeInNanos = rl.pause(bytes);
                                        shard.recoveryStats().addThrottleTime(throttleTimeInNanos);
                                    }
                                }
                                indexInput.readBytes(buf, 0, toRead, false);
                                final BytesArray content = new BytesArray(buf, 0, toRead);
                                readCount += toRead;
                                final boolean lastChunk = readCount == len;
                                final RecoveryFileChunkRequest fileChunkRequest = new RecoveryFileChunkRequest(request.recoveryId(), request.shardId(), md, position,
                                        content, lastChunk,shard.translog().estimatedNumberOfOperations(), throttleTimeInNanos);
                                cancellableThreads.execute(new Interruptable() {
                                    @Override
                                    public void run() throws InterruptedException {
                                        // Actually send the file chunk to the target node, waiting for it to complete
                                        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FILE_CHUNK,
                                                fileChunkRequest, requestOptions, EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                                    }
                                });

                            }
                        } catch (Throwable e) {
                            final Throwable corruptIndexException;
                            if ((corruptIndexException = ExceptionsHelper.unwrapCorruption(e)) != null) {
                                if (store.checkIntegrity(md) == false) { // we are corrupted on the primary -- fail!
                                    logger.warn("{} Corrupted file detected {} checksum mismatch", shard.shardId(), md);
                                    if (corruptedEngine.compareAndSet(null, corruptIndexException) == false) {
                                        // if we are not the first exception, add ourselves as suppressed to the main one:
                                        corruptedEngine.get().addSuppressed(e);
                                    }
                                } else { // corruption has happened on the way to replica
                                    RemoteTransportException exception = new RemoteTransportException("File corruption occurred on recovery but checksums are ok", null);
                                    exception.addSuppressed(e);
                                    exceptions.add(0, exception); // last exception first
                                    logger.warn("{} Remote file corruption on node {}, recovering {}. local checksum OK",
                                            corruptIndexException, shard.shardId(), request.targetNode(), md);

                                }
                            } else {
                                exceptions.add(0, e); // last exceptions first
                            }
                        } finally {
                            store.decRef();

                        }
                    }
                });
                fileIndex++;
            }

            cancellableThreads.execute(new Interruptable() {
                @Override
                public void run() throws InterruptedException {
                    // Wait for all files that need to be transferred to finish transferring
                    latch.await();
                }
            });

            if (corruptedEngine.get() != null) {
                throw corruptedEngine.get();
            } else {
                ExceptionsHelper.rethrowAndSuppress(exceptions);
            }

            cancellableThreads.execute(new Interruptable() {
                @Override
                public void run() throws InterruptedException {
                    // Send the CLEAN_FILES request, which takes all of the files that
                    // were transferred and renames them from their temporary file
                    // names to the actual file names. It also writes checksums for
                    // the files after they have been renamed.
                    //
                    // Once the files have been renamed, any other files that are not
                    // related to this recovery (out of date segments, for example)
                    // are deleted
                    try {
                        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.CLEAN_FILES,
                                new RecoveryCleanFilesRequest(request.recoveryId(), shard.shardId(), recoverySourceMetadata, shard.translog().estimatedNumberOfOperations()),
                                TransportRequestOptions.options().withTimeout(recoverySettings.internalActionTimeout()),
                                EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                    } catch (RemoteTransportException remoteException) {
                        final IOException corruptIndexException;
                        // we realized that after the index was copied and we wanted to finalize the recovery
                        // the index was corrupted:
                        //   - maybe due to a broken segments file on an empty index (transferred with no checksum)
                        //   - maybe due to old segments without checksums or length only checks
                        if ((corruptIndexException = ExceptionsHelper.unwrapCorruption(remoteException)) != null) {
                            try {
                                final Store.MetadataSnapshot recoverySourceMetadata = store.getMetadata(snapshot);
                                StoreFileMetaData[] metadata = Iterables.toArray(recoverySourceMetadata, StoreFileMetaData.class);
                                ArrayUtil.timSort(metadata, new Comparator<StoreFileMetaData>() {
                                    @Override
                                    public int compare(StoreFileMetaData o1, StoreFileMetaData o2) {
                                        return Long.compare(o1.length(), o2.length()); // check small files first
                                    }
                                });
                                for (StoreFileMetaData md : metadata) {
                                    logger.debug("{} checking integrity for file {} after remove corruption exception", shard.shardId(), md);
                                    if (store.checkIntegrity(md) == false) { // we are corrupted on the primary -- fail!
                                        logger.warn("{} Corrupted file detected {} checksum mismatch", shard.shardId(), md);
                                        throw corruptIndexException;
                                    }
                                }
                            } catch (IOException ex) {
                                remoteException.addSuppressed(ex);
                                throw remoteException;
                            }
                            // corruption has happened on the way to replica
                            RemoteTransportException exception = new RemoteTransportException("File corruption occurred on recovery but checksums are ok", null);
                            exception.addSuppressed(remoteException);
                            logger.warn("{} Remote file corruption during finalization on node {}, recovering {}. local checksum OK",
                                    corruptIndexException, shard.shardId(), request.targetNode());
                        } else {
                            throw remoteException;
                        }
                    }
                }
            });
            stopWatch.stop();
            logger.trace("[{}][{}] recovery [phase1] to {}: took [{}]", indexName, shardId, request.targetNode(), stopWatch.totalTime());
            response.phase1Time = stopWatch.totalTime().millis();
        } catch (Throwable e) {
            throw new RecoverFilesRecoveryException(request.shardId(), response.phase1FileNames.size(), new ByteSizeValue(totalSize), e);
        } finally {
            store.decRef();
        }
    }

}

