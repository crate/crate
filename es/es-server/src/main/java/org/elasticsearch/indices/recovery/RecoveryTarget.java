/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotRecoveringException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * Represents a recovery where the current node is the target node of the recovery. To track recoveries in a central place, instances of
 * this class are created through {@link RecoveriesCollection}.
 */
public class RecoveryTarget extends AbstractRefCounted implements RecoveryTargetHandler {

    private final Logger logger;

    private static final AtomicLong idGenerator = new AtomicLong();

    private static final String RECOVERY_PREFIX = "recovery.";

    private final ShardId shardId;
    private final long recoveryId;
    private final IndexShard indexShard;
    private final DiscoveryNode sourceNode;
    private final String tempFilePrefix;
    private final Store store;
    private final PeerRecoveryTargetService.RecoveryListener listener;
    private final LongConsumer ensureClusterStateVersionCallback;

    private final AtomicBoolean finished = new AtomicBoolean();

    private final ConcurrentMap<String, IndexOutput> openIndexOutputs = ConcurrentCollections.newConcurrentMap();
    private final CancellableThreads cancellableThreads;

    // last time this status was accessed
    private volatile long lastAccessTime = System.nanoTime();

    // latch that can be used to blockingly wait for RecoveryTarget to be closed
    private final CountDownLatch closedLatch = new CountDownLatch(1);

    private final Map<String, String> tempFileNames = ConcurrentCollections.newConcurrentMap();

    /**
     * Creates a new recovery target object that represents a recovery to the provided shard.
     *
     * @param indexShard                        local shard where we want to recover to
     * @param sourceNode                        source node of the recovery where we recover from
     * @param listener                          called when recovery is completed/failed
     * @param ensureClusterStateVersionCallback callback to ensure that the current node is at least on a cluster state with the provided
     *                                          version; necessary for primary relocation so that new primary knows about all other ongoing
     *                                          replica recoveries when replicating documents (see {@link RecoverySourceHandler})
     */
    public RecoveryTarget(final IndexShard indexShard,
                   final DiscoveryNode sourceNode,
                   final PeerRecoveryTargetService.RecoveryListener listener,
                   final LongConsumer ensureClusterStateVersionCallback) {
        super("recovery_status");
        this.cancellableThreads = new CancellableThreads();
        this.recoveryId = idGenerator.incrementAndGet();
        this.listener = listener;
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.sourceNode = sourceNode;
        this.shardId = indexShard.shardId();
        this.tempFilePrefix = RECOVERY_PREFIX + UUIDs.randomBase64UUID() + ".";
        this.store = indexShard.store();
        this.ensureClusterStateVersionCallback = ensureClusterStateVersionCallback;
        // make sure the store is not released until we are done.
        store.incRef();
        indexShard.recoveryStats().incCurrentAsTarget();
    }

    /**
     * Returns a fresh recovery target to retry recovery from the same source node onto the same shard and using the same listener.
     *
     * @return a copy of this recovery target
     */
    public RecoveryTarget retryCopy() {
        return new RecoveryTarget(indexShard, sourceNode, listener, ensureClusterStateVersionCallback);
    }

    public long recoveryId() {
        return recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public IndexShard indexShard() {
        ensureRefCount();
        return indexShard;
    }

    public DiscoveryNode sourceNode() {
        return this.sourceNode;
    }

    public RecoveryState state() {
        return indexShard.recoveryState();
    }

    public CancellableThreads cancellableThreads() {
        return cancellableThreads;
    }

    /** return the last time this RecoveryStatus was used (based on System.nanoTime() */
    public long lastAccessTime() {
        return lastAccessTime;
    }

    /** sets the lasAccessTime flag to now */
    public void setLastAccessTime() {
        lastAccessTime = System.nanoTime();
    }

    public Store store() {
        ensureRefCount();
        return store;
    }

    public RecoveryState.Stage stage() {
        return state().getStage();
    }

    /** renames all temporary files to their true name, potentially overriding existing files */
    public void renameAllTempFiles() throws IOException {
        ensureRefCount();
        store.renameTempFilesSafe(tempFileNames);
    }

    /**
     * Closes the current recovery target and waits up to a certain timeout for resources to be freed.
     * Returns true if resetting the recovery was successful, false if the recovery target is already cancelled / failed or marked as done.
     */
    boolean resetRecovery(CancellableThreads newTargetCancellableThreads) throws IOException {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("reset of recovery with shard {} and id [{}]", shardId, recoveryId);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now.
                decRef();
            }
            try {
                newTargetCancellableThreads.execute(closedLatch::await);
            } catch (CancellableThreads.ExecutionCancelledException e) {
                logger.trace("new recovery target cancelled for shard {} while waiting on old recovery target with id [{}] to close",
                    shardId, recoveryId);
                return false;
            }
            RecoveryState.Stage stage = indexShard.recoveryState().getStage();
            if (indexShard.recoveryState().getPrimary() && (stage == RecoveryState.Stage.FINALIZE || stage == RecoveryState.Stage.DONE)) {
                // once primary relocation has moved past the finalization step, the relocation source can put the target into primary mode
                // and start indexing as primary into the target shard (see TransportReplicationAction). Resetting the target shard in this
                // state could mean that indexing is halted until the recovery retry attempt is completed and could also destroy existing
                // documents indexed and acknowledged before the reset.
                assert stage != RecoveryState.Stage.DONE : "recovery should not have completed when it's being reset";
                throw new IllegalStateException("cannot reset recovery as previous attempt made it past finalization step");
            }
            indexShard.performRecoveryRestart();
            return true;
        }
        return false;
    }

    /**
     * cancel the recovery. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     * <p>
     * if {@link #cancellableThreads()} was used, the threads will be interrupted.
     */
    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("recovery canceled (reason: [{}])", reason);
                cancellableThreads.cancel(reason);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
        }
    }

    /**
     * fail the recovery and call listener
     *
     * @param e                exception that encapsulating the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     */
    public void fail(RecoveryFailedException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                notifyListener(e, sendShardFailure);
            } finally {
                try {
                    cancellableThreads.cancel("failed recovery [" + ExceptionsHelper.stackTrace(e) + "]");
                } finally {
                    // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                    decRef();
                }
            }
        }
    }

    public void notifyListener(RecoveryFailedException e, boolean sendShardFailure) {
        listener.onRecoveryFailure(state(), e, sendShardFailure);
    }

    /** mark the current recovery as done */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            assert tempFileNames.isEmpty() : "not all temporary files are renamed";
            try {
                // this might still throw an exception ie. if the shard is CLOSED due to some other event.
                // it's safer to decrement the reference in a try finally here.
                indexShard.postRecovery("peer recovery done");
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
            listener.onRecoveryDone(state());
        }
    }

    /** Get a temporary name for the provided file name. */
    public String getTempNameForFile(String origFile) {
        return tempFilePrefix + origFile;
    }

    public IndexOutput getOpenIndexOutput(String key) {
        ensureRefCount();
        return openIndexOutputs.get(key);
    }

    /** remove and {@link org.apache.lucene.store.IndexOutput} for a given file. It is the caller's responsibility to close it */
    public IndexOutput removeOpenIndexOutputs(String name) {
        ensureRefCount();
        return openIndexOutputs.remove(name);
    }

    /**
     * Creates an {@link org.apache.lucene.store.IndexOutput} for the given file name. Note that the
     * IndexOutput actually point at a temporary file.
     * <p>
     * Note: You can use {@link #getOpenIndexOutput(String)} with the same filename to retrieve the same IndexOutput
     * at a later stage
     */
    public IndexOutput openAndPutIndexOutput(String fileName, StoreFileMetaData metaData, Store store) throws IOException {
        ensureRefCount();
        String tempFileName = getTempNameForFile(fileName);
        if (tempFileNames.containsKey(tempFileName)) {
            throw new IllegalStateException("output for file [" + fileName + "] has already been created");
        }
        // add first, before it's created
        tempFileNames.put(tempFileName, fileName);
        IndexOutput indexOutput = store.createVerifyingOutput(tempFileName, metaData, IOContext.DEFAULT);
        openIndexOutputs.put(fileName, indexOutput);
        return indexOutput;
    }

    @Override
    protected void closeInternal() {
        try {
            // clean open index outputs
            Iterator<Entry<String, IndexOutput>> iterator = openIndexOutputs.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, IndexOutput> entry = iterator.next();
                logger.trace("closing IndexOutput file [{}]", entry.getValue());
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    logger.debug(() -> new ParameterizedMessage("error while closing recovery output [{}]", entry.getValue()), e);
                }
                iterator.remove();
            }
            // trash temporary files
            for (String file : tempFileNames.keySet()) {
                logger.trace("cleaning temporary file [{}]", file);
                store.deleteQuiet(file);
            }
        } finally {
            // free store. increment happens in constructor
            store.decRef();
            indexShard.recoveryStats().decCurrentAsTarget();
            closedLatch.countDown();
        }
    }

    @Override
    public String toString() {
        return shardId + " [" + recoveryId + "]";
    }

    private void ensureRefCount() {
        if (refCount() <= 0) {
            throw new ElasticsearchException("RecoveryStatus is used but it's refcount is 0. Probably a mismatch between incRef/decRef " +
                    "calls");
        }
    }

    /*** Implementation of {@link RecoveryTargetHandler } */

    @Override
    public void prepareForTranslogOperations(boolean fileBasedRecovery, int totalTranslogOps) throws IOException {
        if (fileBasedRecovery && indexShard.indexSettings().getIndexVersionCreated().before(Version.V_6_0_0)) {
            store.ensureIndexHas6xCommitTags();
        }
        state().getTranslog().totalOperations(totalTranslogOps);
        indexShard().openEngineAndSkipTranslogRecovery();
    }

    @Override
    public void finalizeRecovery(final long globalCheckpoint) throws IOException {
        final IndexShard indexShard = indexShard();
        indexShard.updateGlobalCheckpointOnReplica(globalCheckpoint, "finalizing recovery");
        // Persist the global checkpoint.
        indexShard.sync();
        indexShard.finalizeRecovery();
    }

    @Override
    public void ensureClusterStateVersion(long clusterStateVersion) {
        ensureClusterStateVersionCallback.accept(clusterStateVersion);
    }

    @Override
    public void handoffPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        indexShard.activateWithPrimaryContext(primaryContext);
    }

    @Override
    public long indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps, long maxSeenAutoIdTimestampOnPrimary,
                                        long maxSeqNoOfDeletesOrUpdatesOnPrimary) throws IOException {
        final RecoveryState.Translog translog = state().getTranslog();
        translog.totalOperations(totalTranslogOps);
        assert indexShard().recoveryState() == state();
        if (indexShard().state() != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, indexShard().state());
        }
        /*
         * The maxSeenAutoIdTimestampOnPrimary received from the primary is at least the highest auto_id_timestamp from any operation
         * will be replayed. Bootstrapping this timestamp here will disable the optimization for original append-only requests
         * (source of these operations) replicated via replication. Without this step, we may have duplicate documents if we
         * replay these operations first (without timestamp), then optimize append-only requests (with timestamp).
         */
        indexShard().updateMaxUnsafeAutoIdTimestamp(maxSeenAutoIdTimestampOnPrimary);
        /*
         * Bootstrap the max_seq_no_of_updates from the primary to make sure that the max_seq_no_of_updates on this replica when
         * replaying any of these operations will be at least the max_seq_no_of_updates on the primary when that operation was executed on.
         */
        indexShard().advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfDeletesOrUpdatesOnPrimary);
        for (Translog.Operation operation : operations) {
            Engine.Result result = indexShard().applyTranslogOperation(operation, Engine.Operation.Origin.PEER_RECOVERY);
            if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                throw new MapperException("mapping updates are not allowed [" + operation + "]");
            }
            assert result.getFailure() == null: "unexpected failure while replicating translog entry: " + result.getFailure();
            ExceptionsHelper.reThrowIfNotNull(result.getFailure());
        }
        // update stats only after all operations completed (to ensure that mapping updates don't mess with stats)
        translog.incrementRecoveredOperations(operations.size());
        indexShard().sync();
        // roll over / flush / trim if needed
        indexShard().afterWriteOperation();
        return indexShard().getLocalCheckpoint();
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames,
                                List<Long> phase1FileSizes,
                                List<String> phase1ExistingFileNames,
                                List<Long> phase1ExistingFileSizes,
                                int totalTranslogOps) {
        final RecoveryState.Index index = state().getIndex();
        for (int i = 0; i < phase1ExistingFileNames.size(); i++) {
            index.addFileDetail(phase1ExistingFileNames.get(i), phase1ExistingFileSizes.get(i), true);
        }
        for (int i = 0; i < phase1FileNames.size(); i++) {
            index.addFileDetail(phase1FileNames.get(i), phase1FileSizes.get(i), false);
        }
        state().getTranslog().totalOperations(totalTranslogOps);
        state().getTranslog().totalOperationsOnStart(totalTranslogOps);

    }

    @Override
    public void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException {
        state().getTranslog().totalOperations(totalTranslogOps);
        // first, we go and move files that were created with the recovery id suffix to
        // the actual names, its ok if we have a corrupted index here, since we have replicas
        // to recover from in case of a full cluster shutdown just when this code executes...
        renameAllTempFiles();
        final Store store = store();
        store.incRef();
        try {
            store.cleanupAndVerify("recovery CleanFilesRequestHandler", sourceMetaData);
            // TODO: Assign the global checkpoint to the max_seqno of the safe commit if the index version >= 6.2
            final String translogUUID = Translog.createEmptyTranslog(
                indexShard.shardPath().resolveTranslog(), SequenceNumbers.UNASSIGNED_SEQ_NO, shardId,
                indexShard.getPendingPrimaryTerm());
            store.associateIndexWithNewTranslog(translogUUID);
        } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
            // this is a fatal exception at this stage.
            // this means we transferred files from the remote that have not be checksummed and they are
            // broken. We have to clean up this shard entirely, remove all files and bubble it up to the
            // source shard since this index might be broken there as well? The Source can handle this and checks
            // its content on disk if possible.
            try {
                try {
                    store.removeCorruptionMarker();
                } finally {
                    Lucene.cleanLuceneIndex(store.directory()); // clean up and delete all files
                }
            } catch (Exception e) {
                logger.debug("Failed to clean lucene index", e);
                ex.addSuppressed(e);
            }
            RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
            fail(rfe, true);
            throw rfe;
        } catch (Exception ex) {
            RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
            fail(rfe, true);
            throw rfe;
        } finally {
            store.decRef();
        }
    }

    @Override
    public void writeFileChunk(StoreFileMetaData fileMetaData, long position, BytesReference content,
                               boolean lastChunk, int totalTranslogOps) throws IOException {
        final Store store = store();
        final String name = fileMetaData.name();
        state().getTranslog().totalOperations(totalTranslogOps);
        final RecoveryState.Index indexState = state().getIndex();
        IndexOutput indexOutput;
        if (position == 0) {
            indexOutput = openAndPutIndexOutput(name, fileMetaData, store);
        } else {
            indexOutput = getOpenIndexOutput(name);
        }
        BytesRefIterator iterator = content.iterator();
        BytesRef scratch;
        while((scratch = iterator.next()) != null) { // we iterate over all pages - this is a 0-copy for all core impls
            indexOutput.writeBytes(scratch.bytes, scratch.offset, scratch.length);
        }
        indexState.addRecoveredBytesToFile(name, content.length());
        if (indexOutput.getFilePointer() >= fileMetaData.length() || lastChunk) {
            try {
                Store.verify(indexOutput);
            } finally {
                // we are done
                indexOutput.close();
            }
            final String temporaryFileName = getTempNameForFile(name);
            assert Arrays.asList(store.directory().listAll()).contains(temporaryFileName) :
                "expected: [" + temporaryFileName + "] in " + Arrays.toString(store.directory().listAll());
            store.directory().sync(Collections.singleton(temporaryFileName));
            IndexOutput remove = removeOpenIndexOutputs(name);
            assert remove == null || remove == indexOutput; // remove maybe null if we got finished
        }
    }

    Path translogLocation() {
        return indexShard().shardPath().resolveTranslog();
    }
}
