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

package org.elasticsearch.indices;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import io.crate.common.collections.Sets;
import io.crate.common.unit.TimeValue;

public class IndexingMemoryController implements IndexingOperationListener, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(IndexingMemoryController.class);

    /** How much heap (% or bytes) we will share across all actively indexing shards on this node (default: 10%). */
    public static final Setting<ByteSizeValue> INDEX_BUFFER_SIZE_SETTING =
            Setting.memorySizeSetting("indices.memory.index_buffer_size", "10%", Property.NodeScope);

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %, to set a floor on the actual size in bytes (default: 48 MB). */
    public static final Setting<ByteSizeValue> MIN_INDEX_BUFFER_SIZE_SETTING = Setting.byteSizeSetting("indices.memory.min_index_buffer_size",
                                                                                                       new ByteSizeValue(48, ByteSizeUnit.MB),
                                                                                                       new ByteSizeValue(0, ByteSizeUnit.BYTES),
                                                                                                       new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
                                                                                                       Property.NodeScope);

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %, to set a ceiling on the actual size in bytes (default: not set). */
    public static final Setting<ByteSizeValue> MAX_INDEX_BUFFER_SIZE_SETTING = Setting.byteSizeSetting("indices.memory.max_index_buffer_size",
                                                                                                       new ByteSizeValue(-1),
                                                                                                       new ByteSizeValue(-1),
                                                                                                       new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
                                                                                                       Property.NodeScope);

    /** If we see no indexing operations after this much time for a given shard, we consider that shard inactive (default: 5 minutes). */
    public static final Setting<TimeValue> SHARD_INACTIVE_TIME_SETTING = Setting.positiveTimeSetting("indices.memory.shard_inactive_time", TimeValue.timeValueMinutes(5), Property.NodeScope);

    /** How frequently we check indexing memory usage (default: 5 seconds). */
    public static final Setting<TimeValue> SHARD_MEMORY_INTERVAL_TIME_SETTING = Setting.positiveTimeSetting("indices.memory.interval", TimeValue.timeValueSeconds(5), Property.NodeScope);

    private final ThreadPool threadPool;

    private final Iterable<IndexShard> indexShards;

    private final ByteSizeValue indexingBuffer;

    private final TimeValue inactiveTime;
    private final TimeValue interval;

    /** Contains shards currently being throttled because we can't write segments quickly enough */
    private final Set<IndexShard> throttled = new HashSet<>();

    private final Cancellable scheduler;

    private static final EnumSet<IndexShardState> CAN_WRITE_INDEX_BUFFER_STATES = EnumSet.of(
            IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED);

    private final ShardsIndicesStatusChecker statusChecker;
    private final Set<IndexShard> pendingWriteIndexingBufferSet = Sets.newConcurrentHashSet();
    private final Queue<IndexShard> pendingWriteIndexingBufferQueue = new ConcurrentLinkedQueue<>();

    IndexingMemoryController(Settings settings, ThreadPool threadPool, Iterable<IndexShard> indexServices) {
        this.indexShards = indexServices;

        ByteSizeValue indexingBuffer = INDEX_BUFFER_SIZE_SETTING.get(settings);

        String indexingBufferSetting = settings.get(INDEX_BUFFER_SIZE_SETTING.getKey());
        // null means we used the default (10%)
        if (indexingBufferSetting == null || indexingBufferSetting.endsWith("%")) {
            // We only apply the min/max when % value was used for the index buffer:
            ByteSizeValue minIndexingBuffer = MIN_INDEX_BUFFER_SIZE_SETTING.get(settings);
            ByteSizeValue maxIndexingBuffer = MAX_INDEX_BUFFER_SIZE_SETTING.get(settings);
            if (indexingBuffer.getBytes() < minIndexingBuffer.getBytes()) {
                indexingBuffer = minIndexingBuffer;
            }
            if (maxIndexingBuffer.getBytes() != -1 && indexingBuffer.getBytes() > maxIndexingBuffer.getBytes()) {
                indexingBuffer = maxIndexingBuffer;
            }
        }
        this.indexingBuffer = indexingBuffer;

        this.inactiveTime = SHARD_INACTIVE_TIME_SETTING.get(settings);
        // we need to have this relatively small to free up heap quickly enough
        this.interval = SHARD_MEMORY_INTERVAL_TIME_SETTING.get(settings);

        this.statusChecker = new ShardsIndicesStatusChecker();

        LOGGER.debug("using indexing buffer size [{}] with {} [{}], {} [{}]",
                     this.indexingBuffer,
                     SHARD_INACTIVE_TIME_SETTING.getKey(), this.inactiveTime,
                     SHARD_MEMORY_INTERVAL_TIME_SETTING.getKey(), this.interval);
        this.scheduler = scheduleTask(threadPool);

        // Need to save this so we can later launch async "write indexing buffer to disk" on shards:
        this.threadPool = threadPool;
    }

    protected Cancellable scheduleTask(ThreadPool threadPool) {
        // it's fine to run it on the scheduler thread, no busy work
        return threadPool.scheduleWithFixedDelay(statusChecker, interval, Names.SAME);
    }

    @Override
    public void close() {
        scheduler.cancel();
    }

    /**
     * returns the current budget for the total amount of indexing buffers of
     * active shards on this node
     */
    ByteSizeValue indexingBufferSize() {
        return indexingBuffer;
    }

    protected List<IndexShard> availableShards() {
        List<IndexShard> availableShards = new ArrayList<>();
        for (IndexShard shard : indexShards) {
            if (CAN_WRITE_INDEX_BUFFER_STATES.contains(shard.state())) {
                availableShards.add(shard);
            }
        }
        return availableShards;
    }

    /** returns how much heap this shard is using for its indexing buffer */
    protected long getIndexBufferRAMBytesUsed(IndexShard shard) {
        return shard.getIndexBufferRAMBytesUsed();
    }

    /** returns how many bytes this shard is currently writing to disk */
    protected long getShardWritingBytes(IndexShard shard) {
        return shard.getWritingBytes();
    }

    /// Ask this shard to refresh to free up heap
    protected void writeIndexingBuffer(IndexShard shard) {
        // Remove the shard from the set first, so that multiple threads can run writeIndexingBuffer concurrently on the same shard.
        pendingWriteIndexingBufferSet.remove(shard);
        shard.writeIndexingBuffer();
    }

    /// Write the pending indexing buffers in the current thread. This should run in the indexing thread to naturally
    /// apply backpressure, similar to what Lucene does in the DocumentsWriter#postUpdate method.
    protected boolean writePendingIndexingBuffers() {
        boolean wrotePendingIndexingBuffer = false;
        for (IndexShard shard = pendingWriteIndexingBufferQueue.poll(); shard != null; shard = pendingWriteIndexingBufferQueue.poll()) {
            writeIndexingBuffer(shard);
            wrotePendingIndexingBuffer = true;
        }
        return wrotePendingIndexingBuffer;
    }

    /// Write the pending indexing buffers, scheduling each write on the refresh thread pool.
    protected void writePendingIndexingBuffersAsync() {
        for (IndexShard shard = pendingWriteIndexingBufferQueue.poll(); shard != null; shard = pendingWriteIndexingBufferQueue.poll()) {
            final IndexShard finalShard = shard;
            threadPool.executor(ThreadPool.Names.REFRESH).execute(() -> writeIndexingBuffer(finalShard));
        }
    }

    /// Adds a shard to the pending indexing buffer write queue if not already present.
    public void addPendingWriteIndexingBuffer(IndexShard shard) {
        if (pendingWriteIndexingBufferSet.add(shard)) {
            pendingWriteIndexingBufferQueue.add(shard);
        }
    }

    /** force checker to run now */
    void forceCheck() {
        statusChecker.run();
    }

    /** Asks this shard to throttle indexing to one thread */
    protected void activateThrottling(IndexShard shard) {
        shard.activateThrottling();
    }

    /** Asks this shard to stop throttling indexing to one thread */
    protected void deactivateThrottling(IndexShard shard) {
        shard.deactivateThrottling();
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        postOperation(index, result);
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        postOperation(delete, result);
    }

    private void postOperation(Engine.Operation operation, Engine.Result result) {
        recordOperationBytes(operation, result);
        // Piggyback on indexing threads to write segments because we want memory to be reclaimed rapidly. This has the
        // downside of increasing the latency of write requests though, but Lucene is doing the same in its indexing
        // threads anyway on DocumentsWriter#postUpdate (executing all pending flushes while the flush task are created
        // by the DocumentsWriter#updateDocuments if the RAM buffer limits are reached).
        while (writePendingIndexingBuffers()) {
            // Ensure only one thread is writing the indexing buffers
            if (statusChecker.tryRun() == false) {
                break;
            }
        }
    }

    /** called by IndexShard to record estimated bytes written to translog for the operation */
    private void recordOperationBytes(Engine.Operation operation, Engine.Result result) {
        if (result.getResultType() == Engine.Result.Type.SUCCESS) {
            statusChecker.bytesWritten(operation.estimatedSizeInBytes());
        }
    }


    private static final class ShardAndBytesUsed implements Comparable<ShardAndBytesUsed> {
        final long bytesUsed;
        final IndexShard shard;

        ShardAndBytesUsed(long bytesUsed, IndexShard shard) {
            this.bytesUsed = bytesUsed;
            this.shard = shard;
        }

        @Override
        public int compareTo(ShardAndBytesUsed other) {
            // Sort larger shards first:
            return Long.compare(other.bytesUsed, bytesUsed);
        }
    }

    /** not static because we need access to many fields/methods from our containing class (IMC): */
    final class ShardsIndicesStatusChecker implements Runnable {

        final AtomicLong bytesWrittenSinceCheck = new AtomicLong();
        final ReentrantLock runLock = new ReentrantLock();
        private ShardId lastShardIdFlushed;

        /** Shard calls this on each indexing/delete op */
        public void bytesWritten(int bytes) {
            long totalBytes = bytesWrittenSinceCheck.addAndGet(bytes);
            assert totalBytes >= 0;
            while (totalBytes > indexingBuffer.getBytes() / 30) {
                if (runLock.tryLock()) {
                    try {
                        // Must pull this again because it may have changed since we first checked:
                        totalBytes = bytesWrittenSinceCheck.get();
                        if (totalBytes > indexingBuffer.getBytes() / 30) {
                            bytesWrittenSinceCheck.addAndGet(-totalBytes);
                            // NOTE: this is only an approximate check, because bytes written is to the translog, vs indexing memory buffer which is
                            // typically smaller but can be larger in extreme cases (many unique terms).  This logic is here only as a safety against
                            // thread starvation or too infrequent checking, to ensure we are still checking periodically, in proportion to bytes
                            // processed by indexing:
                            runUnlocked();
                        }
                    } finally {
                        runLock.unlock();
                    }

                    // Must get it again since other threads could have increased it while we were in runUnlocked
                    totalBytes = bytesWrittenSinceCheck.get();
                } else {
                    // Another thread beat us to it: let them do all the work, yay!
                    break;
                }
            }
        }

        @Override
        public void run() {
            // Usually, indexing buffers are written in the indexing threads themselves (to apply back-pressure).
            // Running any pending writes here asynchronously in the scheduler thread to ensure memory is freed
            // even when indexing threads are busy.
            writePendingIndexingBuffersAsync();
            runLock.lock();
            try {
                runUnlocked();
            } finally {
                runLock.unlock();
            }
        }

        public boolean tryRun() {
            if (runLock.tryLock()) {
                try {
                    runUnlocked();
                } finally {
                    runLock.unlock();
                }
                return true;
            }
            return false;
        }

        private void runUnlocked() {
            assert runLock.isHeldByCurrentThread() : "IndexMemoryController.StatusChecker->runUnlocked must be called with runLock held";

            // NOTE: even if we hit an errant exc here, our ThreadPool.scheduledWithFixedDelay will log the exception and re-invoke us
            // again, on schedule

            // First pass to sum up how much heap all shards' indexing buffers are using now, and how many bytes they are currently moving
            // to disk:
            long totalBytesUsed = 0;
            long totalBytesWriting = 0;
            List<IndexShard> availableShards = availableShards();
            // Ensure we have a deterministic order for all processing shards, this is especially important when we pick
            // shards to flush to free up memory
            availableShards.sort(Comparator.comparing(IndexShard::shardId));
            for (IndexShard shard : availableShards) {

                // Give shard a chance to transition to inactive so we can flush:
                checkIdle(shard, inactiveTime.nanos());

                // How many bytes this shard is currently (async'd) moving from heap to disk:
                long shardWritingBytes = getShardWritingBytes(shard);

                // How many heap bytes this shard is currently using
                long shardBytesUsed = getIndexBufferRAMBytesUsed(shard);

                shardBytesUsed -= shardWritingBytes;
                totalBytesWriting += shardWritingBytes;

                // If the refresh completed just after we pulled shardWritingBytes and before we pulled shardBytesUsed, then we could
                // have a negative value here.  So we just skip this shard since that means it's now using very little heap:
                if (shardBytesUsed < 0) {
                    continue;
                }

                totalBytesUsed += shardBytesUsed;
            }

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("total indexing heap bytes used [{}] vs {} [{}], currently writing bytes [{}]",
                             new ByteSizeValue(totalBytesUsed), INDEX_BUFFER_SIZE_SETTING.getKey(), indexingBuffer, new ByteSizeValue(totalBytesWriting));
            }

            // If we are using more than 50% of our budget across both indexing buffer and bytes we are still moving to disk, then we now
            // throttle the top shards to send back-pressure to ongoing indexing:
            boolean doThrottle = (totalBytesWriting + totalBytesUsed) > 1.5 * indexingBuffer.getBytes();

            if (totalBytesUsed > indexingBuffer.getBytes()) {
                // OK we are now over-budget; fill the priority queue and ask largest shard(s) to refresh:
                ArrayList<ShardAndBytesUsed> queue = new ArrayList<>();

                for (IndexShard shard : availableShards) {
                    // How many bytes this shard is currently (async'd) moving from heap to disk:
                    long shardWritingBytes = getShardWritingBytes(shard);

                    // How many heap bytes this shard is currently using
                    long shardBytesUsed = getIndexBufferRAMBytesUsed(shard);

                    // Only count up bytes not already being refreshed:
                    shardBytesUsed -= shardWritingBytes;

                    // If the refresh completed just after we pulled shardWritingBytes and before we pulled shardBytesUsed, then we could
                    // have a negative value here.  So we just skip this shard since that means it's now using very little heap:
                    if (shardBytesUsed < 0) {
                        continue;
                    }

                    if (shardBytesUsed > 0) {
                        if (LOGGER.isTraceEnabled()) {
                            if (shardWritingBytes != 0) {
                                LOGGER.trace("shard [{}] is using [{}] heap, writing [{}] heap", shard.shardId(), shardBytesUsed, shardWritingBytes);
                            } else {
                                LOGGER.trace("shard [{}] is using [{}] heap, not writing any bytes", shard.shardId(), shardBytesUsed);
                            }
                        }
                        queue.add(new ShardAndBytesUsed(shardBytesUsed, shard));
                    }
                }

                LOGGER.debug("now write some indexing buffers: total indexing heap bytes used [{}] vs {} [{}], currently writing bytes [{}], [{}] shards with non-zero indexing buffer",
                             new ByteSizeValue(totalBytesUsed), INDEX_BUFFER_SIZE_SETTING.getKey(), indexingBuffer, new ByteSizeValue(totalBytesWriting), queue.size());


                // Rotate the list so we start flushing from the next shard after the last one flushed (round-robin)
                // Why? - most times the largest shard is the most actively indexing one, so flushing it may result
                // in new segments being created right after the flush. This thought seems to be backed by our own
                // benchmarks as well as ES's ones, see https://github.com/elastic/elasticsearch/pull/94607#issuecomment-1490815094.
                if (lastShardIdFlushed != null) {
                    int nextShardIdToFlush = 0;
                    for (ShardAndBytesUsed shardAndBytesUsed : queue) {
                        if (shardAndBytesUsed.shard.shardId().compareTo(lastShardIdFlushed) > 0) {
                            break;
                        }
                        nextShardIdToFlush++;
                    }
                    Collections.rotate(queue, -nextShardIdToFlush);
                }


                for (ShardAndBytesUsed shardAndBytesUsed : queue) {
                    LOGGER.debug("write indexing buffer to disk for shard [{}] to free up its [{}] indexing buffer", shardAndBytesUsed.shard.shardId(), new ByteSizeValue(shardAndBytesUsed.bytesUsed));
                    addPendingWriteIndexingBuffer(shardAndBytesUsed.shard);
                    totalBytesUsed -= shardAndBytesUsed.bytesUsed;
                    lastShardIdFlushed = shardAndBytesUsed.shard.shardId();
                    if (doThrottle && throttled.contains(shardAndBytesUsed.shard) == false) {
                        LOGGER.info("now throttling indexing for shard [{}]: segment writing can't keep up", shardAndBytesUsed.shard.shardId());
                        throttled.add(shardAndBytesUsed.shard);
                        activateThrottling(shardAndBytesUsed.shard);
                    }
                    // Abort once we are back under budget
                    if (totalBytesUsed <= indexingBuffer.getBytes()) {
                        break;
                    }
                }
            }

            if (doThrottle == false) {
                for (IndexShard shard : throttled) {
                    LOGGER.info("stop throttling indexing for shard [{}]", shard.shardId());
                    deactivateThrottling(shard);
                }
                throttled.clear();
            }
        }
    }

    /**
     * ask this shard to check now whether it is inactive, and reduces its indexing buffer if so.
     */
    protected void checkIdle(IndexShard shard, long inactiveTimeNS) {
        try {
            shard.flushOnIdle(inactiveTimeNS);
        } catch (AlreadyClosedException e) {
            LOGGER.trace(() -> new ParameterizedMessage("ignore exception while checking if shard {} is inactive", shard.shardId()), e);
        }
    }
}
