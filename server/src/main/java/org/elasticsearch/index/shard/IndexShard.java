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

package org.elasticsearch.index.shard;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import com.carrotsearch.hppc.ObjectLongMap;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AsyncIOProcessor;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.gateway.WriteStateException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.RefreshFailedEngineException;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.Store.MetadataSnapshot;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.Booleans;
import io.crate.common.CheckedFunction;
import io.crate.common.collections.Tuple;
import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;

public class IndexShard extends AbstractIndexShardComponent implements IndicesClusterStateService.Shard {

    public static final long RETAIN_ALL = -1;

    private final ThreadPool threadPool;
    private final MapperService mapperService;
    private final IndexCache indexCache;
    private final Store store;
    private final Object mutex = new Object();
    private final String checkIndexOnStartup;
    private final CodecService codecService;
    private final TranslogConfig translogConfig;
    private final IndexEventListener indexEventListener;
    private final QueryCachingPolicy cachingPolicy;
    // Package visible for testing
    final CircuitBreakerService circuitBreakerService;

    private final GlobalCheckpointListeners globalCheckpointListeners;
    private final ReplicationTracker replicationTracker;

    protected volatile ShardRouting shardRouting;
    protected volatile IndexShardState state;
    protected volatile long pendingPrimaryTerm; // see JavaDocs for getPendingPrimaryTerm
    protected final AtomicReference<Engine> currentEngineReference = new AtomicReference<>();
    final EngineFactory engineFactory;

    private final IndexingOperationListener indexingOperationListeners;
    private final Runnable globalCheckpointSyncer;

    Runnable getGlobalCheckpointSyncer() {
        return globalCheckpointSyncer;
    }

    private final RetentionLeaseSyncer retentionLeaseSyncer;

    @Nullable
    private RecoveryState recoveryState;

    private final RecoveryStats recoveryStats = new RecoveryStats();
    private final MeanMetric refreshMetric = new MeanMetric();
    private final MeanMetric flushMetric = new MeanMetric();
    private final CounterMetric periodicFlushMetric = new CounterMetric();

    private final ShardEventListener shardEventListener = new ShardEventListener();

    private final ShardPath path;

    private final IndexShardOperationPermits indexShardOperationPermits;

    private static final EnumSet<IndexShardState> READ_ALLOWED_STATES = EnumSet.of(IndexShardState.STARTED, IndexShardState.POST_RECOVERY);

    // for primaries, we only allow to write when actually started (so the cluster has decided we started)
    // in case we have a relocation of a primary, we also allow to write after phase 2 completed, where the shard may be
    // in state RECOVERING or POST_RECOVERY.
    // for replicas, replication is also allowed while recovering, since we index also during recovery to replicas and rely on version checks to make sure its consistent
    // a relocated shard can also be target of a replication if the relocation target has not been marked as active yet and is syncing it's changes back to the relocation source
    private static final EnumSet<IndexShardState> WRITE_ALLOWED_STATES = EnumSet.of(IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED);

    private final CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper;

    /**
     * True if this shard is still indexing (recently) and false if we've been idle for long enough (as periodically checked by {@link
     * IndexingMemoryController}).
     */
    private final AtomicBoolean active = new AtomicBoolean();
    /**
     * Allows for the registration of listeners that are called when a change becomes visible for search.
     */
    private final RefreshListeners refreshListeners;

    public IndexShard(
            ShardRouting shardRouting,
            IndexSettings indexSettings,
            ShardPath path,
            Store store,
            IndexCache indexCache,
            MapperService mapperService,
            @Nullable EngineFactory engineFactory,
            IndexEventListener indexEventListener,
            CheckedFunction<DirectoryReader, DirectoryReader, IOException> indexReaderWrapper,
            ThreadPool threadPool,
            BigArrays bigArrays,
            List<IndexingOperationListener> listeners,
            Runnable globalCheckpointSyncer,
            RetentionLeaseSyncer retentionLeaseSyncer,
            CircuitBreakerService circuitBreakerService) throws IOException {
        super(shardRouting.shardId(), indexSettings);
        assert shardRouting.initializing();
        this.shardRouting = shardRouting;
        final Settings settings = indexSettings.getSettings();
        this.codecService = new CodecService(mapperService, logger);
        Objects.requireNonNull(store, "Store must be provided to the index shard");
        this.engineFactory = Objects.requireNonNull(engineFactory);
        this.store = store;
        this.indexEventListener = indexEventListener;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.indexCache = indexCache;
        this.indexingOperationListeners = new IndexingOperationListener.CompositeListener(listeners, logger);
        this.globalCheckpointSyncer = globalCheckpointSyncer;
        this.retentionLeaseSyncer = retentionLeaseSyncer;
        state = IndexShardState.CREATED;
        this.path = path;
        this.circuitBreakerService = circuitBreakerService;
        /* create engine config */
        logger.debug("state: [CREATED]");

        this.checkIndexOnStartup = indexSettings.getValue(IndexSettings.INDEX_CHECK_ON_STARTUP);
        this.translogConfig = new TranslogConfig(shardId, shardPath().resolveTranslog(), indexSettings, bigArrays);
        final String aId = shardRouting.allocationId().getId();
        final long primaryTerm = indexSettings.getIndexMetadata().primaryTerm(shardId.id());
        this.pendingPrimaryTerm = primaryTerm;
        this.globalCheckpointListeners = new GlobalCheckpointListeners(
            shardId,
            threadPool.executor(ThreadPool.Names.LISTENER),
            threadPool.scheduler(),
            logger
        );
        this.replicationTracker = new ReplicationTracker(
            shardId,
            aId,
            indexSettings,
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            globalCheckpointListeners::globalCheckpointUpdated,
            threadPool::absoluteTimeInMillis,
            (retentionLeases, listener) -> retentionLeaseSyncer.sync(shardId, retentionLeases, listener)
        );

        // the query cache is a node-level thing, however we want the most popular filters
        // to be computed on a per-shard basis
        if (IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.get(settings)) {
            cachingPolicy = new QueryCachingPolicy() {

                @Override
                public void onUse(Query query) {

                }

                @Override
                public boolean shouldCache(Query query) {
                    return true;
                }
            };
        } else {
            cachingPolicy = new UsageTrackingQueryCachingPolicy();
        }
        indexShardOperationPermits = new IndexShardOperationPermits(shardId, logger, threadPool);
        readerWrapper = indexReaderWrapper;
        refreshListeners = buildRefreshListeners();
        persistMetadata(path, indexSettings, shardRouting, null, logger);
    }

    public ThreadPool getThreadPool() {
        return this.threadPool;
    }

    public Store store() {
        return this.store;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    /**
     * USE THIS METHOD WITH CARE!
     * Returns the primary term the index shard is supposed to be on. In case of primary promotion or when a replica learns about
     * a new term due to a new primary, the term that's exposed here will not be the term that the shard internally uses to assign
     * to operations. The shard will auto-correct its internal operation term, but this might take time.
     * See {@link IndexMetadata#primaryTerm(int)}
     */
    public long getPendingPrimaryTerm() {
        return this.pendingPrimaryTerm;
    }

    /** Returns the primary term that is currently being used to assign to operations */
    public long getOperationPrimaryTerm() {
        return replicationTracker.getOperationPrimaryTerm();
    }

    /**
     * Returns the latest cluster routing entry received with this shard.
     */
    @Override
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    @Override
    public void updateShardState(final ShardRouting newRouting,
                                 final long newPrimaryTerm,
                                 final BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
                                 final long applyingClusterStateVersion,
                                 final Set<String> inSyncAllocationIds,
                                 final IndexShardRoutingTable routingTable) throws IOException {
        final ShardRouting currentRouting;
        synchronized (mutex) {
            currentRouting = this.shardRouting;

            if (!newRouting.shardId().equals(shardId())) {
                throw new IllegalArgumentException("Trying to set a routing entry with shardId " + newRouting.shardId() + " on a shard with shardId " + shardId());
            }
            if (currentRouting == null || newRouting.isSameAllocation(currentRouting) == false) {
                throw new IllegalArgumentException("Trying to set a routing entry with a different allocation. Current " + currentRouting + ", new " + newRouting);
            }
            if (currentRouting.primary() && newRouting.primary() == false) {
                throw new IllegalArgumentException("illegal state: trying to move shard from primary mode to replica mode. Current "
                    + currentRouting + ", new " + newRouting);
            }

            if (newRouting.primary()) {
                replicationTracker.updateFromMaster(applyingClusterStateVersion, inSyncAllocationIds, routingTable);
            }

            if (state == IndexShardState.POST_RECOVERY && newRouting.active()) {
                assert currentRouting.active() == false : "we are in POST_RECOVERY, but our shard routing is active " + currentRouting;

                assert currentRouting.isRelocationTarget() == false || currentRouting.primary() == false ||
                       replicationTracker.isPrimaryMode() :
                    "a primary relocation is completed by the master, but primary mode is not active " + currentRouting;

                changeState(IndexShardState.STARTED, "global state is [" + newRouting.state() + "]");
            } else if (currentRouting.primary() && currentRouting.relocating() && replicationTracker.isRelocated() &&
                (newRouting.relocating() == false || newRouting.equalsIgnoringMetadata(currentRouting) == false)) {
                // if the shard is not in primary mode anymore (after primary relocation) we have to fail when any changes in shard routing occur (e.g. due to recovery
                // failure / cancellation). The reason is that at the moment we cannot safely reactivate primary mode without risking two
                // active primaries.
                throw new IndexShardRelocatedException(shardId(), "Shard is marked as relocated, cannot safely move to state " + newRouting.state());
            }
            assert newRouting.active() == false || state == IndexShardState.STARTED || state == IndexShardState.CLOSED :
                "routing is active, but local shard state isn't. routing: " + newRouting + ", local state: " + state;
            persistMetadata(path, indexSettings, newRouting, currentRouting, logger);
            final CountDownLatch shardStateUpdated = new CountDownLatch(1);

            if (newRouting.primary()) {
                if (newPrimaryTerm == pendingPrimaryTerm) {
                    if (currentRouting.initializing() && newRouting.active()) {
                        if (currentRouting.isRelocationTarget() == false) {
                            // the master started a recovering primary, activate primary mode.
                            replicationTracker.activatePrimaryMode(getLocalCheckpoint());
                        }
                    }
                } else {
                    assert currentRouting.primary() == false : "term is only increased as part of primary promotion";
                    /* Note that due to cluster state batching an initializing primary shard term can failed and re-assigned
                     * in one state causing it's term to be incremented. Note that if both current shard state and new
                     * shard state are initializing, we could replace the current shard and reinitialize it. It is however
                     * possible that this shard is being started. This can happen if:
                     * 1) Shard is post recovery and sends shard started to the master
                     * 2) Node gets disconnected and rejoins
                     * 3) Master assigns the shard back to the node
                     * 4) Master processes the shard started and starts the shard
                     * 5) The node process the cluster state where the shard is both started and primary term is incremented.
                     *
                     * We could fail the shard in that case, but this will cause it to be removed from the insync allocations list
                     * potentially preventing re-allocation.
                     */
                    assert newRouting.initializing() == false :
                        "a started primary shard should never update its term; "
                            + "shard " + newRouting + ", "
                            + "current term [" + pendingPrimaryTerm + "], "
                            + "new term [" + newPrimaryTerm + "]";
                    assert newPrimaryTerm > pendingPrimaryTerm :
                        "primary terms can only go up; current term [" + pendingPrimaryTerm + "], new term [" + newPrimaryTerm + "]";
                    /*
                     * Before this call returns, we are guaranteed that all future operations are delayed and so this happens before we
                     * increment the primary term. The latch is needed to ensure that we do not unblock operations before the primary term is
                     * incremented.
                     */
                    // to prevent primary relocation handoff while resync is not completed
                    boolean resyncStarted = primaryReplicaResyncInProgress.compareAndSet(false, true);
                    if (resyncStarted == false) {
                        throw new IllegalStateException("cannot start resync while it's already in progress");
                    }
                    bumpPrimaryTerm(newPrimaryTerm,
                        () -> {
                            shardStateUpdated.await();
                            assert pendingPrimaryTerm == newPrimaryTerm :
                                "shard term changed on primary. expected [" + newPrimaryTerm + "] but was [" + pendingPrimaryTerm + "]" +
                                ", current routing: " + currentRouting + ", new routing: " + newRouting;
                            assert getOperationPrimaryTerm() == newPrimaryTerm;
                            try {
                                replicationTracker.activatePrimaryMode(getLocalCheckpoint());
                                /*
                                 * If this shard was serving as a replica shard when another shard was promoted to primary then
                                 * its Lucene index was reset during the primary term transition. In particular, the Lucene index
                                 * on this shard was reset to the global checkpoint and the operations above the local checkpoint
                                 * were reverted. If the other shard that was promoted to primary subsequently fails before the
                                 * primary/replica re-sync completes successfully and we are now being promoted, we have to restore
                                 * the reverted operations on this shard by replaying the translog to avoid losing acknowledged writes.
                                 */
                                final Engine engine = getEngine();
                                engine.restoreLocalHistoryFromTranslog((resettingEngine, snapshot) ->
                                    runTranslogRecovery(resettingEngine, snapshot, Engine.Operation.Origin.LOCAL_RESET, () -> {}));
                                if (indexSettings.getIndexVersionCreated().onOrBefore(Version.ES_V_6_1_4)) {
                                    // an index that was created before sequence numbers were introduced may contain operations in its
                                    // translog that do not have a sequence numbers. We want to make sure those operations will never
                                    // be replayed as part of peer recovery to avoid an arbitrary mixture of operations with seq# (due
                                    // to active indexing) and operations without a seq# coming from the translog. We therefore flush
                                    // to create a lucene commit point to an empty translog file.
                                    engine.flush(false, true);
                                }
                                /* Rolling the translog generation is not strictly needed here (as we will never have collisions between
                                 * sequence numbers in a translog generation in a new primary as it takes the last known sequence number
                                 * as a starting point), but it simplifies reasoning about the relationship between primary terms and
                                 * translog generations.
                                 */
                                engine.rollTranslogGeneration();
                                engine.fillSeqNoGaps(newPrimaryTerm);
                                replicationTracker.updateLocalCheckpoint(currentRouting.allocationId().getId(), getLocalCheckpoint());
                                primaryReplicaSyncer.accept(this, new ActionListener<ResyncTask>() {
                                    @Override
                                    public void onResponse(ResyncTask resyncTask) {
                                        logger.info("primary-replica resync completed with {} operations",
                                            resyncTask.getResyncedOperations());
                                        boolean resyncCompleted = primaryReplicaResyncInProgress.compareAndSet(true, false);
                                        assert resyncCompleted : "primary-replica resync finished but was not started";
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        boolean resyncCompleted = primaryReplicaResyncInProgress.compareAndSet(true, false);
                                        assert resyncCompleted : "primary-replica resync finished but was not started";
                                        if (state == IndexShardState.CLOSED) {
                                            // ignore, shutting down
                                        } else {
                                            failShard("exception during primary-replica resync", e);
                                        }
                                    }
                                });
                            } catch (final AlreadyClosedException e) {
                                // okay, the index was deleted
                            }
                        });
                }
            }
            // set this last, once we finished updating all internal state.
            this.shardRouting = newRouting;

            assert this.shardRouting.primary() == false ||
                this.shardRouting.started() == false || // note that we use started and not active to avoid relocating shards
                this.indexShardOperationPermits.isBlocked() || // if permits are blocked, we are still transitioning
                this.replicationTracker.isPrimaryMode()
                : "a started primary with non-pending operation term must be in primary mode " + this.shardRouting;
            shardStateUpdated.countDown();
        }
        if (currentRouting.active() == false && newRouting.active()) {
            indexEventListener.afterIndexShardStarted(this);
        }
        if (newRouting.equals(currentRouting) == false) {
            indexEventListener.shardRoutingChanged(this, currentRouting, newRouting);
        }
    }

    /**
     * Marks the shard as recovering based on a recovery state, fails with exception is recovering is not allowed to be set.
     */
    public IndexShardState markAsRecovering(String reason, RecoveryState recoveryState) throws IndexShardStartedException,
        IndexShardRelocatedException, IndexShardRecoveringException, IndexShardClosedException {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RECOVERING) {
                throw new IndexShardRecoveringException(shardId);
            }
            if (state == IndexShardState.POST_RECOVERY) {
                throw new IndexShardRecoveringException(shardId);
            }
            this.recoveryState = recoveryState;
            return changeState(IndexShardState.RECOVERING, reason);
        }
    }

    private final AtomicBoolean primaryReplicaResyncInProgress = new AtomicBoolean();

    /**
     * Completes the relocation. Operations are blocked and current operations are drained before changing state to relocated. The provided
     * {@link Runnable} is executed after all operations are successfully blocked.
     *
     * @param consumer a {@link Runnable} that is executed after operations are blocked
     * @throws IllegalIndexShardStateException if the shard is not relocating due to concurrent cancellation
     * @throws InterruptedException            if blocking operations is interrupted
     */
    public void relocated(final Consumer<ReplicationTracker.PrimaryContext> consumer) throws IllegalIndexShardStateException, InterruptedException {
        assert shardRouting.primary() : "only primaries can be marked as relocated: " + shardRouting;
        try {
            indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> {
                // no shard operation permits are being held here, move state from started to relocated
                assert indexShardOperationPermits.getActiveOperationsCount() == OPERATIONS_BLOCKED :
                        "in-flight operations in progress while moving shard state to relocated";
                /*
                 * We should not invoke the runnable under the mutex as the expected implementation is to handoff the primary context via a
                 * network operation. Doing this under the mutex can implicitly block the cluster state update thread on network operations.
                 */
                verifyRelocatingState();
                final ReplicationTracker.PrimaryContext primaryContext = replicationTracker.startRelocationHandoff();
                try {
                    consumer.accept(primaryContext);
                    synchronized (mutex) {
                        verifyRelocatingState();
                        replicationTracker.completeRelocationHandoff(); // make changes to primaryMode and relocated flag only under mutex
                    }
                } catch (final Exception e) {
                    try {
                        replicationTracker.abortRelocationHandoff();
                    } catch (final Exception inner) {
                        e.addSuppressed(inner);
                    }
                    throw e;
                }
            });
        } catch (TimeoutException e) {
            logger.warn("timed out waiting for relocation hand-off to complete");
            // This is really bad as ongoing replication operations are preventing this shard from completing relocation hand-off.
            // Fail primary relocation source and target shards.
            failShard("timed out waiting for relocation hand-off to complete", null);
            throw new IndexShardClosedException(shardId(), "timed out waiting for relocation hand-off to complete");
        }
    }

    private void verifyRelocatingState() {
        if (state != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shardId, state);
        }
        /*
         * If the master cancelled recovery, the target will be removed and the recovery will be cancelled. However, it is still possible
         * that we concurrently end up here and therefore have to protect that we do not mark the shard as relocated when its shard routing
         * says otherwise.
         */

        if (shardRouting.relocating() == false) {
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED,
                ": shard is no longer relocating " + shardRouting);
        }

        if (primaryReplicaResyncInProgress.get()) {
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED,
                ": primary relocation is forbidden while primary-replica resync is in progress " + shardRouting);
        }
    }

    @Override
    public IndexShardState state() {
        return state;
    }

    /**
     * Changes the state of the current shard
     *
     * @param newState the new shard state
     * @param reason   the reason for the state change
     * @return the previous shard state
     */
    private IndexShardState changeState(IndexShardState newState, String reason) {
        assert Thread.holdsLock(mutex);
        logger.debug("state: [{}]->[{}], reason [{}]", state, newState, reason);
        IndexShardState previousState = state;
        state = newState;
        this.indexEventListener.indexShardStateChanged(this, previousState, newState, reason);
        return previousState;
    }

    public Engine.IndexResult applyIndexOperationOnPrimary(long version,
                                                           VersionType versionType,
                                                           SourceToParse sourceToParse,
                                                           long ifSeqNo,
                                                           long ifPrimaryTerm,
                                                           long autoGeneratedTimestamp,
                                                           boolean isRetry) throws IOException {
        assert versionType.validateVersionForWrites(version);
        return applyIndexOperation(
            getEngine(),
            UNASSIGNED_SEQ_NO,
            getOperationPrimaryTerm(),
            version,
            versionType,
            ifSeqNo,
            ifPrimaryTerm,
            autoGeneratedTimestamp,
            isRetry,
            Engine.Operation.Origin.PRIMARY,
            sourceToParse
        );
    }

    public Engine.IndexResult applyIndexOperationOnReplica(long seqNo,
                                                           long version,
                                                           long autoGeneratedTimeStamp,
                                                           boolean isRetry,
                                                           SourceToParse sourceToParse) throws IOException {
        return applyIndexOperation(
            getEngine(),
            seqNo,
            getOperationPrimaryTerm(),
            version,
            null,
            UNASSIGNED_SEQ_NO,
            0,
            autoGeneratedTimeStamp,
            isRetry,
            Engine.Operation.Origin.REPLICA,
            sourceToParse
        );
    }

    private Engine.IndexResult applyIndexOperation(Engine engine,
                                                   long seqNo,
                                                   long opPrimaryTerm,
                                                   long version,
                                                   @Nullable VersionType versionType,
                                                   long ifSeqNo,
                                                   long ifPrimaryTerm,
                                                   long autoGeneratedTimeStamp,
                                                   boolean isRetry,
                                                   Engine.Operation.Origin origin,
                                                   SourceToParse sourceToParse) throws IOException {
        assert opPrimaryTerm <= getOperationPrimaryTerm()
                : "op term [ " + opPrimaryTerm + " ] > shard term [" + getOperationPrimaryTerm() + "]";
        ensureWriteAllowed(origin);
        Engine.Index operation;
        try {
            operation = prepareIndex(
                defaultDocMapper(),
                sourceToParse,
                seqNo,
                opPrimaryTerm,
                version,
                versionType,
                origin,
                autoGeneratedTimeStamp,
                isRetry,
                ifSeqNo,
                ifPrimaryTerm
            );
            Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
            if (update != null) {
                return new Engine.IndexResult(update);
            }
        } catch (Exception e) {
            // We treat any exception during parsing and or mapping update as a document level failure
            // with the exception side effects of closing the shard. Since we don't have the shard, we
            // can not raise an exception that may block any replication of previous operations to the
            // replicas
            verifyNotClosed(e);
            return new Engine.IndexResult(e, version, opPrimaryTerm, seqNo);
        }

        return index(engine, operation);
    }

    public static Engine.Index prepareIndex(DocumentMapper docMapper,
                                            SourceToParse source,
                                            long seqNo,
                                            long primaryTerm,
                                            long version,
                                            VersionType versionType,
                                            Engine.Operation.Origin origin,
                                            long autoGeneratedIdTimestamp,
                                            boolean isRetry,
                                            long ifSeqNo,
                                            long ifPrimaryTerm) {
        long startTime = System.nanoTime();
        ParsedDocument doc = docMapper.parse(source);
        Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id()));
        return new Engine.Index(uid, doc, seqNo, primaryTerm, version, versionType, origin, startTime, autoGeneratedIdTimestamp, isRetry,
                                ifSeqNo, ifPrimaryTerm);
    }

    private Engine.IndexResult index(Engine engine, Engine.Index index) throws IOException {
        active.set(true);
        final Engine.IndexResult result;
        index = indexingOperationListeners.preIndex(shardId, index);
        boolean traceEnabled = logger.isTraceEnabled();
        try {
            if (traceEnabled) {
                // don't use index.source().utf8ToString() here source might not be valid UTF-8
                logger.trace(
                    "index [{}] seq# [{}] allocation-id [{}] primaryTerm [{}] operationPrimaryTerm [{}] origin [{}]",
                    index.id(),
                    index.seqNo(),
                    routingEntry().allocationId(),
                    index.primaryTerm(),
                    getOperationPrimaryTerm(),
                    index.origin());
            }
            result = engine.index(index);
            if (traceEnabled) {
                logger.trace(
                    "index-done [{}] seq# [{}] allocation-id [{}] primaryTerm [{}] operationPrimaryTerm [{}] origin [{}] " +
                    "result-seq# [{}] result-term [{}] failure [{}]",
                    index.id(),
                    index.seqNo(),
                    routingEntry().allocationId(),
                    index.primaryTerm(),
                    getOperationPrimaryTerm(),
                    index.origin(),
                    result.getSeqNo(),
                    result.getTerm(),
                    result.getFailure());
            }
        } catch (Exception e) {
            if (traceEnabled) {
                logger.trace(new ParameterizedMessage(
                    "index-fail [{}] seq# [{}] allocation-id [{}] primaryTerm [{}] operationPrimaryTerm [{}] origin [{}]",
                    index.id(),
                    index.seqNo(),
                    routingEntry().allocationId(),
                    index.primaryTerm(),
                    getOperationPrimaryTerm(),
                    index.origin()
                ), e);
            }
            indexingOperationListeners.postIndex(shardId, index, e);
            throw e;
        }
        indexingOperationListeners.postIndex(shardId, index, result);
        return result;
    }

    private Engine.NoOpResult markSeqNoAsNoop(Engine engine, long seqNo, long opPrimaryTerm, String reason,
                                              Engine.Operation.Origin origin) throws IOException {
        assert opPrimaryTerm <= getOperationPrimaryTerm()
                : "op term [ " + opPrimaryTerm + " ] > shard term [" + getOperationPrimaryTerm() + "]";
        long startTime = System.nanoTime();
        ensureWriteAllowed(origin);
        final Engine.NoOp noOp = new Engine.NoOp(seqNo, opPrimaryTerm, origin, startTime, reason);
        return noOp(engine, noOp);
    }

    private Engine.NoOpResult noOp(Engine engine, Engine.NoOp noOp) throws IOException {
        active.set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("noop (seq# [{}])", noOp.seqNo());
        }
        return engine.noOp(noOp);
    }

    public Engine.IndexResult getFailedIndexResult(Exception e, long version) {
        return new Engine.IndexResult(e, version, getOperationPrimaryTerm());
    }

    public Engine.DeleteResult applyDeleteOperationOnPrimary(long version,
                                                             String id,
                                                             VersionType versionType,
                                                             long ifSeqNo,
                                                             long ifPrimaryTerm) throws IOException {
        return applyDeleteOperation(
            getEngine(),
            UNASSIGNED_SEQ_NO,
            getOperationPrimaryTerm(),
            version,
            id,
            versionType,
            ifSeqNo,
            ifPrimaryTerm,
            Engine.Operation.Origin.PRIMARY
        );
    }

    public Engine.DeleteResult applyDeleteOperationOnReplica(long seqNo,
                                                             long version,
                                                             String id) throws IOException {
        return applyDeleteOperation(
            getEngine(),
            seqNo,
            getOperationPrimaryTerm(),
            version,
            id,
            null,
            UNASSIGNED_SEQ_NO,
            0,
            Engine.Operation.Origin.REPLICA
        );
    }

    private Engine.DeleteResult applyDeleteOperation(Engine engine,
                                                     long seqNo,
                                                     long opPrimaryTerm,
                                                     long version,
                                                     String id,
                                                     @Nullable VersionType versionType,
                                                     long ifSeqNo,
                                                     long ifPrimaryTerm,
                                                     Engine.Operation.Origin origin) throws IOException {
        assert opPrimaryTerm <= getOperationPrimaryTerm()
                : "op term [ " + opPrimaryTerm + " ] > shard term [" + getOperationPrimaryTerm() + "]";
        ensureWriteAllowed(origin);
        final Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(id));
        final Engine.Delete delete = prepareDelete(
            id,
            uid,
            seqNo,
            opPrimaryTerm,
            version,
            versionType,
            origin,
            ifSeqNo,
            ifPrimaryTerm
        );
        return delete(engine, delete);
    }

    private Engine.Delete prepareDelete(String id,
                                        Term uid,
                                        long seqNo,
                                        long primaryTerm,
                                        long version,
                                        VersionType versionType,
                                        Engine.Operation.Origin origin,
                                        long ifSeqNo,
                                        long ifPrimaryTerm) {
        long startTime = System.nanoTime();
        return new Engine.Delete(
            id,
            uid,
            seqNo,
            primaryTerm,
            version,
            versionType,
            origin,
            startTime,
            ifSeqNo,
            ifPrimaryTerm
        );
    }

    private Engine.DeleteResult delete(Engine engine, Engine.Delete delete) throws IOException {
        active.set(true);
        final Engine.DeleteResult result;
        delete = indexingOperationListeners.preDelete(shardId, delete);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("delete [{}] (seq no [{}])", delete.uid().text(), delete.seqNo());
            }
            result = engine.delete(delete);
        } catch (Exception e) {
            indexingOperationListeners.postDelete(shardId, delete, e);
            throw e;
        }
        indexingOperationListeners.postDelete(shardId, delete, result);
        return result;
    }

    public Engine.GetResult get(Engine.Get get) {
        readAllowed();
        return getEngine().get(get, this::acquireSearcher);
    }

    /**
     * Writes all indexing changes to disk and opens a new searcher reflecting all changes.  This can throw {@link AlreadyClosedException}.
     */
    public void refresh(String source) {
        verifyNotClosed();
        if (logger.isTraceEnabled()) {
            logger.trace("refresh with source [{}]", source);
        }
        getEngine().refresh(source);
    }

    /**
     * Returns how many bytes we are currently moving from heap to disk
     */
    public long getWritingBytes() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        return engine.getWritingBytes();
    }

    public DocsStats docStats() {
        readAllowed();
        return getEngine().docStats();
    }

    /**
     * @return {@link CommitStats}
     * @throws AlreadyClosedException if shard is closed
     */
    public CommitStats commitStats() {
        return getEngine().commitStats();
    }

    /**
     * @return {@link SeqNoStats}
     * @throws AlreadyClosedException if shard is closed
     */
    public SeqNoStats seqNoStats() {
        return getEngine().getSeqNoStats(replicationTracker.getGlobalCheckpoint());
    }

    public TranslogStats translogStats() {
        return getEngine().getTranslogStats();
    }

    public StoreStats storeStats() {
        try {
            return store.stats();
        } catch (IOException e) {
            failShard("Failing shard because of exception during storeStats", e);
            throw new ElasticsearchException("io exception while building 'store stats'", e);
        }
    }

    public Engine.SyncedFlushResult syncFlush(String syncId, Engine.CommitId expectedCommitId) {
        verifyNotClosed();
        logger.trace("trying to sync flush. sync id [{}]. expected commit id [{}]]", syncId, expectedCommitId);
        return getEngine().syncFlush(syncId, expectedCommitId);
    }

    /**
     * Executes the given flush request against the engine.
     *
     * @param request the flush request
     * @return the commit ID
     */
    public Engine.CommitId flush(FlushRequest request) {
        final boolean waitIfOngoing = request.waitIfOngoing();
        final boolean force = request.force();
        logger.trace("flush with {}", request);
        /*
         * We allow flushes while recovery since we allow operations to happen while recovering and we want to keep the translog under
         * control (up to deletes, which we do not GC). Yet, we do not use flush internally to clear deletes and flush the index writer
         * since we use Engine#writeIndexingBuffer for this now.
         */
        verifyNotClosed();
        final long time = System.nanoTime();
        final Engine.CommitId commitId = getEngine().flush(force, waitIfOngoing);
        flushMetric.inc(System.nanoTime() - time);
        return commitId;
    }

    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.elasticsearch.index.translog.TranslogDeletionPolicy} for details
     */
    public void trimTranslog() {
        verifyNotClosed();
        final Engine engine = getEngine();
        engine.trimUnreferencedTranslogFiles();
    }

    /**
     * Rolls the tranlog generation and cleans unneeded.
     */
    private void rollTranslogGeneration() {
        final Engine engine = getEngine();
        engine.rollTranslogGeneration();
    }

    public void forceMerge(ForceMergeRequest forceMerge) throws IOException {
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("force merge with {}", forceMerge);
        }
        Engine engine = getEngine();
        engine.forceMerge(
            forceMerge.flush(),
            forceMerge.maxNumSegments(),
            forceMerge.onlyExpungeDeletes(),
            false,
            false,
            forceMerge.forceMergeUUID()
        );
    }

    /**
     * Upgrades the shard to the current version of Lucene and returns the minimum segment version
     */
    public org.apache.lucene.util.Version upgrade(UpgradeRequest upgrade) throws IOException {
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("upgrade with {}", upgrade);
        }
        org.apache.lucene.util.Version previousVersion = minimumCompatibleVersion();
        // we just want to upgrade the segments, not actually forge merge to a single segment
        final Engine engine = getEngine();
        engine.forceMerge(
            true,  // we need to flush at the end to make sure the upgrade is durable
            Integer.MAX_VALUE, // we just want to upgrade the segments, not actually optimize to a single segment
            false,
            true,
            upgrade.upgradeOnlyAncientSegments(),
            null
        );

        org.apache.lucene.util.Version version = minimumCompatibleVersion();
        if (logger.isTraceEnabled()) {
            logger.trace("upgraded segments for {} from version {} to version {}", shardId, previousVersion, version);
        }

        return version;
    }

    public org.apache.lucene.util.Version minimumCompatibleVersion() {
        org.apache.lucene.util.Version luceneVersion = null;
        for (Segment segment : getEngine().segments(false)) {
            if (luceneVersion == null || luceneVersion.onOrAfter(segment.getVersion())) {
                luceneVersion = segment.getVersion();
            }
        }
        return luceneVersion == null ? indexSettings.getIndexVersionCreated().luceneVersion : luceneVersion;
    }

    /**
     * Creates a new {@link IndexCommit} snapshot from the currently running engine. All resources referenced by this
     * commit won't be freed until the commit / snapshot is closed.
     *
     * @param flushFirst <code>true</code> if the index should first be flushed to disk / a low level lucene commit should be executed
     */
    public Engine.IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        final IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.CLOSED) {
            return getEngine().acquireLastIndexCommit(flushFirst);
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    /**
     * Snapshots the most recent safe index commit from the currently running engine.
     * All index files referenced by this index commit won't be freed until the commit/snapshot is closed.
     */
    public Engine.IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        final IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.CLOSED) {
            return getEngine().acquireSafeIndexCommit();
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    /**
     * gets a {@link Store.MetadataSnapshot} for the current directory. This method is safe to call in all lifecycle of the index shard,
     * without having to worry about the current state of the engine and concurrent flushes.
     *
     * @throws org.apache.lucene.index.IndexNotFoundException     if no index is found in the current directory
     * @throws org.apache.lucene.index.CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum
     *                                                            mismatch or an unexpected exception when opening the index reading the
     *                                                            segments file.
     * @throws org.apache.lucene.index.IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws org.apache.lucene.index.IndexFormatTooNewException if the lucene index is too new to be opened.
     * @throws java.io.FileNotFoundException                      if one or more files referenced by a commit are not present.
     * @throws java.nio.file.NoSuchFileException                  if one or more files referenced by a commit are not present.
     */
    public Store.MetadataSnapshot snapshotStoreMetadata() throws IOException {
        Engine.IndexCommitRef indexCommit = null;
        store.incRef();
        try {
            Engine engine;
            synchronized (mutex) {
                // if the engine is not running, we can access the store directly, but we need to make sure no one starts
                // the engine on us. If the engine is running, we can get a snapshot via the deletion policy which is initialized.
                // That can be done out of mutex, since the engine can be closed half way.
                engine = getEngineOrNull();
                if (engine == null) {
                    return store.getMetadata(null, true);
                }
            }
            indexCommit = engine.acquireLastIndexCommit(false);
            return store.getMetadata(indexCommit.getIndexCommit());
        } finally {
            store.decRef();
            IOUtils.close(indexCommit);
        }
    }

    /**
     * Fails the shard and marks the shard store as corrupted if
     * <code>e</code> is caused by index corruption
     */
    public void failShard(String reason, @Nullable Exception e) {
        // fail the engine. This will cause this shard to also be removed from the node's index service.
        getEngine().failEngine(reason, e);
    }

    public Engine.Searcher acquireSearcher(String source) {
        return acquireSearcher(source, Engine.SearcherScope.EXTERNAL);
    }

    private Engine.Searcher acquireSearcher(String source, Engine.SearcherScope scope) {
        readAllowed();
        final Engine engine = getEngine();
        final Engine.Searcher searcher = engine.acquireSearcher(source, scope);
        boolean success = false;
        try {
            final Engine.Searcher newSearcher = readerWrapper == null ? searcher : wrapSearcher(searcher, readerWrapper);
            assert newSearcher != null;
            success = true;
            return newSearcher;
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to wrap searcher", ex);
        } finally {
            if (success == false) {
                Releasables.close(success, searcher);
            }
        }
    }

    static Engine.Searcher wrapSearcher(Engine.Searcher engineSearcher,
                                        CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper) throws IOException {
        assert readerWrapper != null;
        final ElasticsearchDirectoryReader elasticsearchDirectoryReader =
            ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(engineSearcher.getDirectoryReader());
        if (elasticsearchDirectoryReader == null) {
            throw new IllegalStateException("Can't wrap non elasticsearch directory reader");
        }
        NonClosingReaderWrapper nonClosingReaderWrapper = new NonClosingReaderWrapper(engineSearcher.getDirectoryReader());
        DirectoryReader reader = readerWrapper.apply(nonClosingReaderWrapper);
        if (reader != nonClosingReaderWrapper) {
            if (reader.getReaderCacheHelper() != elasticsearchDirectoryReader.getReaderCacheHelper()) {
                throw new IllegalStateException("wrapped directory reader doesn't delegate IndexReader#getCoreCacheKey," +
                    " wrappers must override this method and delegate to the original readers core cache key. Wrapped readers can't be " +
                    "used as cache keys since their are used only per request which would lead to subtle bugs");
            }
            if (ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(reader) != elasticsearchDirectoryReader) {
                // prevent that somebody wraps with a non-filter reader
                throw new IllegalStateException("wrapped directory reader hides actual ElasticsearchDirectoryReader but shouldn't");
            }
        }

        if (reader == nonClosingReaderWrapper) {
            return engineSearcher;
        } else {
            // we close the reader to make sure wrappers can release resources if needed....
            // our NonClosingReaderWrapper makes sure that our reader is not closed
            return new Engine.Searcher(
                engineSearcher.source(),
                reader,
                engineSearcher.getQueryCache(),
                engineSearcher.getQueryCachingPolicy(),
                () -> IOUtils.close(reader, engineSearcher)  // this will close the wrappers excluding the NonClosingReaderWrapper
            );
        }
    }

    private static final class NonClosingReaderWrapper extends FilterDirectoryReader {

        private NonClosingReaderWrapper(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return reader;
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new NonClosingReaderWrapper(in);
        }

        @Override
        protected void doClose() throws IOException {
            // don't close here - mimic the MultiReader#doClose = false behavior that FilterDirectoryReader doesn't have
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

    }

    public void close(String reason, boolean flushEngine) throws IOException {
        synchronized (mutex) {
            try {
                changeState(IndexShardState.CLOSED, reason);
            } finally {
                final Engine engine = this.currentEngineReference.getAndSet(null);
                try {
                    if (engine != null && flushEngine) {
                        engine.flushAndClose();
                    }
                } finally {
                    // playing safe here and close the engine even if the above succeeds - close can be called multiple times
                    // Also closing refreshListeners to prevent us from accumulating any more listeners
                    IOUtils.close(engine, globalCheckpointListeners, refreshListeners);
                    indexShardOperationPermits.close();
                }
            }
        }
    }

    public IndexShard postRecovery(String reason) throws IndexShardStartedException, IndexShardRelocatedException, IndexShardClosedException {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            // we need to refresh again to expose all operations that were index until now. Otherwise
            // we may not expose operations that were indexed with a refresh listener that was immediately
            // responded to in addRefreshListener.
            getEngine().refresh("post_recovery");
            recoveryState.setStage(RecoveryState.Stage.DONE);
            changeState(IndexShardState.POST_RECOVERY, reason);
        }
        return this;
    }

    /**
     * called before starting to copy index files over
     */
    public void prepareForIndexRecovery() {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.setStage(RecoveryState.Stage.INDEX);
        assert currentEngineReference.get() == null;
    }

    public void trimOperationOfPreviousPrimaryTerms(long aboveSeqNo) {
        getEngine().trimOperationsFromTranslog(getOperationPrimaryTerm(), aboveSeqNo);
    }

    /**
     * Returns the maximum auto_id_timestamp of all append-only requests have been processed by this shard or the auto_id_timestamp received
     * from the primary via {@link #updateMaxUnsafeAutoIdTimestamp(long)} at the beginning of a peer-recovery or a primary-replica resync.
     *
     * @see #updateMaxUnsafeAutoIdTimestamp(long)
     */
    public long getMaxSeenAutoIdTimestamp() {
        return getEngine().getMaxSeenAutoIdTimestamp();
    }

    /**
     * Since operations stored in soft-deletes do not have max_auto_id_timestamp, the primary has to propagate its max_auto_id_timestamp
     * (via {@link #getMaxSeenAutoIdTimestamp()} of all processed append-only requests to replicas at the beginning of a peer-recovery
     * or a primary-replica resync to force a replica to disable optimization for all append-only requests which are replicated via
     * replication while its retry variants are replicated via recovery without auto_id_timestamp.
     * <p>
     * Without this force-update, a replica can generate duplicate documents (for the same id) if it first receives
     * a retry append-only (without timestamp) via recovery, then an original append-only (with timestamp) via replication.
     */
    public void updateMaxUnsafeAutoIdTimestamp(long maxSeenAutoIdTimestampFromPrimary) {
        getEngine().updateMaxUnsafeAutoIdTimestamp(maxSeenAutoIdTimestampFromPrimary);
    }

    public Engine.Result applyTranslogOperation(Translog.Operation operation, Engine.Operation.Origin origin) throws IOException {
        return applyTranslogOperation(getEngine(), operation, origin);
    }

    private Engine.Result applyTranslogOperation(Engine engine, Translog.Operation operation,
                                                 Engine.Operation.Origin origin) throws IOException {
        // If a translog op is replayed on the primary (eg. ccr), we need to use external instead of null for its version type.
        final VersionType versionType = (origin == Engine.Operation.Origin.PRIMARY) ? VersionType.EXTERNAL : null;
        final Engine.Result result;
        switch (operation.opType()) {
            case INDEX:
                final Translog.Index index = (Translog.Index) operation;
                // we set canHaveDuplicates to true all the time such that we de-optimze the translog case and ensure that all
                // autoGeneratedID docs that are coming from the primary are updated correctly.
                result = applyIndexOperation(
                    engine,
                    index.seqNo(),
                    index.primaryTerm(),
                    index.version(),
                    versionType,
                    UNASSIGNED_SEQ_NO,
                    0,
                    index.getAutoGeneratedIdTimestamp(),
                    true,
                    origin,
                    new SourceToParse(
                        shardId.getIndexName(),
                        index.id(),
                        index.source(),
                        XContentHelper.xContentType(index.source()), index.routing())
                );
                break;
            case DELETE:
                final Translog.Delete delete = (Translog.Delete) operation;
                result = applyDeleteOperation(
                    engine,
                    delete.seqNo(),
                    delete.primaryTerm(),
                    delete.version(),
                    delete.id(),
                    versionType,
                    UNASSIGNED_SEQ_NO,
                    0,
                    origin
                );
                break;
            case NO_OP:
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                result = markSeqNoAsNoop(engine, noOp.seqNo(), noOp.primaryTerm(), noOp.reason(), origin);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
        return result;
    }

    /**
     * Replays translog operations from the provided translog {@code snapshot} to the current engine using the given {@code origin}.
     * The callback {@code onOperationRecovered} is notified after each translog operation is replayed successfully.
     */
    int runTranslogRecovery(Engine engine, Translog.Snapshot snapshot, Engine.Operation.Origin origin,
                            Runnable onOperationRecovered) throws IOException {
        int opsRecovered = 0;
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            try {
                logger.trace("[translog] recover op {}", operation);
                Engine.Result result = applyTranslogOperation(engine, operation, origin);
                switch (result.getResultType()) {
                    case FAILURE:
                        throw result.getFailure();
                    case MAPPING_UPDATE_REQUIRED:
                        throw new IllegalArgumentException("unexpected mapping update: " + result.getRequiredMappingUpdate());
                    case SUCCESS:
                        break;
                    default:
                        throw new AssertionError("Unknown result type [" + result.getResultType() + "]");
                }

                opsRecovered++;
                onOperationRecovered.run();
            } catch (Exception e) {
                if (ExceptionsHelper.status(e) == RestStatus.BAD_REQUEST) {
                    // mainly for MapperParsingException and Failure to detect xcontent
                    logger.info("ignoring recovery of a corrupt translog entry", e);
                } else {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }
        }
        return opsRecovered;
    }

    /**
     * opens the engine on top of the existing lucene engine and translog.
     * Operations from the translog will be replayed to bring lucene up to date.
     **/
    public void openEngineAndRecoverFromTranslog() throws IOException {
        final RecoveryState.Translog translogRecoveryStats = recoveryState.getTranslog();
        final Engine.TranslogRecoveryRunner translogRecoveryRunner = (engine, snapshot) -> {
            translogRecoveryStats.totalOperations(snapshot.totalOperations());
            translogRecoveryStats.totalOperationsOnStart(snapshot.totalOperations());
            return runTranslogRecovery(engine, snapshot, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                translogRecoveryStats::incrementRecoveredOperations);
        };
        innerOpenEngineAndTranslog();
        getEngine().recoverFromTranslog(translogRecoveryRunner, Long.MAX_VALUE);
    }

    /**
     * Opens the engine on top of the existing lucene engine and translog.
     * The translog is kept but its operations won't be replayed.
     */
    public void openEngineAndSkipTranslogRecovery() throws IOException {
        innerOpenEngineAndTranslog();
        getEngine().skipTranslogRecovery();
    }

    private void innerOpenEngineAndTranslog() throws IOException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);
        // also check here, before we apply the translog
        if (Booleans.isTrue(checkIndexOnStartup) || "checksum".equals(checkIndexOnStartup)) {
            try {
                checkIndex();
            } catch (IOException ex) {
                throw new RecoveryFailedException(recoveryState, "check index failed", ex);
            }
        }
        recoveryState.setStage(RecoveryState.Stage.TRANSLOG);

        final EngineConfig config = newEngineConfig();

        // we disable deletes since we allow for operations to be executed against the shard while recovering
        // but we need to make sure we don't loose deletes until we are done recovering
        config.setEnableGcDeletes(false);
        // we have to set it before we open an engine and recover from the translog because
        // acquiring a snapshot from the translog causes a sync which causes the global checkpoint to be pulled in,
        // and an engine can be forced to close in ctor which also causes the global checkpoint to be pulled in.
        final String translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY);
        final long globalCheckpoint = Translog.readGlobalCheckpoint(translogConfig.getTranslogPath(), translogUUID);
        replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, "read from translog checkpoint");
        updateRetentionLeasesOnReplica(loadRetentionLeases());
        assert recoveryState.getRecoverySource().expectEmptyRetentionLeases() == false || getRetentionLeases().leases().isEmpty()
            : "expected empty set of retention leases with recovery source [" + recoveryState.getRecoverySource()
            + "] but got " + getRetentionLeases();

        createNewEngine(config);
        verifyNotClosed();
        // We set active because we are now writing operations to the engine; this way, if we go idle after some time and become inactive,
        // we still give sync'd flush a chance to run:
        active.set(true);
        assertSequenceNumbersInCommit();
        assert recoveryState.getStage() == RecoveryState.Stage.TRANSLOG : "TRANSLOG stage expected but was: " + recoveryState.getStage();
    }

    private boolean assertSequenceNumbersInCommit() throws IOException {
        final Map<String, String> userData = SegmentInfos.readLatestCommit(store.directory()).getUserData();
        assert userData.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) : "commit point doesn't contains a local checkpoint";
        assert userData.containsKey(SequenceNumbers.MAX_SEQ_NO) : "commit point doesn't contains a maximum sequence number";
        assert userData.containsKey(Engine.HISTORY_UUID_KEY) : "commit point doesn't contains a history uuid";
        assert userData.get(Engine.HISTORY_UUID_KEY).equals(getHistoryUUID()) : "commit point history uuid ["
            + userData.get(Engine.HISTORY_UUID_KEY) + "] is different than engine [" + getHistoryUUID() + "]";
        // as of 5.5.0, the engine stores the maxUnsafeAutoIdTimestamp in the commit point.
        // This should have baked into the commit by the primary we recover from, regardless of the index age.
        assert userData.containsKey(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID) :
            "opening index which was created post 5.5.0 but " + Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID
                + " is not found in commit";
        return true;
    }

    protected void onNewEngine(Engine newEngine) {
        refreshListeners.setCurrentRefreshLocationSupplier(newEngine::getTranslogLastWriteLocation);
    }

    /**
     * called if recovery has to be restarted after network error / delay **
     */
    public void performRecoveryRestart() throws IOException {
        synchronized (mutex) {
            if (state != IndexShardState.RECOVERING) {
                throw new IndexShardNotRecoveringException(shardId, state);
            }
            assert refreshListeners.pendingCount() == 0 : "we can't restart with pending listeners";
            final Engine engine = this.currentEngineReference.getAndSet(null);
            IOUtils.close(engine);
            recoveryState().setStage(RecoveryState.Stage.INIT);
        }
    }

    /**
     * returns stats about ongoing recoveries, both source and target
     */
    public RecoveryStats recoveryStats() {
        return recoveryStats;
    }

    /**
     * Returns the current {@link RecoveryState} if this shard is recovering or has been recovering.
     * Returns null if the recovery has not yet started or shard was not recovered (created via an API).
     */
    @Override
    public RecoveryState recoveryState() {
        return this.recoveryState;
    }

    /**
     * perform the last stages of recovery once all translog operations are done.
     * note that you should still call {@link #postRecovery(String)}.
     */
    public void finalizeRecovery() {
        recoveryState().setStage(RecoveryState.Stage.FINALIZE);
        Engine engine = getEngine();
        engine.refresh("recovery_finalization");
        engine.config().setEnableGcDeletes(true);
    }

    /**
     * Returns {@code true} if this shard can ignore a recovery attempt made to it (since the already doing/done it)
     */
    public boolean ignoreRecoveryAttempt() {
        IndexShardState state = state(); // one time volatile read
        return state == IndexShardState.POST_RECOVERY || state == IndexShardState.RECOVERING || state == IndexShardState.STARTED ||
            state == IndexShardState.CLOSED;
    }

    public void readAllowed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (READ_ALLOWED_STATES.contains(state) == false) {
            throw new IllegalIndexShardStateException(shardId, state, "operations only allowed when shard state is one of " + READ_ALLOWED_STATES.toString());
        }
    }

    /** returns true if the {@link IndexShardState} allows reading */
    public boolean isReadAllowed() {
        return READ_ALLOWED_STATES.contains(state);
    }

    private void ensureWriteAllowed(Engine.Operation.Origin origin) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read

        if (origin.isRecovery()) {
            if (state != IndexShardState.RECOVERING) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when recovering, origin [" + origin + "]");
            }
        } else {
            if (origin == Engine.Operation.Origin.PRIMARY) {
                assert assertPrimaryMode();
            } else if (origin == Engine.Operation.Origin.REPLICA) {
                assert assertReplicationTarget();
            } else {
                assert origin == Engine.Operation.Origin.LOCAL_RESET;
                assert getActiveOperationsCount() == OPERATIONS_BLOCKED
                    : "locally resetting without blocking operations, active operations are [" + getActiveOperations() + "]";
            }
            if (WRITE_ALLOWED_STATES.contains(state) == false) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard state is one of " + WRITE_ALLOWED_STATES + ", origin [" + origin + "]");
            }
        }
    }

    private boolean assertPrimaryMode() {
        assert shardRouting.primary() && replicationTracker.isPrimaryMode() : "shard " + shardRouting + " is not a primary shard in primary mode";
        return true;
    }

    private boolean assertReplicationTarget() {
        assert replicationTracker.isPrimaryMode() == false : "shard " + shardRouting + " in primary mode cannot be a replication target";
        return true;
    }

    private void verifyNotClosed() throws IllegalIndexShardStateException {
        verifyNotClosed(null);
    }

    private void verifyNotClosed(Exception suppressed) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state == IndexShardState.CLOSED) {
            final IllegalIndexShardStateException exc = new IndexShardClosedException(shardId, "operation only allowed when not closed");
            if (suppressed != null) {
                exc.addSuppressed(suppressed);
            }
            throw exc;
        }
    }

    protected final void verifyActive() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED) {
            throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard is active");
        }
    }

    /**
     * Returns number of heap bytes used by the indexing buffer for this shard, or 0 if the shard is closed
     */
    public long getIndexBufferRAMBytesUsed() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        try {
            return engine.getIndexBufferRAMBytesUsed();
        } catch (AlreadyClosedException ex) {
            return 0;
        }
    }

    public void addShardFailureCallback(Consumer<ShardFailure> onShardFailure) {
        this.shardEventListener.delegates.add(onShardFailure);
    }

    /**
     * Called by {@link IndexingMemoryController} to check whether more than {@code inactiveTimeNS} has passed since the last
     * indexing operation, and notify listeners that we are now inactive so e.g. sync'd flush can happen.
     */
    public void checkIdle(long inactiveTimeNS) {
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null && System.nanoTime() - engineOrNull.getLastWriteNanos() >= inactiveTimeNS) {
            boolean wasActive = active.getAndSet(false);
            if (wasActive) {
                logger.debug("shard is now inactive");
                try {
                    indexEventListener.onShardInactive(this);
                } catch (Exception e) {
                    logger.warn("failed to notify index event listener", e);
                }
            }
        }
    }

    public boolean isActive() {
        return active.get();
    }

    public ShardPath shardPath() {
        return path;
    }

    public boolean recoverFromLocalShards(BiConsumer<String, MappingMetadata> mappingUpdateConsumer, List<IndexShard> localShards) throws IOException {
        assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
        assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS : "invalid recovery type: " + recoveryState.getRecoverySource();
        final List<LocalShardSnapshot> snapshots = new ArrayList<>();
        try {
            for (IndexShard shard : localShards) {
                snapshots.add(new LocalShardSnapshot(shard));
            }

            // we are the first primary, recover from the gateway
            // if its post api allocation, the index should exists
            assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
            StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
            return storeRecovery.recoverFromLocalShards(mappingUpdateConsumer, this, snapshots);
        } finally {
            IOUtils.close(snapshots);
        }
    }

    public boolean recoverFromStore() {
        // we are the first primary, recover from the gateway
        // if its post api allocation, the index should exists
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        assert shardRouting.initializing() : "can only start recovery on initializing shard";
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromStore(this);
    }

    public boolean restoreFromRepository(Repository repository) {
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT : "invalid recovery type: " + recoveryState.getRecoverySource();
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromRepository(this, repository);
    }

    /**
     * Tests whether or not the engine should be flushed periodically.
     * This test is based on the current size of the translog compared to the configured flush threshold size.
     *
     * @return {@code true} if the engine should be flushed
     */
    boolean shouldPeriodicallyFlush() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                return engine.shouldPeriodicallyFlush();
            } catch (final AlreadyClosedException e) {
                // we are already closed, no need to flush or roll
            }
        }
        return false;
    }

    /**
     * Tests whether or not the translog generation should be rolled to a new generation. This test is based on the size of the current
     * generation compared to the configured generation threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    boolean shouldRollTranslogGeneration() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                return engine.shouldRollTranslogGeneration();
            } catch (final AlreadyClosedException e) {
                // we are already closed, no need to flush or roll
            }
        }
        return false;
    }

    public void onSettingsChanged() {
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null) {
            engineOrNull.onSettingsChanged();
        }
    }

    /**
     * Acquires a lock on the translog files and Lucene soft-deleted documents to prevent them from being trimmed
     */
    public Closeable acquireRetentionLock() {
        return getEngine().acquireRetentionLock();
    }

    /**
     * Returns the estimated number of history operations whose seq# at least the provided seq# in this shard.
     */
    public int estimateNumberOfHistoryOperations(String source, long startingSeqNo) throws IOException {
        return getEngine().estimateNumberOfHistoryOperations(source, mapperService, startingSeqNo);
    }

    /**
     * Creates a new history snapshot for reading operations since the provided starting seqno (inclusive).
     * The returned snapshot can be retrieved from either Lucene index or translog files.
     */
    public Translog.Snapshot getHistoryOperations(String source, long startingSeqNo) throws IOException {
        return getEngine().readHistoryOperations(source, mapperService, startingSeqNo);
    }

    /**
     * Checks if we have a completed history of operations since the given starting seqno (inclusive).
     * This method should be called after acquiring the retention lock; See {@link #acquireRetentionLock()}
     */
    public boolean hasCompleteHistoryOperations(String source, long startingSeqNo) throws IOException {
        return getEngine().hasCompleteOperationHistory(source, mapperService, startingSeqNo);
    }

    /**
     * Gets the minimum retained sequence number for this engine.
     *
     * @return the minimum retained sequence number
     */
    public long getMinRetainedSeqNo() {
        return getEngine().getMinRetainedSeqNo();
    }

    /**
     * Creates a new changes snapshot for reading operations whose seq_no are between {@code fromSeqNo}(inclusive)
     * and {@code toSeqNo}(inclusive). The caller has to close the returned snapshot after finishing the reading.
     *
     * @param source            the source of the request
     * @param fromSeqNo         the from seq_no (inclusive) to read
     * @param toSeqNo           the to seq_no (inclusive) to read
     * @param requiredFullRange if {@code true} then {@link Translog.Snapshot#next()} will throw {@link IllegalStateException}
     *                          if any operation between {@code fromSeqNo} and {@code toSeqNo} is missing.
     *                          This parameter should be only enabled when the entire requesting range is below the global checkpoint.
     */
    public Translog.Snapshot newChangesSnapshot(String source, long fromSeqNo, long toSeqNo, boolean requiredFullRange) throws IOException {
        return getEngine().newChangesSnapshot(source, mapperService, fromSeqNo, toSeqNo, requiredFullRange);
    }

    public List<Segment> segments(boolean verbose) {
        return getEngine().segments(verbose);
    }

    public void flushAndCloseEngine() throws IOException {
        getEngine().flushAndClose();
    }

    public String getHistoryUUID() {
        return getEngine().getHistoryUUID();
    }

    public IndexEventListener getIndexEventListener() {
        return indexEventListener;
    }

    public void activateThrottling() {
        try {
            getEngine().activateThrottling();
        } catch (AlreadyClosedException ex) {
            // ignore
        }
    }

    public void deactivateThrottling() {
        try {
            getEngine().deactivateThrottling();
        } catch (AlreadyClosedException ex) {
            // ignore
        }
    }

    private void handleRefreshException(Exception e) {
        if (e instanceof AlreadyClosedException) {
            // ignore
        } else if (e instanceof RefreshFailedEngineException) {
            RefreshFailedEngineException rfee = (RefreshFailedEngineException) e;
            if (rfee.getCause() instanceof InterruptedException) {
                // ignore, we are being shutdown
            } else if (rfee.getCause() instanceof ClosedByInterruptException) {
                // ignore, we are being shutdown
            } else if (rfee.getCause() instanceof ThreadInterruptedException) {
                // ignore, we are being shutdown
            } else {
                if (state != IndexShardState.CLOSED) {
                    logger.warn("Failed to perform engine refresh", e);
                }
            }
        } else {
            if (state != IndexShardState.CLOSED) {
                logger.warn("Failed to perform engine refresh", e);
            }
        }
    }

    /**
     * Called when our shard is using too much heap and should move buffered indexed/deleted documents to disk.
     */
    public void writeIndexingBuffer() {
        try {
            Engine engine = getEngine();
            engine.writeIndexingBuffer();
        } catch (Exception e) {
            handleRefreshException(e);
        }
    }

    /**
     * Notifies the service to update the local checkpoint for the shard with the provided allocation ID. See
     * {@link ReplicationTracker#updateLocalCheckpoint(String, long)} for
     * details.
     *
     * @param allocationId the allocation ID of the shard to update the local checkpoint for
     * @param checkpoint   the local checkpoint for the shard
     */
    public void updateLocalCheckpointForShard(final String allocationId, final long checkpoint) {
        assert assertPrimaryMode();
        verifyNotClosed();
        replicationTracker.updateLocalCheckpoint(allocationId, checkpoint);
    }

    /**
     * Update the local knowledge of the persisted global checkpoint for the specified allocation ID.
     *
     * @param allocationId     the allocation ID to update the global checkpoint for
     * @param globalCheckpoint the global checkpoint
     */
    public void updateGlobalCheckpointForShard(final String allocationId, final long globalCheckpoint) {
        assert assertPrimaryMode();
        verifyNotClosed();
        replicationTracker.updateGlobalCheckpointForShard(allocationId, globalCheckpoint);
    }

    /**
     * Add a global checkpoint listener. If the global checkpoint is equal to or above the global checkpoint the listener is waiting for,
     * then the listener will be notified immediately via an executor (so possibly not on the current thread). If the specified timeout
     * elapses before the listener is notified, the listener will be notified with an {@link TimeoutException}. A caller may pass null to
     * specify no timeout.
     *
     * @param waitingForGlobalCheckpoint the global checkpoint the listener is waiting for
     * @param listener                   the listener
     * @param timeout                    the timeout
     */
    public void addGlobalCheckpointListener(
            final long waitingForGlobalCheckpoint,
            final GlobalCheckpointListeners.GlobalCheckpointListener listener,
            final TimeValue timeout) {
        this.globalCheckpointListeners.add(waitingForGlobalCheckpoint, listener, timeout);
    }

    private void ensureSoftDeletesEnabled(String feature) {
        if (indexSettings.isSoftDeleteEnabled() == false) {
            String message = feature + " requires soft deletes but " + indexSettings.getIndex() + " does not have soft deletes enabled";
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

    /**
     * Get all non-expired retention leases tracked on this shard.
     *
     * @return the retention leases
     */
    public RetentionLeases getRetentionLeases() {
        return getRetentionLeases(false).v2();
    }

    /**
     * If the expire leases parameter is false, gets all retention leases tracked on this shard and otherwise first calculates
     * expiration of existing retention leases, and then gets all non-expired retention leases tracked on this shard. Note that only the
     * primary shard calculates which leases are expired, and if any have expired, syncs the retention leases to any replicas. If the
     * expire leases parameter is true, this replication tracker must be in primary mode.
     *
     * @return a tuple indicating whether or not any retention leases were expired, and the non-expired retention leases
     */
    public Tuple<Boolean, RetentionLeases> getRetentionLeases(final boolean expireLeases) {
        assert expireLeases == false || assertPrimaryMode();
        verifyNotClosed();
        return replicationTracker.getRetentionLeases(expireLeases);
    }

    public RetentionLeaseStats getRetentionLeaseStats() {
        verifyNotClosed();
        return new RetentionLeaseStats(getRetentionLeases());
    }

    /**
     * Adds a new retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @param listener                the callback when the retention lease is successfully added and synced to replicas
     * @return the new retention lease
     * @throws IllegalArgumentException if the specified retention lease already exists
     */
    public RetentionLease addRetentionLease(
            final String id,
            final long retainingSequenceNumber,
            final String source,
            final ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(listener);
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled("retention leases");
        try (Closeable ignore = acquireRetentionLock()) {
            final long actualRetainingSequenceNumber =
                    retainingSequenceNumber == RETAIN_ALL ? getMinRetainedSeqNo() : retainingSequenceNumber;
            return replicationTracker.addRetentionLease(id, actualRetainingSequenceNumber, source, listener);
        } catch (final IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Renews an existing retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @return the renewed retention lease
     * @throws IllegalArgumentException if the specified retention lease does not exist
     */
    public RetentionLease renewRetentionLease(final String id, final long retainingSequenceNumber, final String source) {
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled("retention leases");
        try (Closeable ignore = acquireRetentionLock()) {
            final long actualRetainingSequenceNumber =
                    retainingSequenceNumber == RETAIN_ALL ? getMinRetainedSeqNo() : retainingSequenceNumber;
            return replicationTracker.renewRetentionLease(id, actualRetainingSequenceNumber, source);
        } catch (final IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Removes an existing retention lease.
     *
     * @param id       the identifier of the retention lease
     * @param listener the callback when the retention lease is successfully removed and synced to replicas
     */
    public void removeRetentionLease(final String id, final ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(listener);
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled("retention leases");
        replicationTracker.removeRetentionLease(id, listener);
    }

    /**
     * Updates retention leases on a replica.
     *
     * @param retentionLeases the retention leases
     */
    public void updateRetentionLeasesOnReplica(final RetentionLeases retentionLeases) {
        assert assertReplicationTarget();
        verifyNotClosed();
        replicationTracker.updateRetentionLeasesOnReplica(retentionLeases);
    }

    /**
     * Loads the latest retention leases from their dedicated state file.
     *
     * @return the retention leases
     * @throws IOException if an I/O exception occurs reading the retention leases
     */
    public RetentionLeases loadRetentionLeases() throws IOException {
        verifyNotClosed();
        return replicationTracker.loadRetentionLeases(path.getShardStatePath());
    }

    /**
     * Persists the current retention leases to their dedicated state file.
     *
     * @throws WriteStateException if an exception occurs writing the state file
     */
    public void persistRetentionLeases() throws WriteStateException {
        verifyNotClosed();
        replicationTracker.persistRetentionLeases(path.getShardStatePath());
    }

    public boolean assertRetentionLeasesPersisted() throws IOException {
        return replicationTracker.assertRetentionLeasesPersisted(path.getShardStatePath());
    }

    /**
     * Syncs the current retention leases to all replicas.
     */
    public void syncRetentionLeases() {
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled("retention leases");
        final Tuple<Boolean, RetentionLeases> retentionLeases = getRetentionLeases(true);
        if (retentionLeases.v1()) {
            logger.trace("syncing retention leases [{}] after expiration check", retentionLeases.v2());
            retentionLeaseSyncer.sync(
                shardId,
                retentionLeases.v2(),
                ActionListener.wrap(
                    r -> {},
                    e -> logger.warn(
                        new ParameterizedMessage(
                            "failed to sync retention leases [{}] after expiration check", retentionLeases),
                        e
                    )
                )
            );
        } else {
            logger.trace("background syncing retention leases [{}] after expiration check", retentionLeases.v2());
            retentionLeaseSyncer.backgroundSync(shardId, retentionLeases.v2());
        }
    }

    /**
     * Called when the recovery process for a shard has opened the engine on the target shard. Ensures that the right data structures
     * have been set up locally to track local checkpoint information for the shard and that the shard is added to the replication group.
     *
     * @param allocationId  the allocation ID of the shard for which recovery was initiated
     */
    public void initiateTracking(final String allocationId) {
        assert assertPrimaryMode();
        replicationTracker.initiateTracking(allocationId);
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. See
     * {@link ReplicationTracker#markAllocationIdAsInSync(String, long)}
     * for additional details.
     *
     * @param allocationId    the allocation ID of the shard to mark as in-sync
     * @param localCheckpoint the current local checkpoint on the shard
     */
    public void markAllocationIdAsInSync(final String allocationId, final long localCheckpoint) throws InterruptedException {
        assert assertPrimaryMode();
        replicationTracker.markAllocationIdAsInSync(allocationId, localCheckpoint);
    }

    /**
     * Returns the persisted local checkpoint for the shard.
     *
     * @return the local checkpoint
     */
    public long getLocalCheckpoint() {
        return getEngine().getPersistedLocalCheckpoint();
    }

    /**
     * Returns the global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public long getLastKnownGlobalCheckpoint() {
        return replicationTracker.getGlobalCheckpoint();
    }

    /**
     * Returns the latest global checkpoint value that has been persisted in the underlying storage (i.e. translog's checkpoint)
     */
    public long getLastSyncedGlobalCheckpoint() {
        return getEngine().getLastSyncedGlobalCheckpoint();
    }

    /**
     * Get the local knowledge of the global checkpoints for all in-sync allocation IDs.
     *
     * @return a map from allocation ID to the local knowledge of the global checkpoint for that allocation ID
     */
    public ObjectLongMap<String> getInSyncGlobalCheckpoints() {
        assert assertPrimaryMode();
        verifyNotClosed();
        return replicationTracker.getInSyncGlobalCheckpoints();
    }

    /**
     * Syncs the global checkpoint to the replicas if the global checkpoint on at least one replica is behind the global checkpoint on the
     * primary.
     */
    public void maybeSyncGlobalCheckpoint(final String reason) {
        verifyNotClosed();
        assert shardRouting.primary() : "only call maybeSyncGlobalCheckpoint on primary shard";
        if (replicationTracker.isPrimaryMode() == false) {
            return;
        }
        assert assertPrimaryMode();
        // only sync if there are no operations in flight, or when using async durability
        final SeqNoStats stats = getEngine().getSeqNoStats(replicationTracker.getGlobalCheckpoint());
        final boolean asyncDurability = indexSettings().getTranslogDurability() == Translog.Durability.ASYNC;
        if (stats.getMaxSeqNo() == stats.getGlobalCheckpoint() || asyncDurability) {
            final ObjectLongMap<String> globalCheckpoints = getInSyncGlobalCheckpoints();
            final long globalCheckpoint = replicationTracker.getGlobalCheckpoint();
            // async durability means that the local checkpoint might lag (as it is only advanced on fsync)
            // periodically ask for the newest local checkpoint by syncing the global checkpoint, so that ultimately the global
            // checkpoint can be synced. Also take into account that a shard might be pending sync, which means that it isn't
            // in the in-sync set just yet but might be blocked on waiting for its persisted local checkpoint to catch up to
            // the global checkpoint.
            final boolean syncNeeded =
                (asyncDurability && (stats.getGlobalCheckpoint() < stats.getMaxSeqNo()))
                    // check if the persisted global checkpoint
                    || StreamSupport
                            .stream(globalCheckpoints.values().spliterator(), false)
                            .anyMatch(v -> v.value < globalCheckpoint);
            // only sync if index is not closed and there is a shard lagging the primary
            if (syncNeeded && indexSettings.getIndexMetadata().getState() == IndexMetadata.State.OPEN) {
                logger.trace("syncing global checkpoint for [{}]", reason);
                globalCheckpointSyncer.run();
            }
        }
    }

    /**
     * Returns the current replication group for the shard.
     *
     * @return the replication group
     */
    public ReplicationGroup getReplicationGroup() {
        assert assertPrimaryMode();
        verifyNotClosed();
        return replicationTracker.getReplicationGroup();
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param globalCheckpoint the global checkpoint
     * @param reason           the reason the global checkpoint was updated
     */
    public void updateGlobalCheckpointOnReplica(final long globalCheckpoint, final String reason) {
        assert assertReplicationTarget();
        final long localCheckpoint = getLocalCheckpoint();
        if (globalCheckpoint > localCheckpoint) {
            /*
             * This can happen during recovery when the shard has started its engine but recovery is not finalized and is receiving global
             * checkpoint updates. However, since this shard is not yet contributing to calculating the global checkpoint, it can be the
             * case that the global checkpoint update from the primary is ahead of the local checkpoint on this shard. In this case, we
             * ignore the global checkpoint update. This can happen if we are in the translog stage of recovery. Prior to this, the engine
             * is not opened and this shard will not receive global checkpoint updates, and after this the shard will be contributing to
             * calculations of the global checkpoint. However, we can not assert that we are in the translog stage of recovery here as
             * while the global checkpoint update may have emanated from the primary when we were in that state, we could subsequently move
             * to recovery finalization, or even finished recovery before the update arrives here.
             */
            assert state() != IndexShardState.POST_RECOVERY && state() != IndexShardState.STARTED :
                "supposedly in-sync shard copy received a global checkpoint [" + globalCheckpoint + "] " +
                    "that is higher than its local checkpoint [" + localCheckpoint + "]";
            return;
        }
        replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, reason);
    }

    /**
     * Updates the known allocation IDs and the local checkpoints for the corresponding allocations from a primary relocation source.
     *
     * @param primaryContext the sequence number context
     */
    public void activateWithPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        assert shardRouting.primary() && shardRouting.isRelocationTarget() :
            "only primary relocation target can update allocation IDs from primary context: " + shardRouting;
        assert primaryContext.getCheckpointStates().containsKey(routingEntry().allocationId().getId()) :
            "primary context [" + primaryContext + "] does not contain relocation target [" + routingEntry() + "]";
        assert getLocalCheckpoint() == primaryContext.getCheckpointStates().get(routingEntry().allocationId().getId())
            .getLocalCheckpoint() || indexSettings().getTranslogDurability() == Translog.Durability.ASYNC :
            "local checkpoint [" + getLocalCheckpoint() + "] does not match checkpoint from primary context [" + primaryContext + "]";
        synchronized (mutex) {
            replicationTracker.activateWithPrimaryContext(primaryContext); // make changes to primaryMode flag only under mutex
        }
    }

    /**
     * Check if there are any recoveries pending in-sync.
     *
     * @return {@code true} if there is at least one shard pending in-sync, otherwise false
     */
    public boolean pendingInSync() {
        assert assertPrimaryMode();
        return replicationTracker.pendingInSync();
    }

    void checkIndex() throws IOException {
        if (store.tryIncRef()) {
            try {
                doCheckIndex();
            } catch (IOException e) {
                store.markStoreCorrupted(e);
                throw e;
            } finally {
                store.decRef();
            }
        }
    }

    private void doCheckIndex() throws IOException {
        final long timeNS = System.nanoTime();
        if (!Lucene.indexExists(store.directory())) {
            return;
        }
        BytesStreamOutput os = new BytesStreamOutput();
        PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());

        if ("checksum".equals(checkIndexOnStartup)) {
            // physical verification only: verify all checksums for the latest commit
            IOException corrupt = null;
            MetadataSnapshot metadata = snapshotStoreMetadata();
            for (Map.Entry<String, StoreFileMetadata> entry : metadata.asMap().entrySet()) {
                try {
                    Store.checkIntegrity(entry.getValue(), store.directory());
                    out.println("checksum passed: " + entry.getKey());
                } catch (IOException exc) {
                    out.println("checksum failed: " + entry.getKey());
                    exc.printStackTrace(out);
                    corrupt = exc;
                }
            }
            out.flush();
            if (corrupt != null) {
                logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                throw corrupt;
            }
        } else {
            // full checkindex
            final CheckIndex.Status status = store.checkIndex(out);
            out.flush();
            if (!status.clean) {
                if (state == IndexShardState.CLOSED) {
                    // ignore if closed....
                    return;
                }
                logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                throw new IOException("index check failure");
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("check index [success]\n{}", os.bytes().utf8ToString());
        }

        recoveryState.getVerifyIndex().checkIndexTime(Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - timeNS)));
    }

    Engine getEngine() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            throw new AlreadyClosedException("engine is closed");
        }
        return engine;
    }

    /**
     * NOTE: returns null if engine is not yet started (e.g. recovery phase 1, copying over index files, is still running), or if engine is
     * closed.
     */
    protected Engine getEngineOrNull() {
        return this.currentEngineReference.get();
    }

    public void startRecovery(RecoveryState recoveryState,
                              PeerRecoveryTargetService recoveryTargetService,
                              PeerRecoveryTargetService.RecoveryListener recoveryListener,
                              RepositoriesService repositoriesService,
                              BiConsumer<String, MappingMetadata> mappingUpdateConsumer,
                              IndicesService indicesService) {
        // TODO: Create a proper object to encapsulate the recovery context
        // all of the current methods here follow a pattern of:
        // resolve context which isn't really dependent on the local shards and then async
        // call some external method with this pointer.
        // with a proper recovery context object we can simply change this to:
        // startRecovery(RecoveryState recoveryState, ShardRecoverySource source ) {
        //     markAsRecovery("from " + source.getShortDescription(), recoveryState);
        //     threadPool.generic().execute()  {
        //           onFailure () { listener.failure() };
        //           doRun() {
        //                if (source.recover(this)) {
        //                  recoveryListener.onRecoveryDone(recoveryState);
        //                }
        //           }
        //     }}
        // }
        assert recoveryState.getRecoverySource().equals(shardRouting.recoverySource());
        switch (recoveryState.getRecoverySource().getType()) {
            case EMPTY_STORE:
            case EXISTING_STORE:
                markAsRecovering("from store", recoveryState); // mark the shard as recovering on the cluster state thread
                threadPool.generic().execute(() -> {
                    try {
                        if (recoverFromStore()) {
                            recoveryListener.onRecoveryDone(recoveryState);
                        }
                    } catch (Exception e) {
                        recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                    }
                });
                break;
            case PEER:
                try {
                    markAsRecovering("from " + recoveryState.getSourceNode(), recoveryState);
                    recoveryTargetService.startRecovery(this, recoveryState.getSourceNode(), recoveryListener);
                } catch (Exception e) {
                    failShard("corrupted preexisting index", e);
                    recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                }
                break;
            case SNAPSHOT:
                markAsRecovering("from snapshot", recoveryState); // mark the shard as recovering on the cluster state thread
                SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) recoveryState.getRecoverySource();
                threadPool.generic().execute(() -> {
                    try {
                        final Repository repository = repositoriesService.repository(recoverySource.snapshot().getRepository());
                        if (restoreFromRepository(repository)) {
                            recoveryListener.onRecoveryDone(recoveryState);
                        }
                    } catch (Exception e) {
                        recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                    }
                });
                break;
            case LOCAL_SHARDS:
                final IndexMetadata indexMetadata = indexSettings().getIndexMetadata();
                final Index resizeSourceIndex = indexMetadata.getResizeSourceIndex();
                final List<IndexShard> startedShards = new ArrayList<>();
                final IndexService sourceIndexService = indicesService.indexService(resizeSourceIndex);
                final Set<ShardId> requiredShards;
                final int numShards;
                if (sourceIndexService != null) {
                    requiredShards = IndexMetadata.selectRecoverFromShards(shardId().id(),
                                                                           sourceIndexService.getMetadata(), indexMetadata.getNumberOfShards());
                    for (IndexShard shard : sourceIndexService) {
                        if (shard.state() == IndexShardState.STARTED && requiredShards.contains(shard.shardId())) {
                            startedShards.add(shard);
                        }
                    }
                    numShards = requiredShards.size();
                } else {
                    numShards = -1;
                    requiredShards = Collections.emptySet();
                }

                if (numShards == startedShards.size()) {
                    assert requiredShards.isEmpty() == false;
                    markAsRecovering("from local shards", recoveryState); // mark the shard as recovering on the cluster state thread
                    threadPool.generic().execute(() -> {
                        try {
                            if (recoverFromLocalShards(mappingUpdateConsumer, startedShards.stream()
                                .filter((s) -> requiredShards.contains(s.shardId())).collect(Collectors.toList()))) {
                                recoveryListener.onRecoveryDone(recoveryState);
                            }
                        } catch (Exception e) {
                            recoveryListener.onRecoveryFailure(recoveryState,
                                new RecoveryFailedException(recoveryState, null, e), true);
                        }
                    });
                } else {
                    final RuntimeException e;
                    if (numShards == -1) {
                        e = new IndexNotFoundException(resizeSourceIndex);
                    } else {
                        e = new IllegalStateException("not all required shards of index " + resizeSourceIndex
                            + " are started yet, expected " + numShards + " found " + startedShards.size() + " can't recover shard "
                            + shardId());
                    }
                    throw e;
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown recovery source " + recoveryState.getRecoverySource());
        }
    }

    /**
     * Returns whether the shard is a relocated primary, i.e. not in charge anymore of replicating changes (see {@link ReplicationTracker}).
     */
    public boolean isRelocatedPrimary() {
        assert shardRouting.primary() : "only call isRelocatedPrimary on primary shard";
        return replicationTracker.isRelocated();
    }

    class ShardEventListener implements Engine.EventListener {
        private final CopyOnWriteArrayList<Consumer<ShardFailure>> delegates = new CopyOnWriteArrayList<>();

        // called by the current engine
        @Override
        public void onFailedEngine(String reason, @Nullable Exception failure) {
            final ShardFailure shardFailure = new ShardFailure(shardRouting, reason, failure);
            for (Consumer<ShardFailure> listener : delegates) {
                try {
                    listener.accept(shardFailure);
                } catch (Exception inner) {
                    inner.addSuppressed(failure);
                    logger.warn("exception while notifying engine failure", inner);
                }
            }
        }
    }

    private Engine createNewEngine(EngineConfig config) {
        synchronized (mutex) {
            verifyNotClosed();
            assert this.currentEngineReference.get() == null;
            Engine engine = newEngine(config);
            onNewEngine(engine); // call this before we pass the memory barrier otherwise actions that happen
            // inside the callback are not visible. This one enforces happens-before
            this.currentEngineReference.set(engine);
        }

        // time elapses after the engine is created above (pulling the config settings) until we set the engine reference, during which
        // settings changes could possibly have happened, so here we forcefully push any config changes to the new engine:
        Engine engine = getEngineOrNull();

        // engine could perhaps be null if we were e.g. concurrently closed:
        if (engine != null) {
            engine.onSettingsChanged();
        }
        return engine;
    }

    protected Engine newEngine(EngineConfig config) {
        return engineFactory.newReadWriteEngine(config);
    }

    private static void persistMetadata(
            final ShardPath shardPath,
            final IndexSettings indexSettings,
            final ShardRouting newRouting,
            final @Nullable ShardRouting currentRouting,
            final Logger logger) throws IOException {
        assert newRouting != null : "newRouting must not be null";

        // only persist metadata if routing information that is persisted in shard state metadata actually changed
        final ShardId shardId = newRouting.shardId();
        if (currentRouting == null
            || currentRouting.primary() != newRouting.primary()
            || currentRouting.allocationId().equals(newRouting.allocationId()) == false) {
            assert currentRouting == null || currentRouting.isSameAllocation(newRouting);
            final String writeReason;
            if (currentRouting == null) {
                writeReason = "initial state with allocation id [" + newRouting.allocationId() + "]";
            } else {
                writeReason = "routing changed from " + currentRouting + " to " + newRouting;
            }
            logger.trace("{} writing shard state, reason [{}]", shardId, writeReason);
            final ShardStateMetadata newShardStateMetadata =
                    new ShardStateMetadata(newRouting.primary(), indexSettings.getUUID(), newRouting.allocationId());
            ShardStateMetadata.FORMAT.write(newShardStateMetadata, shardPath.getShardStatePath());
        } else {
            logger.trace("{} skip writing shard state, has been written before", shardId);
        }
    }

    private DocumentMapper defaultDocMapper() {
        return mapperService.documentMapperSafe();
    }

    private EngineConfig newEngineConfig() {
        return new EngineConfig(
            shardId,
            shardRouting.allocationId().getId(),
            threadPool,
            indexSettings,
            store,
            indexSettings.getMergePolicy(),
            mapperService == null ? null : mapperService.indexAnalyzer(),
            codecService,
            shardEventListener,
            indexCache == null ? null : indexCache.query(),
            cachingPolicy,
            translogConfig,
            IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.get(indexSettings.getSettings()),
            Collections.singletonList(refreshListeners),
            Collections.singletonList(new RefreshMetricUpdater(refreshMetric)),
            circuitBreakerService,
            replicationTracker,
            replicationTracker::getRetentionLeases,
            this::getOperationPrimaryTerm,
            tombstoneDocSupplier()
        );
    }

    /**
     * Acquire a primary operation permit whenever the shard is ready for indexing. If a permit is directly available, the provided
     * ActionListener will be called on the calling thread. During relocation hand-off, permit acquisition can be delayed. The provided
     * ActionListener will then be called using the provided executor.
     *
     * @param debugInfo an extra information that can be useful when tracing an unreleased permit. When assertions are enabled
     *                  the tracing will capture the supplied object's {@link Object#toString()} value. Otherwise the object
     *                  isn't used
     */
    public void acquirePrimaryOperationPermit(ActionListener<Releasable> onPermitAcquired, String executorOnDelay, Object debugInfo) {
        verifyNotClosed();
        assert shardRouting.primary() : "acquirePrimaryOperationPermit should only be called on primary shard: " + shardRouting;

        indexShardOperationPermits.acquire(onPermitAcquired, executorOnDelay, false, debugInfo);
    }

    /**
     * Acquire all primary operation permits. Once all permits are acquired, the provided ActionListener is called.
     * It is the responsibility of the caller to close the {@link Releasable}.
     */
    public void acquireAllPrimaryOperationsPermits(final ActionListener<Releasable> onPermitAcquired, final TimeValue timeout) {
        verifyNotClosed();
        assert shardRouting.primary() : "acquireAllPrimaryOperationsPermits should only be called on primary shard: " + shardRouting;

        indexShardOperationPermits.asyncBlockOperations(onPermitAcquired, timeout.duration(), timeout.timeUnit());
    }

    /**
     * Runs the specified runnable under a permit and otherwise calling back the specified failure callback. This method is really a
     * convenience for {@link #acquirePrimaryOperationPermit(ActionListener, String, Object)} where the listener equates to
     * try-with-resources closing the releasable after executing the runnable on successfully acquiring the permit, an otherwise calling
     * back the failure callback.
     *
     * @param runnable the runnable to execute under permit
     * @param onFailure the callback on failure
     * @param executorOnDelay the executor to execute the runnable on if permit acquisition is blocked
     * @param debugInfo debug info
     */
    public void runUnderPrimaryPermit(
            final Runnable runnable,
            final Consumer<Exception> onFailure,
            final String executorOnDelay,
            final Object debugInfo) {
        verifyNotClosed();
        assert shardRouting.primary() : "runUnderPrimaryPermit should only be called on primary shard but was " + shardRouting;
        final ActionListener<Releasable> onPermitAcquired = ActionListener.wrap(
            releasable -> {
                try (Releasable ignore = releasable) {
                    runnable.run();
                }
            },
            onFailure);
        acquirePrimaryOperationPermit(onPermitAcquired, executorOnDelay, debugInfo);
    }

    private <E extends Exception> void bumpPrimaryTerm(long newPrimaryTerm, final CheckedRunnable<E> onBlocked) {
        assert Thread.holdsLock(mutex);
        assert newPrimaryTerm > pendingPrimaryTerm;
        assert getOperationPrimaryTerm() <= pendingPrimaryTerm;
        final CountDownLatch termUpdated = new CountDownLatch(1);
        indexShardOperationPermits.asyncBlockOperations(new ActionListener<Releasable>() {
            @Override
            public void onFailure(final Exception e) {
                try {
                    failShard("exception during primary term transition", e);
                } catch (AlreadyClosedException ace) {
                    // ignore, shard is already closed
                }
            }

            @Override
            public void onResponse(final Releasable releasable) {
                try (Releasable ignored = releasable) {
                    assert getOperationPrimaryTerm() <= pendingPrimaryTerm;
                    termUpdated.await();
                    // indexShardOperationPermits doesn't guarantee that async submissions are executed
                    // in the order submitted. We need to guard against another term bump
                    if (getOperationPrimaryTerm() < newPrimaryTerm) {
                        replicationTracker.setOperationPrimaryTerm(newPrimaryTerm);
                        onBlocked.run();
                    }
                } catch (final Exception e) {
                    onFailure(e);
                }
            }
        }, 30, TimeUnit.MINUTES);
        pendingPrimaryTerm = newPrimaryTerm;
        termUpdated.countDown();
    }

    /**
     * Acquire a replica operation permit whenever the shard is ready for indexing (see
     * {@link #acquirePrimaryOperationPermit(ActionListener, String, Object)}). If the given primary term is lower than then one in
     * {@link #shardRouting}, the {@link ActionListener#onFailure(Exception)} method of the provided listener is invoked with an
     * {@link IllegalStateException}. If permit acquisition is delayed, the listener will be invoked on the executor with the specified
     * name.
     *
     * @param opPrimaryTerm              the operation primary term
     * @param globalCheckpoint           the global checkpoint associated with the request
     * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates (index operations overwrite Lucene) or deletes captured on the primary
     *                                   after this replication request was executed on it (see {@link #getMaxSeqNoOfUpdatesOrDeletes()}
     * @param onPermitAcquired           the listener for permit acquisition
     * @param executorOnDelay            the name of the executor to invoke the listener on if permit acquisition is delayed
     * @param debugInfo                  an extra information that can be useful when tracing an unreleased permit. When assertions are
     *                                   enabled the tracing will capture the supplied object's {@link Object#toString()} value.
     *                                   Otherwise the object isn't used
     */
    public void acquireReplicaOperationPermit(final long opPrimaryTerm,
                                              final long globalCheckpoint,
                                              final long maxSeqNoOfUpdatesOrDeletes,
                                              final ActionListener<Releasable> onPermitAcquired,
                                              final String executorOnDelay,
                                              final Object debugInfo) {
        innerAcquireReplicaOperationPermit(opPrimaryTerm, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, onPermitAcquired,
            (listener) -> indexShardOperationPermits.acquire(listener, executorOnDelay, true, debugInfo));
    }

    /**
     * Acquire all replica operation permits whenever the shard is ready for indexing (see
     * {@link #acquireAllPrimaryOperationsPermits(ActionListener, TimeValue)}. If the given primary term is lower than then one in
     * {@link #shardRouting}, the {@link ActionListener#onFailure(Exception)} method of the provided listener is invoked with an
     * {@link IllegalStateException}.
     *
     * @param opPrimaryTerm              the operation primary term
     * @param globalCheckpoint           the global checkpoint associated with the request
     * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates (index operations overwrite Lucene) or deletes captured on the primary
     *                                   after this replication request was executed on it (see {@link #getMaxSeqNoOfUpdatesOrDeletes()}
     * @param onPermitAcquired           the listener for permit acquisition
     * @param timeout                    the maximum time to wait for the in-flight operations block
     */
    public void acquireAllReplicaOperationsPermits(final long opPrimaryTerm,
                                                   final long globalCheckpoint,
                                                   final long maxSeqNoOfUpdatesOrDeletes,
                                                   final ActionListener<Releasable> onPermitAcquired,
                                                   final TimeValue timeout) {
        innerAcquireReplicaOperationPermit(opPrimaryTerm, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, onPermitAcquired,
            (listener) -> indexShardOperationPermits.asyncBlockOperations(listener, timeout.duration(), timeout.timeUnit()));
    }

    private void innerAcquireReplicaOperationPermit(final long opPrimaryTerm,
                                                    final long globalCheckpoint,
                                                    final long maxSeqNoOfUpdatesOrDeletes,
                                                    final ActionListener<Releasable> onPermitAcquired,
                                                    final Consumer<ActionListener<Releasable>> consumer) {
        verifyNotClosed();
        if (opPrimaryTerm > pendingPrimaryTerm) {
            synchronized (mutex) {
                if (opPrimaryTerm > pendingPrimaryTerm) {
                    final IndexShardState shardState = state();
                    // only roll translog and update primary term if shard has made it past recovery
                    // Having a new primary term here means that the old primary failed and that there is a new primary, which again
                    // means that the master will fail this shard as all initializing shards are failed when a primary is selected
                    // We abort early here to prevent an ongoing recovery from the failed primary to mess with the global / local checkpoint
                    if (shardState != IndexShardState.POST_RECOVERY &&
                        shardState != IndexShardState.STARTED) {
                        throw new IndexShardNotStartedException(shardId, shardState);
                    }

                    if (opPrimaryTerm > pendingPrimaryTerm) {
                        bumpPrimaryTerm(opPrimaryTerm, () -> {
                            updateGlobalCheckpointOnReplica(globalCheckpoint, "primary term transition");
                            final long currentGlobalCheckpoint = getLastKnownGlobalCheckpoint();
                            final long maxSeqNo = seqNoStats().getMaxSeqNo();
                            logger.info("detected new primary with primary term [{}], global checkpoint [{}], max_seq_no [{}]",
                                opPrimaryTerm, currentGlobalCheckpoint, maxSeqNo);
                            if (currentGlobalCheckpoint < maxSeqNo) {
                                resetEngineToGlobalCheckpoint();
                            } else {
                                getEngine().rollTranslogGeneration();
                            }
                        });
                    }
                }
            }
        }
        assert opPrimaryTerm <= pendingPrimaryTerm
            : "operation primary term [" + opPrimaryTerm + "] should be at most [" + pendingPrimaryTerm + "]";
        consumer.accept(new ActionListener<Releasable>() {
            @Override
            public void onResponse(final Releasable releasable) {
                if (opPrimaryTerm < getOperationPrimaryTerm()) {
                    releasable.close();
                    final String message = String.format(
                        Locale.ROOT,
                        "%s operation primary term [%d] is too old (current [%d])",
                        shardId,
                        opPrimaryTerm,
                        getOperationPrimaryTerm());
                    onPermitAcquired.onFailure(new IllegalStateException(message));
                } else {
                    assert assertReplicationTarget();
                    try {
                        updateGlobalCheckpointOnReplica(globalCheckpoint, "operation");
                        advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfUpdatesOrDeletes);
                    } catch (Exception e) {
                        releasable.close();
                        onPermitAcquired.onFailure(e);
                        return;
                    }
                    onPermitAcquired.onResponse(releasable);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                onPermitAcquired.onFailure(e);
            }
        });
    }

    public static final int OPERATIONS_BLOCKED = -1;

    /**
     * Obtain the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} if all permits are held (even if there are
     * outstanding operations in flight).
     *
     * @return the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} when all permits are held.
     */
    public int getActiveOperationsCount() {
        return indexShardOperationPermits.getActiveOperationsCount();
    }

    /**
     * @return a list of describing each permit that wasn't released yet. The description consist of the debugInfo supplied
     *         when the permit was acquired plus a stack traces that was captured when the permit was request.
     */
    public List<String> getActiveOperations() {
        return indexShardOperationPermits.getActiveOperations();
    }

    private final AsyncIOProcessor<Translog.Location> translogSyncProcessor = new AsyncIOProcessor<Translog.Location>(logger, 1024) {
        @Override
        protected void write(List<Tuple<Translog.Location, Consumer<Exception>>> candidates) throws IOException {
            try {
                getEngine().ensureTranslogSynced(candidates.stream().map(Tuple::v1));
            } catch (AlreadyClosedException ex) {
                // that's fine since we already synced everything on engine close - this also is conform with the methods
                // documentation
            } catch (IOException ex) { // if this fails we are in deep shit - fail the request
                logger.debug("failed to sync translog", ex);
                throw ex;
            }
        }
    };

    /**
     * Syncs the given location with the underlying storage unless already synced. This method might return immediately without
     * actually fsyncing the location until the sync listener is called. Yet, unless there is already another thread fsyncing
     * the transaction log the caller thread will be hijacked to run the fsync for all pending fsync operations.
     * This method allows indexing threads to continue indexing without blocking on fsync calls. We ensure that there is only
     * one thread blocking on the sync an all others can continue indexing.
     * NOTE: if the syncListener throws an exception when it's processed the exception will only be logged. Users should make sure that the
     * listener handles all exception cases internally.
     */
    public final void sync(Translog.Location location, Consumer<Exception> syncListener) {
        verifyNotClosed();
        translogSyncProcessor.put(location, syncListener);
    }

    public void sync() throws IOException {
        verifyNotClosed();
        getEngine().syncTranslog();
    }

    /**
     * Checks if the underlying storage sync is required.
     */
    public boolean isSyncNeeded() {
        return getEngine().isTranslogSyncNeeded();
    }

    /**
     * Returns the current translog durability mode
     */
    public Translog.Durability getTranslogDurability() {
        return indexSettings.getTranslogDurability();
    }

    // we can not protect with a lock since we "release" on a different thread
    private final AtomicBoolean flushOrRollRunning = new AtomicBoolean();

    /**
     * Schedules a flush or translog generation roll if needed but will not schedule more than one concurrently. The operation will be
     * executed asynchronously on the flush thread pool.
     */
    public void afterWriteOperation() {
        if (shouldPeriodicallyFlush() || shouldRollTranslogGeneration()) {
            if (flushOrRollRunning.compareAndSet(false, true)) {
                /*
                 * We have to check again since otherwise there is a race when a thread passes the first check next to another thread which
                 * performs the operation quickly enough to  finish before the current thread could flip the flag. In that situation, we
                 * have an extra operation.
                 *
                 * Additionally, a flush implicitly executes a translog generation roll so if we execute a flush then we do not need to
                 * check if we should roll the translog generation.
                 */
                if (shouldPeriodicallyFlush()) {
                    logger.debug("submitting async flush request");
                    final AbstractRunnable flush = new AbstractRunnable() {
                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to flush index", e);
                            }
                        }

                        @Override
                        protected void doRun() throws IOException {
                            flush(new FlushRequest());
                            periodicFlushMetric.inc();
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false);
                            afterWriteOperation();
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(flush);
                } else if (shouldRollTranslogGeneration()) {
                    logger.debug("submitting async roll translog generation request");
                    final AbstractRunnable roll = new AbstractRunnable() {
                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to roll translog generation", e);
                            }
                        }

                        @Override
                        protected void doRun() throws Exception {
                            rollTranslogGeneration();
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false);
                            afterWriteOperation();
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(roll);
                } else {
                    flushOrRollRunning.compareAndSet(true, false);
                }
            }
        }
    }

    /**
     * Build {@linkplain RefreshListeners} for this shard.
     */
    private RefreshListeners buildRefreshListeners() {
        return new RefreshListeners(
            indexSettings::getMaxRefreshListeners,
            () -> refresh("too_many_listeners"),
            threadPool.executor(ThreadPool.Names.LISTENER)::execute,
            logger
        );
    }

    /**
     * Simple struct encapsulating a shard failure
     *
     * @see IndexShard#addShardFailureCallback(Consumer)
     */
    public static final class ShardFailure {
        public final ShardRouting routing;
        public final String reason;
        @Nullable
        public final Exception cause;

        public ShardFailure(ShardRouting routing, String reason, @Nullable Exception cause) {
            this.routing = routing;
            this.reason = reason;
            this.cause = cause;
        }
    }

    EngineFactory getEngineFactory() {
        return engineFactory;
    }

    // for tests
    ReplicationTracker getReplicationTracker() {
        return replicationTracker;
    }

    /**
     * Returns <code>true</code> iff one or more changes to the engine are not visible to via the current searcher *or* there are pending
     * refresh listeners.
     * Otherwise <code>false</code>.
     *
     * @throws AlreadyClosedException if the engine or internal indexwriter in the engine is already closed
     */
    public boolean isRefreshNeeded() {
        return getEngine().refreshNeeded() || (refreshListeners != null && refreshListeners.refreshNeeded());
    }

    /**
     * Add a listener for refreshes.
     *
     * @param location the location to listen for
     * @param listener for the refresh. Called with true if registering the listener ran it out of slots and forced a refresh. Called with
     *        false otherwise.
     */
    public void addRefreshListener(Translog.Location location, Consumer<Boolean> listener) {
        final boolean readAllowed;
        if (isReadAllowed()) {
            readAllowed = true;
        } else {
            // check again under mutex. this is important to create a happens before relationship
            // between the switch to POST_RECOVERY + associated refresh. Otherwise we may respond
            // to a listener before a refresh actually happened that contained that operation.
            synchronized (mutex) {
                readAllowed = isReadAllowed();
            }
        }
        if (readAllowed) {
            refreshListeners.addOrNotify(location, listener);
        } else {
            // we're not yet ready fo ready for reads, just ignore refresh cycles
            listener.accept(false);
        }
    }

    private static class RefreshMetricUpdater implements ReferenceManager.RefreshListener {

        private final MeanMetric refreshMetric;
        private long currentRefreshStartTime;
        private Thread callingThread = null;

        private RefreshMetricUpdater(MeanMetric refreshMetric) {
            this.refreshMetric = refreshMetric;
        }

        @Override
        public void beforeRefresh() throws IOException {
            if (Assertions.ENABLED) {
                assert callingThread == null : "beforeRefresh was called by " + callingThread.getName() +
                    " without a corresponding call to afterRefresh";
                callingThread = Thread.currentThread();
            }
            currentRefreshStartTime = System.nanoTime();
        }

        @Override
        public void afterRefresh(boolean didRefresh) throws IOException {
            if (Assertions.ENABLED) {
                assert callingThread != null : "afterRefresh called but not beforeRefresh";
                assert callingThread == Thread.currentThread() : "beforeRefreshed called by a different thread. current ["
                    + Thread.currentThread().getName() + "], thread that called beforeRefresh [" + callingThread.getName() + "]";
                callingThread = null;
            }
            refreshMetric.inc(System.nanoTime() - currentRefreshStartTime);
        }
    }

    private EngineConfig.TombstoneDocSupplier tombstoneDocSupplier() {
        final RootObjectMapper.Builder noopRootMapper = new RootObjectMapper.Builder("__noop");
        final DocumentMapper noopDocumentMapper = mapperService == null
            ? null
            : new DocumentMapper.Builder(noopRootMapper, mapperService).build(mapperService);
        return new EngineConfig.TombstoneDocSupplier() {

            @Override
            public ParsedDocument newDeleteTombstoneDoc(String id) {
                return defaultDocMapper().createDeleteTombstoneDoc(shardId.getIndexName(), id);
            }

            @Override
            public ParsedDocument newNoopTombstoneDoc(String reason) {
                return noopDocumentMapper.createNoopTombstoneDoc(shardId.getIndexName(), reason);
            }
        };
    }

    /**
     * Rollback the current engine to the safe commit, then replay local translog up to the global checkpoint.
     */
    void resetEngineToGlobalCheckpoint() throws IOException {
        assert getActiveOperationsCount() == OPERATIONS_BLOCKED
            : "resetting engine without blocking operations; active operations are [" + getActiveOperations() + ']';
        sync(); // persist the global checkpoint to disk
        final long globalCheckpoint = getLastKnownGlobalCheckpoint();
        assert globalCheckpoint == getLastSyncedGlobalCheckpoint();
        final Engine newEngine;
        synchronized (mutex) {
            verifyNotClosed();
            IOUtils.close(currentEngineReference.getAndSet(null));
            newEngine = createNewEngine(newEngineConfig());
            active.set(true);
        }
        final Engine.TranslogRecoveryRunner translogRunner = (engine, snapshot) -> runTranslogRecovery(
            engine, snapshot, Engine.Operation.Origin.LOCAL_RESET, () -> {
                // TODO: add a dedicate recovery stats for the reset translog
            });
        newEngine.recoverFromTranslog(translogRunner, globalCheckpoint);
    }

    /**
     * Returns the maximum sequence number of either update or delete operations have been processed in this shard
     * or the sequence number from {@link #advanceMaxSeqNoOfUpdatesOrDeletes(long)}. An index request is considered
     * as an update operation if it overwrites the existing documents in Lucene index with the same document id.
     * <p>
     * The primary captures this value after executes a replication request, then transfers it to a replica before
     * executing that replication request on a replica.
     */
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return getEngine().getMaxSeqNoOfUpdatesOrDeletes();
    }

    /**
     * A replica calls this method to advance the max_seq_no_of_updates marker of its engine to at least the max_seq_no_of_updates
     * value (piggybacked in a replication request) that it receives from its primary before executing that replication request.
     * The receiving value is at least as high as the max_seq_no_of_updates on the primary was when any of the operations of that
     * replication request were processed on it.
     * <p>
     * A replica shard also calls this method to bootstrap the max_seq_no_of_updates marker with the value that it received from
     * the primary in peer-recovery, before it replays remote translog operations from the primary. The receiving value is at least
     * as high as the max_seq_no_of_updates on the primary was when any of these operations were processed on it.
     * <p>
     * These transfers guarantee that every index/delete operation when executing on a replica engine will observe this marker a value
     * which is at least the value of the max_seq_no_of_updates marker on the primary after that operation was executed on the primary.
     *
     * @see #acquireReplicaOperationPermit(long, long, long, ActionListener, String, Object)
     * @see RecoveryTarget#indexTranslogOperations(List, int, long, long, RetentionLeases, ActionListener)
     */
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long seqNo) {
        getEngine().advanceMaxSeqNoOfUpdatesOrDeletes(seqNo);
    }
}
