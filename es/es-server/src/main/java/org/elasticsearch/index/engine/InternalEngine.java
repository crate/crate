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

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.Assertions;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.LoggerInfoStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ElasticsearchMergePolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class InternalEngine extends Engine {

    /**
     * When we last pruned expired tombstones from versionMap.deletes:
     */
    private volatile long lastDeleteVersionPruneTimeMSec;

    private final Translog translog;
    private final ElasticsearchConcurrentMergeScheduler mergeScheduler;

    private final IndexWriter indexWriter;

    private final ExternalSearcherManager externalSearcherManager;
    private final SearcherManager internalSearcherManager;

    private final Lock flushLock = new ReentrantLock();
    private final ReentrantLock optimizeLock = new ReentrantLock();

    // A uid (in the form of BytesRef) to the version map
    // we use the hashed variant since we iterate over it and check removal and additions on existing keys
    private final LiveVersionMap versionMap = new LiveVersionMap();

    private volatile SegmentInfos lastCommittedSegmentInfos;

    private final IndexThrottle throttle;

    private final LocalCheckpointTracker localCheckpointTracker;

    private final String uidField;

    private final CombinedDeletionPolicy combinedDeletionPolicy;

    // How many callers are currently requesting index throttling.  Currently there are only two situations where we do this: when merges
    // are falling behind and when writing indexing buffer to disk is too slow.  When this is 0, there is no throttling, else we throttling
    // incoming indexing ops to a single thread:
    private final AtomicInteger throttleRequestCount = new AtomicInteger();
    private final AtomicBoolean pendingTranslogRecovery = new AtomicBoolean(false);
    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);
    private final AtomicLong maxSeenAutoIdTimestamp = new AtomicLong(-1);
    private final AtomicLong maxSeqNoOfNonAppendOnlyOperations = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
    private final CounterMetric numVersionLookups = new CounterMetric();
    private final CounterMetric numIndexVersionsLookups = new CounterMetric();
    // Lucene operations since this engine was opened - not include operations from existing segments.
    private final CounterMetric numDocDeletes = new CounterMetric();
    private final CounterMetric numDocAppends = new CounterMetric();
    private final CounterMetric numDocUpdates = new CounterMetric();
    private final NumericDocValuesField softDeletesField = Lucene.newSoftDeletesField();
    private final boolean softDeleteEnabled;
    private final SoftDeletesPolicy softDeletesPolicy;
    private final LastRefreshedCheckpointListener lastRefreshedCheckpointListener;

    private final AtomicBoolean trackTranslogLocation = new AtomicBoolean(false);
    private final KeyedLock<Long> noOpKeyedLock = new KeyedLock<>();

    @Nullable
    private final String historyUUID;

    public InternalEngine(EngineConfig engineConfig) {
        this(engineConfig, LocalCheckpointTracker::new);
    }

    InternalEngine(
            final EngineConfig engineConfig,
            final BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) {
        super(engineConfig);
        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            updateAutoIdTimestamp(Long.MAX_VALUE, true);
        }
        this.uidField = engineConfig.getIndexSettings().isSingleType() ? IdFieldMapper.NAME : UidFieldMapper.NAME;
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(
                engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
                engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis()
        );
        store.incRef();
        IndexWriter writer = null;
        Translog translog = null;
        ExternalSearcherManager externalSearcherManager = null;
        SearcherManager internalSearcherManager = null;
        EngineMergeScheduler scheduler = null;
        boolean success = false;
        try {
            this.lastDeleteVersionPruneTimeMSec = engineConfig.getThreadPool().relativeTimeInMillis();

            mergeScheduler = scheduler = new EngineMergeScheduler(engineConfig.getShardId(), engineConfig.getIndexSettings());
            throttle = new IndexThrottle();
            try {
                translog = openTranslog(engineConfig, translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier());
                assert translog.getGeneration() != null;
                this.translog = translog;
                this.softDeleteEnabled = engineConfig.getIndexSettings().isSoftDeleteEnabled();
                this.softDeletesPolicy = newSoftDeletesPolicy();
                this.combinedDeletionPolicy =
                    new CombinedDeletionPolicy(logger, translogDeletionPolicy, softDeletesPolicy, translog::getLastSyncedGlobalCheckpoint);
                writer = createWriter();
                bootstrapAppendOnlyInfoFromWriter(writer);
                historyUUID = loadHistoryUUID(writer);
                indexWriter = writer;
            } catch (IOException | TranslogCorruptedException e) {
                throw new EngineCreationFailureException(shardId, "failed to create engine", e);
            } catch (AssertionError e) {
                // IndexWriter throws AssertionError on init, if asserts are enabled, if any files don't exist, but tests that
                // randomly throw FNFE/NSFE can also hit this:
                if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                    throw new EngineCreationFailureException(shardId, "failed to create engine", e);
                } else {
                    throw e;
                }
            }
            externalSearcherManager = createSearcherManager(new SearchFactory(logger, isClosed, engineConfig));
            internalSearcherManager = externalSearcherManager.internalSearcherManager;
            this.internalSearcherManager = internalSearcherManager;
            this.externalSearcherManager = externalSearcherManager;
            internalSearcherManager.addListener(versionMap);
            assert pendingTranslogRecovery.get() == false : "translog recovery can't be pending before we set it";
            // don't allow commits until we are done with recovering
            pendingTranslogRecovery.set(true);
            for (ReferenceManager.RefreshListener listener: engineConfig.getExternalRefreshListener()) {
                this.externalSearcherManager.addListener(listener);
            }
            for (ReferenceManager.RefreshListener listener: engineConfig.getInternalRefreshListener()) {
                this.internalSearcherManager.addListener(listener);
            }
            this.localCheckpointTracker = createLocalCheckpointTracker(engineConfig, lastCommittedSegmentInfos, logger,
                () -> acquireSearcher("create_local_checkpoint_tracker", SearcherScope.INTERNAL), localCheckpointTrackerSupplier);
            this.lastRefreshedCheckpointListener = new LastRefreshedCheckpointListener(localCheckpointTracker.getCheckpoint());
            this.internalSearcherManager.addListener(lastRefreshedCheckpointListener);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(writer, translog, internalSearcherManager, externalSearcherManager, scheduler);
                if (isClosed.get() == false) {
                    // failure we need to dec the store reference
                    store.decRef();
                }
            }
        }
        logger.trace("created new InternalEngine");
    }

    private static LocalCheckpointTracker createLocalCheckpointTracker(EngineConfig engineConfig, SegmentInfos lastCommittedSegmentInfos,
        Logger logger, Supplier<Searcher> searcherSupplier, BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) {
        try {
            final SequenceNumbers.CommitInfo seqNoStats =
                SequenceNumbers.loadSeqNoInfoFromLuceneCommit(lastCommittedSegmentInfos.userData.entrySet());
            final long maxSeqNo = seqNoStats.maxSeqNo;
            final long localCheckpoint = seqNoStats.localCheckpoint;
            logger.trace("recovered maximum sequence number [{}] and local checkpoint [{}]", maxSeqNo, localCheckpoint);
            final LocalCheckpointTracker tracker = localCheckpointTrackerSupplier.apply(maxSeqNo, localCheckpoint);
            // Operations that are optimized using max_seq_no_of_updates optimization must not be processed twice; otherwise, they will
            // create duplicates in Lucene. To avoid this we check the LocalCheckpointTracker to see if an operation was already processed.
            // Thus, we need to restore the LocalCheckpointTracker bit by bit to ensure the consistency between LocalCheckpointTracker and
            // Lucene index. This is not the only solution since we can bootstrap max_seq_no_of_updates with max_seq_no of the commit to
            // disable the MSU optimization during recovery. Here we prefer to maintain the consistency of LocalCheckpointTracker.
            if (localCheckpoint < maxSeqNo && engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
                try (Searcher searcher = searcherSupplier.get()) {
                    Lucene.scanSeqNosInReader(searcher.getDirectoryReader(), localCheckpoint + 1, maxSeqNo, tracker::markSeqNoAsCompleted);
                }
            }
            return tracker;
        } catch (IOException ex) {
            throw new EngineCreationFailureException(engineConfig.getShardId(), "failed to create local checkpoint tracker", ex);
        }
    }

    private SoftDeletesPolicy newSoftDeletesPolicy() throws IOException {
        final Map<String, String> commitUserData = store.readLastCommittedSegmentsInfo().userData;
        final long lastMinRetainedSeqNo;
        if (commitUserData.containsKey(Engine.MIN_RETAINED_SEQNO)) {
            lastMinRetainedSeqNo = Long.parseLong(commitUserData.get(Engine.MIN_RETAINED_SEQNO));
        } else {
            lastMinRetainedSeqNo = Long.parseLong(commitUserData.get(SequenceNumbers.MAX_SEQ_NO)) + 1;
        }
        return new SoftDeletesPolicy(translog::getLastSyncedGlobalCheckpoint, lastMinRetainedSeqNo,
            engineConfig.getIndexSettings().getSoftDeleteRetentionOperations());
    }

    /**
     * This reference manager delegates all it's refresh calls to another (internal) SearcherManager
     * The main purpose for this is that if we have external refreshes happening we don't issue extra
     * refreshes to clear version map memory etc. this can cause excessive segment creation if heavy indexing
     * is happening and the refresh interval is low (ie. 1 sec)
     *
     * This also prevents segment starvation where an internal reader holds on to old segments literally forever
     * since no indexing is happening and refreshes are only happening to the external reader manager, while with
     * this specialized implementation an external refresh will immediately be reflected on the internal reader
     * and old segments can be released in the same way previous version did this (as a side-effect of _refresh)
     */
    @SuppressForbidden(reason = "reference counting is required here")
    private static final class ExternalSearcherManager extends ReferenceManager<IndexSearcher> {
        private final SearcherFactory searcherFactory;
        private final SearcherManager internalSearcherManager;

        ExternalSearcherManager(SearcherManager internalSearcherManager, SearcherFactory searcherFactory) throws IOException {
            IndexSearcher acquire = internalSearcherManager.acquire();
            try {
                IndexReader indexReader = acquire.getIndexReader();
                assert indexReader instanceof ElasticsearchDirectoryReader:
                    "searcher's IndexReader should be an ElasticsearchDirectoryReader, but got " + indexReader;
                indexReader.incRef(); // steal the reader - getSearcher will decrement if it fails
                current = SearcherManager.getSearcher(searcherFactory, indexReader, null);
            } finally {
                internalSearcherManager.release(acquire);
            }
            this.searcherFactory = searcherFactory;
            this.internalSearcherManager = internalSearcherManager;
        }

        @Override
        protected IndexSearcher refreshIfNeeded(IndexSearcher referenceToRefresh) throws IOException {
            // we simply run a blocking refresh on the internal reference manager and then steal it's reader
            // it's a save operation since we acquire the reader which incs it's reference but then down the road
            // steal it by calling incRef on the "stolen" reader
            internalSearcherManager.maybeRefreshBlocking();
            IndexSearcher acquire = internalSearcherManager.acquire();
            try {
                final IndexReader previousReader = referenceToRefresh.getIndexReader();
                assert previousReader instanceof ElasticsearchDirectoryReader:
                    "searcher's IndexReader should be an ElasticsearchDirectoryReader, but got " + previousReader;

                final IndexReader newReader = acquire.getIndexReader();
                if (newReader == previousReader) {
                    // nothing has changed - both ref managers share the same instance so we can use reference equality
                    return null;
                } else {
                    newReader.incRef(); // steal the reader - getSearcher will decrement if it fails
                    return SearcherManager.getSearcher(searcherFactory, newReader, previousReader);
                }
            } finally {
                internalSearcherManager.release(acquire);
            }
        }

        @Override
        protected boolean tryIncRef(IndexSearcher reference) {
            return reference.getIndexReader().tryIncRef();
        }

        @Override
        protected int getRefCount(IndexSearcher reference) {
            return reference.getIndexReader().getRefCount();
        }

        @Override
        protected void decRef(IndexSearcher reference) throws IOException { reference.getIndexReader().decRef(); }
    }

    @Override
    public int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = localCheckpointTracker.getCheckpoint();
            try (Translog.Snapshot snapshot = getTranslog().newSnapshotFromMinSeqNo(localCheckpoint + 1)) {
                return translogRecoveryRunner.run(this, snapshot);
            }
        }
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = localCheckpointTracker.getCheckpoint();
            final long maxSeqNo = localCheckpointTracker.getMaxSeqNo();
            int numNoOpsAdded = 0;
            for (
                    long seqNo = localCheckpoint + 1;
                    seqNo <= maxSeqNo;
                    seqNo = localCheckpointTracker.getCheckpoint() + 1 /* the local checkpoint might have advanced so we leap-frog */) {
                innerNoOp(new NoOp(seqNo, primaryTerm, Operation.Origin.PRIMARY, System.nanoTime(), "filling gaps"));
                numNoOpsAdded++;
                assert seqNo <= localCheckpointTracker.getCheckpoint()
                        : "local checkpoint did not advance; was [" + seqNo + "], now [" + localCheckpointTracker.getCheckpoint() + "]";

            }
            return numNoOpsAdded;
        }
    }

    private void bootstrapAppendOnlyInfoFromWriter(IndexWriter writer) {
        for (Map.Entry<String, String> entry : writer.getLiveCommitData()) {
            final String key = entry.getKey();
            if (key.equals(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID)) {
                assert maxUnsafeAutoIdTimestamp.get() == -1 :
                    "max unsafe timestamp was assigned already [" + maxUnsafeAutoIdTimestamp.get() + "]";
                updateAutoIdTimestamp(Long.parseLong(entry.getValue()), true);
            }
            if (key.equals(SequenceNumbers.MAX_SEQ_NO)) {
                assert maxSeqNoOfNonAppendOnlyOperations.get() == -1 :
                    "max unsafe append-only seq# was assigned already [" + maxSeqNoOfNonAppendOnlyOperations.get() + "]";
                maxSeqNoOfNonAppendOnlyOperations.set(Long.parseLong(entry.getValue()));
            }
        }
    }

    @Override
    public InternalEngine recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo) throws IOException {
        flushLock.lock();
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            assert getMaxSeqNoOfUpdatesOrDeletes() != SequenceNumbers.UNASSIGNED_SEQ_NO ||
                engineConfig.getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0) : "max_seq_no_of_updates is uninitialized";
            if (pendingTranslogRecovery.get() == false) {
                throw new IllegalStateException("Engine has already been recovered");
            }
            try {
                recoverFromTranslogInternal(translogRecoveryRunner, recoverUpToSeqNo);
            } catch (Exception e) {
                try {
                    pendingTranslogRecovery.set(true); // just play safe and never allow commits on this see #ensureCanFlush
                    failEngine("failed to recover from translog", e);
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                }
                throw e;
            }
        } finally {
            flushLock.unlock();
        }
        return this;
    }

    @Override
    public void skipTranslogRecovery() {
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        pendingTranslogRecovery.set(false); // we are good - now we can commit
    }

    private void recoverFromTranslogInternal(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo) throws IOException {
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        final int opsRecovered;
        final long translogFileGen = Long.parseLong(lastCommittedSegmentInfos.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
        try (Translog.Snapshot snapshot = translog.newSnapshotFromGen(
            new Translog.TranslogGeneration(translog.getTranslogUUID(), translogFileGen), recoverUpToSeqNo)) {
            opsRecovered = translogRecoveryRunner.run(this, snapshot);
        } catch (Exception e) {
            throw new EngineException(shardId, "failed to recover from translog", e);
        }
        // flush if we recovered something or if we have references to older translogs
        // note: if opsRecovered == 0 and we have older translogs it means they are corrupted or 0 length.
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        pendingTranslogRecovery.set(false); // we are good - now we can commit
        if (opsRecovered > 0) {
            logger.trace("flushing post recovery from translog. ops recovered [{}]. committed translog id [{}]. current id [{}]",
                opsRecovered, translogGeneration == null ? null : translogGeneration.translogFileGeneration, translog.currentFileGeneration());
            commitIndexWriter(indexWriter, translog, null);
            refreshLastCommittedSegmentInfos();
            refresh("translog_recovery");
        }
        translog.trimUnreferencedReaders();
    }

    private Translog openTranslog(EngineConfig engineConfig, TranslogDeletionPolicy translogDeletionPolicy, LongSupplier globalCheckpointSupplier) throws IOException {
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        final String translogUUID = loadTranslogUUIDFromLastCommit();
        // A translog checkpoint from 5.x index does not have translog_generation_key and Translog's ctor will read translog gen values
        // from translogDeletionPolicy. We need to bootstrap these values from the recovering commit before calling Translog ctor.
        if (engineConfig.getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0)) {
            final SegmentInfos lastCommitInfo = store.readLastCommittedSegmentsInfo();
            final long minRequiredTranslogGen = Long.parseLong(lastCommitInfo.userData.get(Translog.TRANSLOG_GENERATION_KEY));
            translogDeletionPolicy.setTranslogGenerationOfLastCommit(minRequiredTranslogGen);
            translogDeletionPolicy.setMinTranslogGenerationForRecovery(minRequiredTranslogGen);
        }
        // We expect that this shard already exists, so it must already have an existing translog else something is badly wrong!
        return new Translog(translogConfig, translogUUID, translogDeletionPolicy, globalCheckpointSupplier, engineConfig.getPrimaryTermSupplier());
    }

    // Package private for testing purposes only
    Translog getTranslog() {
        ensureOpen();
        return translog;
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return getTranslog().syncNeeded();
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        final boolean synced = translog.ensureSynced(locations);
        if (synced) {
            revisitIndexDeletionPolicyOnTranslogSynced();
        }
        return synced;
    }

    @Override
    public void syncTranslog() throws IOException {
        translog.sync();
        revisitIndexDeletionPolicyOnTranslogSynced();
    }

    /**
     * Creates a new history snapshot for reading operations since the provided seqno.
     * The returned snapshot can be retrieved from either Lucene index or translog files.
     */
    @Override
    public Translog.Snapshot readHistoryOperations(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            return newChangesSnapshot(source, mapperService, Math.max(0, startingSeqNo), Long.MAX_VALUE, false);
        } else {
            return getTranslog().newSnapshotFromMinSeqNo(startingSeqNo);
        }
    }

    /**
     * Returns the estimated number of history operations whose seq# at least the provided seq# in this engine.
     */
    @Override
    public int estimateNumberOfHistoryOperations(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            try (Translog.Snapshot snapshot = newChangesSnapshot(source, mapperService, Math.max(0, startingSeqNo), Long.MAX_VALUE, false)) {
                return snapshot.totalOperations();
            }
        } else {
            return getTranslog().estimateTotalOperationsFromMinSeq(startingSeqNo);
        }
    }

    @Override
    public TranslogStats getTranslogStats() {
        return getTranslog().stats();
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return getTranslog().getLastWriteLocation();
    }

    private void revisitIndexDeletionPolicyOnTranslogSynced() throws IOException {
        if (combinedDeletionPolicy.hasUnreferencedCommits()) {
            indexWriter.deleteUnusedFiles();
            translog.trimUnreferencedReaders();
        }
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    /** Returns how many bytes we are currently moving from indexing buffer to segments on disk */
    @Override
    public long getWritingBytes() {
        return indexWriter.getFlushingBytes() + versionMap.getRefreshingBytes();
    }

    /**
     * Reads the current stored translog ID from the last commit data.
     */
    @Nullable
    private String loadTranslogUUIDFromLastCommit() throws IOException {
        final Map<String, String> commitUserData = store.readLastCommittedSegmentsInfo().getUserData();
        if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY) == false) {
            throw new IllegalStateException("commit doesn't contain translog generation id");
        }
        return commitUserData.get(Translog.TRANSLOG_UUID_KEY);
    }

    /**
     * Reads the current stored history ID from the IW commit data.
     */
    private String loadHistoryUUID(final IndexWriter writer) throws IOException {
        final String uuid = commitDataAsMap(writer).get(HISTORY_UUID_KEY);
        if (uuid == null) {
            throw new IllegalStateException("commit doesn't contain history uuid");
        }
        return uuid;
    }

    private ExternalSearcherManager createSearcherManager(SearchFactory externalSearcherFactory) throws EngineException {
        boolean success = false;
        SearcherManager internalSearcherManager = null;
        try {
            try {
                final DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);
                internalSearcherManager = new SearcherManager(directoryReader,
                        new RamAccountingSearcherFactory(engineConfig.getCircuitBreakerService()));
                lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                ExternalSearcherManager externalSearcherManager = new ExternalSearcherManager(internalSearcherManager,
                    externalSearcherFactory);
                success = true;
                return externalSearcherManager;
            } catch (IOException e) {
                maybeFailEngine("start", e);
                try {
                    indexWriter.rollback();
                } catch (IOException inner) { // iw is closed below
                    e.addSuppressed(inner);
                }
                throw new EngineCreationFailureException(shardId, "failed to open reader on writer", e);
            }
        } finally {
            if (success == false) { // release everything we created on a failure
                IOUtils.closeWhileHandlingException(internalSearcherManager, indexWriter);
            }
        }
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        assert Objects.equals(get.uid().field(), uidField) : get.uid().field();
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            SearcherScope scope;
            if (get.realtime()) {
                VersionValue versionValue = null;
                try (Releasable ignore = versionMap.acquireLock(get.uid().bytes())) {
                    // we need to lock here to access the version map to do this truly in RT
                    versionValue = getVersionFromMap(get.uid().bytes());
                }
                if (versionValue != null) {
                    if (versionValue.isDelete()) {
                        return GetResult.NOT_EXISTS;
                    }
                    if (get.versionType().isVersionConflictForReads(versionValue.version, get.version())) {
                        throw new VersionConflictEngineException(shardId, get.type(), get.id(),
                            get.versionType().explainConflictForReads(versionValue.version, get.version()));
                    }
                    if (get.isReadFromTranslog()) {
                        // this is only used for updates - API _GET calls will always read form a reader for consistency
                        // the update call doesn't need the consistency since it's source only + _parent but parent can go away in 7.0
                        if (versionValue.getLocation() != null) {
                            try {
                                Translog.Operation operation = translog.readOperation(versionValue.getLocation());
                                if (operation != null) {
                                    // in the case of a already pruned translog generation we might get null here - yet very unlikely
                                    TranslogLeafReader reader = new TranslogLeafReader((Translog.Index) operation, engineConfig
                                        .getIndexSettings().getIndexVersionCreated());
                                    return new GetResult(new Searcher("realtime_get", new IndexSearcher(reader), reader::close),
                                        new VersionsAndSeqNoResolver.DocIdAndVersion(0, ((Translog.Index) operation).version(), reader, 0));
                                }
                            } catch (IOException e) {
                                maybeFailEngine("realtime_get", e); // lets check if the translog has failed with a tragic event
                                throw new EngineException(shardId, "failed to read operation from translog", e);
                            }
                        } else {
                            trackTranslogLocation.set(true);
                        }
                    }
                    refresh("realtime_get", SearcherScope.INTERNAL);
                }
                scope = SearcherScope.INTERNAL;
            } else {
                // we expose what has been externally expose in a point in time snapshot via an explicit refresh
                scope = SearcherScope.EXTERNAL;
            }

            // no version, get the version from the index, we know that we refresh on flush
            return getFromSearcher(get, searcherFactory, scope);
        }
    }

    /**
     * the status of the current doc version in lucene, compared to the version in an incoming
     * operation
     */
    enum OpVsLuceneDocStatus {
        /** the op is more recent than the one that last modified the doc found in lucene*/
        OP_NEWER,
        /** the op is older or the same as the one that last modified the doc found in lucene*/
        OP_STALE_OR_EQUAL,
        /** no doc was found in lucene */
        LUCENE_DOC_NOT_FOUND
    }

    private OpVsLuceneDocStatus compareOpToLuceneDocBasedOnSeqNo(final Operation op) throws IOException {
        assert op.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "resolving ops based on seq# but no seqNo is found";
        final OpVsLuceneDocStatus status;
        VersionValue versionValue = getVersionFromMap(op.uid().bytes());
        assert incrementVersionLookup();
        if (versionValue != null) {
            if (op.seqNo() > versionValue.seqNo) {
                status = OpVsLuceneDocStatus.OP_NEWER;
            } else if (op.seqNo() == versionValue.seqNo) {
                assert versionValue.term == op.primaryTerm() : "primary term not matched; id=" + op.id() + " seq_no=" + op.seqNo()
                    + " op_term=" + op.primaryTerm() + " existing_term=" + versionValue.term;
                status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
            } else {
                status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
            }
        } else {
            // load from index
            assert incrementIndexVersionLookup();
            try (Searcher searcher = acquireSearcher("load_seq_no", SearcherScope.INTERNAL)) {
                final DocIdAndSeqNo docAndSeqNo = VersionsAndSeqNoResolver.loadDocIdAndSeqNo(searcher.reader(), op.uid());
                if (docAndSeqNo == null) {
                    status = OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND;
                } else if (op.seqNo() > docAndSeqNo.seqNo) {
                    if (docAndSeqNo.isLive) {
                        status = OpVsLuceneDocStatus.OP_NEWER;
                    } else {
                        status = OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND;
                    }
                } else if (op.seqNo() == docAndSeqNo.seqNo) {
                    assert localCheckpointTracker.contains(op.seqNo()) || softDeleteEnabled == false :
                        "local checkpoint tracker is not updated seq_no=" + op.seqNo() + " id=" + op.id();
                    status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
                } else {
                    status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
                }
            }
        }
        return status;
    }

    /** resolves the current version of the document, returning null if not found */
    private VersionValue resolveDocVersion(final Operation op) throws IOException {
        assert incrementVersionLookup(); // used for asserting in tests
        VersionValue versionValue = getVersionFromMap(op.uid().bytes());
        if (versionValue == null) {
            assert incrementIndexVersionLookup(); // used for asserting in tests
            final long currentVersion = loadCurrentVersionFromIndex(op.uid());
            if (currentVersion != Versions.NOT_FOUND) {
                versionValue = new IndexVersionValue(null, currentVersion, SequenceNumbers.UNASSIGNED_SEQ_NO, 0L);
            }
        } else if (engineConfig.isEnableGcDeletes() && versionValue.isDelete() &&
            (engineConfig.getThreadPool().relativeTimeInMillis() - ((DeleteVersionValue)versionValue).time) > getGcDeletesInMillis()) {
            versionValue = null;
        }
        return versionValue;
    }

    private OpVsLuceneDocStatus compareOpToLuceneDocBasedOnVersions(final Operation op)
        throws IOException {
        assert op.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO : "op is resolved based on versions but have a seq#";
        assert op.version() >= 0 : "versions should be non-negative. got " + op.version();
        final VersionValue versionValue = resolveDocVersion(op);
        if (versionValue == null) {
            return OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND;
        } else {
            return op.versionType().isVersionConflictForWrites(versionValue.version, op.version(), versionValue.isDelete()) ?
                OpVsLuceneDocStatus.OP_STALE_OR_EQUAL : OpVsLuceneDocStatus.OP_NEWER;
        }
    }

    private VersionValue getVersionFromMap(BytesRef id) {
        if (versionMap.isUnsafe()) {
            synchronized (versionMap) {
                // we are switching from an unsafe map to a safe map. This might happen concurrently
                // but we only need to do this once since the last operation per ID is to add to the version
                // map so once we pass this point we can safely lookup from the version map.
                if (versionMap.isUnsafe()) {
                    refresh("unsafe_version_map", SearcherScope.INTERNAL);
                }
                versionMap.enforceSafeAccess();
            }
        }
        return versionMap.getUnderLock(id);
    }

    private boolean canOptimizeAddDocument(Index index) {
        if (index.getAutoGeneratedIdTimestamp() != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
            assert index.getAutoGeneratedIdTimestamp() >= 0 : "autoGeneratedIdTimestamp must be positive but was: "
                + index.getAutoGeneratedIdTimestamp();
            switch (index.origin()) {
                case PRIMARY:
                    assertPrimaryCanOptimizeAddDocument(index);
                    return true;
                case PEER_RECOVERY:
                case REPLICA:
                    assert index.version() == 1 && index.versionType() == VersionType.EXTERNAL
                        : "version: " + index.version() + " type: " + index.versionType();
                    return true;
                case LOCAL_TRANSLOG_RECOVERY:
                case LOCAL_RESET:
                    assert index.isRetry();
                    return true; // allow to optimize in order to update the max safe time stamp
                default:
                    throw new IllegalArgumentException("unknown origin " + index.origin());
            }
        }
        return false;
    }

    protected boolean assertPrimaryCanOptimizeAddDocument(final Index index) {
        assert (index.version() == Versions.MATCH_ANY && index.versionType() == VersionType.INTERNAL)
                : "version: " + index.version() + " type: " + index.versionType();
        return true;
    }

    private boolean assertVersionType(final Engine.Operation operation) {
        if (operation.origin() == Operation.Origin.REPLICA ||
                operation.origin() == Operation.Origin.PEER_RECOVERY ||
                operation.origin() == Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
            // ensure that replica operation has expected version type for replication
            // ensure that versionTypeForReplicationAndRecovery is idempotent
            assert operation.versionType() == operation.versionType().versionTypeForReplicationAndRecovery()
                    : "unexpected version type in request from [" + operation.origin().name() + "] " +
                    "found [" + operation.versionType().name() + "] " +
                    "expected [" + operation.versionType().versionTypeForReplicationAndRecovery().name() + "]";
        }
        return true;
    }

    private boolean assertIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        if (engineConfig.getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1) && origin == Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
            // legacy support
            assert seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : "old op recovering but it already has a seq no.;" +
                " index version: " + engineConfig.getIndexSettings().getIndexVersionCreated() + ", seqNo: " + seqNo;
        } else if (origin == Operation.Origin.PRIMARY) {
            assertPrimaryIncomingSequenceNumber(origin, seqNo);
        } else if (engineConfig.getIndexSettings().getIndexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1)) {
            // sequence number should be set when operation origin is not primary
            assert seqNo >= 0 : "recovery or replica ops should have an assigned seq no.; origin: " + origin;
        }
        return true;
    }

    protected boolean assertPrimaryIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        // sequence number should not be set when operation origin is primary
        assert seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO
                : "primary operations must never have an assigned sequence number but was [" + seqNo + "]";
        return true;
    }

    private boolean assertSequenceNumberBeforeIndexing(final Engine.Operation.Origin origin, final long seqNo) {
        if (engineConfig.getIndexSettings().getIndexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1) ||
            origin == Operation.Origin.PRIMARY) {
            // sequence number should be set when operation origin is primary or when all shards are on new nodes
            assert seqNo >= 0 : "ops should have an assigned seq no.; origin: " + origin;
        }
        return true;
    }

    private long generateSeqNoForOperation(final Operation operation) {
        assert operation.origin() == Operation.Origin.PRIMARY;
        return doGenerateSeqNoForOperation(operation);
    }

    /**
     * Generate the sequence number for the specified operation.
     *
     * @param operation the operation
     * @return the sequence number
     */
    long doGenerateSeqNoForOperation(final Operation operation) {
        return localCheckpointTracker.generateSeqNo();
    }

    private long getPrimaryTerm() {
        return engineConfig.getPrimaryTermSupplier().getAsLong();
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        assert Objects.equals(index.uid().field(), uidField) : index.uid().field();
        final boolean doThrottle = index.origin().isRecovery() == false;
        try (ReleasableLock releasableLock = readLock.acquire()) {
            ensureOpen();
            assert assertIncomingSequenceNumber(index.origin(), index.seqNo());
            assert assertVersionType(index);
            try (Releasable ignored = versionMap.acquireLock(index.uid().bytes());
                Releasable indexThrottle = doThrottle ? () -> {} : throttle.acquireThrottle()) {
                lastWriteNanos = index.startTime();
                /* A NOTE ABOUT APPEND ONLY OPTIMIZATIONS:
                 * if we have an autoGeneratedID that comes into the engine we can potentially optimize
                 * and just use addDocument instead of updateDocument and skip the entire version and index lookupVersion across the board.
                 * Yet, we have to deal with multiple document delivery, for this we use a property of the document that is added
                 * to detect if it has potentially been added before. We use the documents timestamp for this since it's something
                 * that:
                 *  - doesn't change per document
                 *  - is preserved in the transaction log
                 *  - and is assigned before we start to index / replicate
                 * NOTE: it's not important for this timestamp to be consistent across nodes etc. it's just a number that is in the common
                 * case increasing and can be used in the failure case when we retry and resent documents to establish a happens before relationship.
                 * for instance:
                 *  - doc A has autoGeneratedIdTimestamp = 10, isRetry = false
                 *  - doc B has autoGeneratedIdTimestamp = 9, isRetry = false
                 *
                 *  while both docs are in in flight, we disconnect on one node, reconnect and send doc A again
                 *  - now doc A' has autoGeneratedIdTimestamp = 10, isRetry = true
                 *
                 *  if A' arrives on the shard first we update maxUnsafeAutoIdTimestamp to 10 and use update document. All subsequent
                 *  documents that arrive (A and B) will also use updateDocument since their timestamps are less than maxUnsafeAutoIdTimestamp.
                 *  While this is not strictly needed for doc B it is just much simpler to implement since it will just de-optimize some doc in the worst case.
                 *
                 *  if A arrives on the shard first we use addDocument since maxUnsafeAutoIdTimestamp is < 10. A` will then just be skipped or calls
                 *  updateDocument.
                 */
                final IndexingStrategy plan = indexingStrategyForOperation(index);

                final IndexResult indexResult;
                if (plan.earlyResultOnPreFlightError.isPresent()) {
                    indexResult = plan.earlyResultOnPreFlightError.get();
                    assert indexResult.getResultType() == Result.Type.FAILURE : indexResult.getResultType();
                } else if (plan.indexIntoLucene || plan.addStaleOpToLucene) {
                    indexResult = indexIntoLucene(index, plan);
                } else {
                    indexResult = new IndexResult(
                            plan.versionForIndexing, getPrimaryTerm(), plan.seqNoForIndexing, plan.currentNotFoundOrDeleted);
                }
                if (index.origin().isFromTranslog() == false) {
                    final Translog.Location location;
                    if (indexResult.getResultType() == Result.Type.SUCCESS) {
                        location = translog.add(new Translog.Index(index, indexResult));
                    } else if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        // if we have document failure, record it as a no-op in the translog and Lucene with the generated seq_no
                        final NoOp noOp = new NoOp(indexResult.getSeqNo(), index.primaryTerm(), index.origin(),
                            index.startTime(), indexResult.getFailure().toString());
                        location = innerNoOp(noOp).getTranslogLocation();
                    } else {
                        location = null;
                    }
                    indexResult.setTranslogLocation(location);
                }
                if (plan.indexIntoLucene && indexResult.getResultType() == Result.Type.SUCCESS) {
                    final Translog.Location translogLocation = trackTranslogLocation.get() ? indexResult.getTranslogLocation() : null;
                    versionMap.maybePutIndexUnderLock(index.uid().bytes(),
                        new IndexVersionValue(translogLocation, plan.versionForIndexing, plan.seqNoForIndexing, index.primaryTerm()));
                }
                if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    localCheckpointTracker.markSeqNoAsCompleted(indexResult.getSeqNo());
                }
                indexResult.setTook(System.nanoTime() - index.startTime());
                indexResult.freeze();
                return indexResult;
            }
        } catch (RuntimeException | IOException e) {
            try {
                maybeFailEngine("index", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
    }

    protected final IndexingStrategy planIndexingAsNonPrimary(Index index) throws IOException {
        assertNonPrimaryOrigin(index);
        final IndexingStrategy plan;
        final boolean appendOnlyRequest = canOptimizeAddDocument(index);
        if (appendOnlyRequest && mayHaveBeenIndexedBefore(index) == false && index.seqNo() > maxSeqNoOfNonAppendOnlyOperations.get()) {
            /*
             * As soon as an append-only request was indexed into the primary, it can be exposed to a search then users can issue
             * a follow-up operation on it. In rare cases, the follow up operation can be arrived and processed on a replica before
             * the original append-only. In this case we can't simply proceed with the append only without consulting the version map.
             * If a replica has seen a non-append-only operation with a higher seqno than the seqno of an append-only, it may have seen
             * the document of that append-only request. However if the seqno of an append-only is higher than seqno of any non-append-only
             * requests, we can assert the replica have not seen the document of that append-only request, thus we can apply optimization.
             */
            assert index.version() == 1L : "can optimize on replicas but incoming version is [" + index.version() + "]";
            plan = IndexingStrategy.optimizedAppendOnly(index.seqNo(), 1L);
        } else {
            if (appendOnlyRequest == false) {
                maxSeqNoOfNonAppendOnlyOperations.updateAndGet(curr -> Math.max(index.seqNo(), curr));
                assert maxSeqNoOfNonAppendOnlyOperations.get() >= index.seqNo() : "max_seqno of non-append-only was not updated;" +
                    "max_seqno non-append-only [" + maxSeqNoOfNonAppendOnlyOperations.get() + "], seqno of index [" + index.seqNo() + "]";
            }
            versionMap.enforceSafeAccess();
            // drop out of order operations
            assert index.versionType().versionTypeForReplicationAndRecovery() == index.versionType() :
                "resolving out of order delivery based on versioning but version type isn't fit for it. got [" + index.versionType() + "]";
            // unlike the primary, replicas don't really care to about creation status of documents
            // this allows to ignore the case where a document was found in the live version maps in
            // a delete state and return false for the created flag in favor of code simplicity
            if (index.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && index.seqNo() <= localCheckpointTracker.getCheckpoint()){
                // the operation seq# is lower then the current local checkpoint and thus was already put into lucene
                // this can happen during recovery where older operations are sent from the translog that are already
                // part of the lucene commit (either from a peer recovery or a local translog)
                // or due to concurrent indexing & recovery. For the former it is important to skip lucene as the operation in
                // question may have been deleted in an out of order op that is not replayed.
                // See testRecoverFromStoreWithOutOfOrderDelete for an example of local recovery
                // See testRecoveryWithOutOfOrderDelete for an example of peer recovery
                plan = IndexingStrategy.processButSkipLucene(false, index.seqNo(), index.version());
            } else {
                final OpVsLuceneDocStatus opVsLucene;
                if (index.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    // This can happen if the primary is still on an old node and send traffic without seq#
                    // or we recover from translog created by an old version.
                    assert config().getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1) :
                        "index is newly created but op has no sequence numbers. op: " + index;
                    opVsLucene = compareOpToLuceneDocBasedOnVersions(index);
                } else {
                    opVsLucene = compareOpToLuceneDocBasedOnSeqNo(index);
                }
                if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
                    plan = IndexingStrategy.processAsStaleOp(softDeleteEnabled, index.seqNo(), index.version());
                } else {
                    plan = IndexingStrategy.processNormally(opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND,
                        index.seqNo(), index.version());
                }
            }
        }
        return plan;
    }

    protected IndexingStrategy indexingStrategyForOperation(final Index index) throws IOException {
        if (index.origin() == Operation.Origin.PRIMARY) {
            return planIndexingAsPrimary(index);
        } else {
            // non-primary mode (i.e., replica or recovery)
            return planIndexingAsNonPrimary(index);
        }
    }

    protected final IndexingStrategy planIndexingAsPrimary(Index index) throws IOException {
        assert index.origin() == Operation.Origin.PRIMARY : "planing as primary but origin isn't. got " + index.origin();
        assert getMaxSeqNoOfUpdatesOrDeletes() != SequenceNumbers.UNASSIGNED_SEQ_NO : "max_seq_no_of_updates is not initialized";
        final IndexingStrategy plan;
        // resolve an external operation into an internal one which is safe to replay
        if (canOptimizeAddDocument(index)) {
            if (mayHaveBeenIndexedBefore(index)) {
                plan = IndexingStrategy.overrideExistingAsIfNotThere(generateSeqNoForOperation(index), 1L);
                versionMap.enforceSafeAccess();
            } else {
                plan = IndexingStrategy.optimizedAppendOnly(generateSeqNoForOperation(index), 1L);
            }
        } else {
            versionMap.enforceSafeAccess();
            // resolves incoming version
            final VersionValue versionValue = resolveDocVersion(index);
            final long currentVersion;
            final boolean currentNotFoundOrDeleted;
            if (versionValue == null) {
                currentVersion = Versions.NOT_FOUND;
                currentNotFoundOrDeleted = true;
            } else {
                currentVersion = versionValue.version;
                currentNotFoundOrDeleted = versionValue.isDelete();
            }
            if (index.versionType().isVersionConflictForWrites(
                currentVersion, index.version(), currentNotFoundOrDeleted)) {
                final VersionConflictEngineException e =
                        new VersionConflictEngineException(shardId, index, currentVersion, currentNotFoundOrDeleted);
                plan = IndexingStrategy.skipDueToVersionConflict(e, currentNotFoundOrDeleted, currentVersion, getPrimaryTerm());
            } else {
                plan = IndexingStrategy.processNormally(currentNotFoundOrDeleted,
                    generateSeqNoForOperation(index),
                    index.versionType().updateVersion(currentVersion, index.version())
                );
            }
        }
        final boolean toAppend = plan.indexIntoLucene && plan.useLuceneUpdateDocument == false;
        if (toAppend == false) {
            advanceMaxSeqNoOfUpdatesOrDeletes(plan.seqNoForIndexing);
        }
        return plan;
    }

    private IndexResult indexIntoLucene(Index index, IndexingStrategy plan)
        throws IOException {
        assert assertSequenceNumberBeforeIndexing(index.origin(), plan.seqNoForIndexing);
        assert plan.versionForIndexing >= 0 : "version must be set. got " + plan.versionForIndexing;
        assert plan.indexIntoLucene || plan.addStaleOpToLucene;
        /* Update the document's sequence number and primary term; the sequence number here is derived here from either the sequence
         * number service if this is on the primary, or the existing document's sequence number if this is on the replica. The
         * primary term here has already been set, see IndexShard#prepareIndex where the Engine$Index operation is created.
         */
        index.parsedDoc().updateSeqID(plan.seqNoForIndexing, index.primaryTerm());
        index.parsedDoc().version().setLongValue(plan.versionForIndexing);
        try {
            if (plan.addStaleOpToLucene) {
                addStaleDocs(index.docs(), indexWriter);
            } else if (plan.useLuceneUpdateDocument) {
                assert assertMaxSeqNoOfUpdatesIsAdvanced(index.uid(), plan.seqNoForIndexing, true, true);
                updateDocs(index.uid(), index.docs(), indexWriter);
            } else {
                // document does not exists, we can optimize for create, but double check if assertions are running
                assert assertDocDoesNotExist(index, canOptimizeAddDocument(index) == false);
                addDocs(index.docs(), indexWriter);
            }
            return new IndexResult(plan.versionForIndexing, getPrimaryTerm(), plan.seqNoForIndexing, plan.currentNotFoundOrDeleted);
        } catch (Exception ex) {
            if (indexWriter.getTragicException() == null) {
                /* There is no tragic event recorded so this must be a document failure.
                 *
                 * The handling inside IW doesn't guarantee that an tragic / aborting exception
                 * will be used as THE tragicEventException since if there are multiple exceptions causing an abort in IW
                 * only one wins. Yet, only the one that wins will also close the IW and in turn fail the engine such that
                 * we can potentially handle the exception before the engine is failed.
                 * Bottom line is that we can only rely on the fact that if it's a document failure then
                 * `indexWriter.getTragicException()` will be null otherwise we have to rethrow and treat it as fatal or rather
                 * non-document failure
                 *
                 * we return a `MATCH_ANY` version to indicate no document was index. The value is
                 * not used anyway
                 */
                return new IndexResult(ex, Versions.MATCH_ANY, getPrimaryTerm(), plan.seqNoForIndexing);
            } else {
                throw ex;
            }
        }
    }

    /**
     * returns true if the indexing operation may have already be processed by this engine.
     * Note that it is OK to rarely return true even if this is not the case. However a `false`
     * return value must always be correct.
     *
     */
    private boolean mayHaveBeenIndexedBefore(Index index) {
        assert canOptimizeAddDocument(index);
        final boolean mayHaveBeenIndexBefore;
        if (index.isRetry()) {
            mayHaveBeenIndexBefore = true;
            updateAutoIdTimestamp(index.getAutoGeneratedIdTimestamp(), true);
            assert maxUnsafeAutoIdTimestamp.get() >= index.getAutoGeneratedIdTimestamp();
        } else {
            // in this case we force
            mayHaveBeenIndexBefore = maxUnsafeAutoIdTimestamp.get() >= index.getAutoGeneratedIdTimestamp();
            updateAutoIdTimestamp(index.getAutoGeneratedIdTimestamp(), false);
        }
        return mayHaveBeenIndexBefore;
    }

    // for testing
    long getMaxSeqNoOfNonAppendOnlyOperations() {
        return maxSeqNoOfNonAppendOnlyOperations.get();
    }

    private void addDocs(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
        numDocAppends.inc(docs.size());
    }

    private void addStaleDocs(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        assert softDeleteEnabled : "Add history documents but soft-deletes is disabled";
        for (ParseContext.Document doc : docs) {
            doc.add(softDeletesField); // soft-deleted every document before adding to Lucene
        }
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
    }

    protected static final class IndexingStrategy {
        final boolean currentNotFoundOrDeleted;
        final boolean useLuceneUpdateDocument;
        final long seqNoForIndexing;
        final long versionForIndexing;
        final boolean indexIntoLucene;
        final boolean addStaleOpToLucene;
        final Optional<IndexResult> earlyResultOnPreFlightError;

        private IndexingStrategy(boolean currentNotFoundOrDeleted, boolean useLuceneUpdateDocument,
                                 boolean indexIntoLucene, boolean addStaleOpToLucene, long seqNoForIndexing,
                                 long versionForIndexing, IndexResult earlyResultOnPreFlightError) {
            assert useLuceneUpdateDocument == false || indexIntoLucene :
                "use lucene update is set to true, but we're not indexing into lucene";
            assert (indexIntoLucene && earlyResultOnPreFlightError != null) == false :
                "can only index into lucene or have a preflight result but not both." +
                    "indexIntoLucene: " + indexIntoLucene
                    + "  earlyResultOnPreFlightError:" + earlyResultOnPreFlightError;
            this.currentNotFoundOrDeleted = currentNotFoundOrDeleted;
            this.useLuceneUpdateDocument = useLuceneUpdateDocument;
            this.seqNoForIndexing = seqNoForIndexing;
            this.versionForIndexing = versionForIndexing;
            this.indexIntoLucene = indexIntoLucene;
            this.addStaleOpToLucene = addStaleOpToLucene;
            this.earlyResultOnPreFlightError =
                earlyResultOnPreFlightError == null ? Optional.empty() :
                    Optional.of(earlyResultOnPreFlightError);
        }

        public static IndexingStrategy optimizedAppendOnly(long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(true, false, true, false, seqNoForIndexing, versionForIndexing, null);
        }

        public static IndexingStrategy skipDueToVersionConflict(
                VersionConflictEngineException e, boolean currentNotFoundOrDeleted, long currentVersion, long term) {
            final IndexResult result = new IndexResult(e, currentVersion, term);
            return new IndexingStrategy(
                    currentNotFoundOrDeleted, false, false, false, SequenceNumbers.UNASSIGNED_SEQ_NO, Versions.NOT_FOUND, result);
        }

        static IndexingStrategy processNormally(boolean currentNotFoundOrDeleted,
                                                long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, currentNotFoundOrDeleted == false,
                true, false, seqNoForIndexing, versionForIndexing, null);
        }

        static IndexingStrategy overrideExistingAsIfNotThere(
            long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(true, true, true, false, seqNoForIndexing, versionForIndexing, null);
        }

        public static IndexingStrategy processButSkipLucene(boolean currentNotFoundOrDeleted, long seqNoForIndexing,
                                                            long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, false, false, false, seqNoForIndexing, versionForIndexing, null);
        }

        static IndexingStrategy processAsStaleOp(boolean addStaleOpToLucene, long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(false, false, false, addStaleOpToLucene, seqNoForIndexing, versionForIndexing, null);
        }
    }

    /**
     * Asserts that the doc in the index operation really doesn't exist
     */
    private boolean assertDocDoesNotExist(final Index index, final boolean allowDeleted) throws IOException {
        // NOTE this uses direct access to the version map since we are in the assertion code where we maintain a secondary
        // map in the version map such that we don't need to refresh if we are unsafe;
        final VersionValue versionValue = versionMap.getVersionForAssert(index.uid().bytes());
        if (versionValue != null) {
            if (versionValue.isDelete() == false || allowDeleted == false) {
                throw new AssertionError("doc [" + index.type() + "][" + index.id() + "] exists in version map (version " + versionValue + ")");
            }
        } else {
            try (Searcher searcher = acquireSearcher("assert doc doesn't exist", SearcherScope.INTERNAL)) {
                final long docsWithId = searcher.searcher().count(new TermQuery(index.uid()));
                if (docsWithId > 0) {
                    throw new AssertionError("doc [" + index.type() + "][" + index.id() + "] exists [" + docsWithId + "] times in index");
                }
            }
        }
        return true;
    }

    private void updateDocs(final Term uid, final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (softDeleteEnabled) {
            if (docs.size() > 1) {
                indexWriter.softUpdateDocuments(uid, docs, softDeletesField);
            } else {
                indexWriter.softUpdateDocument(uid, docs.get(0), softDeletesField);
            }
        } else {
            if (docs.size() > 1) {
                indexWriter.updateDocuments(uid, docs);
            } else {
                indexWriter.updateDocument(uid, docs.get(0));
            }
        }
        numDocUpdates.inc(docs.size());
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        versionMap.enforceSafeAccess();
        assert Objects.equals(delete.uid().field(), uidField) : delete.uid().field();
        assert assertVersionType(delete);
        assert assertIncomingSequenceNumber(delete.origin(), delete.seqNo());
        final DeleteResult deleteResult;
        // NOTE: we don't throttle this when merges fall behind because delete-by-id does not create new segments:
        try (ReleasableLock ignored = readLock.acquire(); Releasable ignored2 = versionMap.acquireLock(delete.uid().bytes())) {
            ensureOpen();
            lastWriteNanos = delete.startTime();
            final DeletionStrategy plan = deletionStrategyForOperation(delete);

            if (plan.earlyResultOnPreflightError.isPresent()) {
                deleteResult = plan.earlyResultOnPreflightError.get();
            } else if (plan.deleteFromLucene || plan.addStaleOpToLucene) {
                deleteResult = deleteInLucene(delete, plan);
            } else {
                deleteResult = new DeleteResult(
                        plan.versionOfDeletion, getPrimaryTerm(), plan.seqNoOfDeletion, plan.currentlyDeleted == false);
            }
            if (delete.origin().isFromTranslog() == false) {
                final Translog.Location location;
                if (deleteResult.getResultType() == Result.Type.SUCCESS) {
                    location = translog.add(new Translog.Delete(delete, deleteResult));
                } else if (deleteResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    // if we have document failure, record it as a no-op in the translog and Lucene with the generated seq_no
                    final NoOp noOp = new NoOp(deleteResult.getSeqNo(), delete.primaryTerm(), delete.origin(),
                        delete.startTime(), deleteResult.getFailure().toString());
                    location = innerNoOp(noOp).getTranslogLocation();
                } else {
                    location = null;
                }
                deleteResult.setTranslogLocation(location);
            }
            if (deleteResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                localCheckpointTracker.markSeqNoAsCompleted(deleteResult.getSeqNo());
            }
            deleteResult.setTook(System.nanoTime() - delete.startTime());
            deleteResult.freeze();
        } catch (RuntimeException | IOException e) {
            try {
                maybeFailEngine("index", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
        maybePruneDeletes();
        return deleteResult;
    }

    protected DeletionStrategy deletionStrategyForOperation(final Delete delete) throws IOException {
        if (delete.origin() == Operation.Origin.PRIMARY) {
            return planDeletionAsPrimary(delete);
        } else {
            // non-primary mode (i.e., replica or recovery)
            return planDeletionAsNonPrimary(delete);
        }
    }

    protected final DeletionStrategy planDeletionAsNonPrimary(Delete delete) throws IOException {
        assertNonPrimaryOrigin(delete);
        // drop out of order operations
        assert delete.versionType().versionTypeForReplicationAndRecovery() == delete.versionType() :
            "resolving out of order delivery based on versioning but version type isn't fit for it. got ["
                + delete.versionType() + "]";
        maxSeqNoOfNonAppendOnlyOperations.updateAndGet(curr -> Math.max(delete.seqNo(), curr));
        assert maxSeqNoOfNonAppendOnlyOperations.get() >= delete.seqNo() : "max_seqno of non-append-only was not updated;" +
            "max_seqno non-append-only [" + maxSeqNoOfNonAppendOnlyOperations.get() + "], seqno of delete [" + delete.seqNo() + "]";
        // unlike the primary, replicas don't really care to about found status of documents
        // this allows to ignore the case where a document was found in the live version maps in
        // a delete state and return true for the found flag in favor of code simplicity
        final DeletionStrategy plan;
        if (delete.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && delete.seqNo() <= localCheckpointTracker.getCheckpoint()) {
            // the operation seq# is lower then the current local checkpoint and thus was already put into lucene
            // this can happen during recovery where older operations are sent from the translog that are already
            // part of the lucene commit (either from a peer recovery or a local translog)
            // or due to concurrent indexing & recovery. For the former it is important to skip lucene as the operation in
            // question may have been deleted in an out of order op that is not replayed.
            // See testRecoverFromStoreWithOutOfOrderDelete for an example of local recovery
            // See testRecoveryWithOutOfOrderDelete for an example of peer recovery
            plan = DeletionStrategy.processButSkipLucene(false, delete.seqNo(), delete.version());
        } else {
            final OpVsLuceneDocStatus opVsLucene;
            if (delete.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                // This can happen if the primary is still on an old node and send traffic without seq#
                // or we recover from translog created by an old version.
                assert config().getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1) :
                    "index is newly created but op has no sequence numbers. op: " + delete;
                opVsLucene = compareOpToLuceneDocBasedOnVersions(delete);
            } else {
                opVsLucene = compareOpToLuceneDocBasedOnSeqNo(delete);
            }
            if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
                plan = DeletionStrategy.processAsStaleOp(softDeleteEnabled, false, delete.seqNo(), delete.version());
            } else {
                plan = DeletionStrategy.processNormally(opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND,
                    delete.seqNo(), delete.version());
            }
        }
        return plan;
    }

    protected boolean assertNonPrimaryOrigin(final Operation operation) {
        assert operation.origin() != Operation.Origin.PRIMARY : "planing as primary but got " + operation.origin();
        return true;
    }

    protected final DeletionStrategy planDeletionAsPrimary(Delete delete) throws IOException {
        assert delete.origin() == Operation.Origin.PRIMARY : "planing as primary but got " + delete.origin();
        assert getMaxSeqNoOfUpdatesOrDeletes() != SequenceNumbers.UNASSIGNED_SEQ_NO : "max_seq_no_of_updates is not initialized";
        // resolve operation from external to internal
        final VersionValue versionValue = resolveDocVersion(delete);
        assert incrementVersionLookup();
        final long currentVersion;
        final boolean currentlyDeleted;
        if (versionValue == null) {
            currentVersion = Versions.NOT_FOUND;
            currentlyDeleted = true;
        } else {
            currentVersion = versionValue.version;
            currentlyDeleted = versionValue.isDelete();
        }
        final DeletionStrategy plan;
        if (delete.versionType().isVersionConflictForWrites(currentVersion, delete.version(), currentlyDeleted)) {
            final VersionConflictEngineException e = new VersionConflictEngineException(shardId, delete, currentVersion, currentlyDeleted);
            plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, getPrimaryTerm(), currentlyDeleted);
        } else {
            plan = DeletionStrategy.processNormally(
                    currentlyDeleted,
                    generateSeqNoForOperation(delete),
                    delete.versionType().updateVersion(currentVersion, delete.version()));
            advanceMaxSeqNoOfUpdatesOrDeletes(plan.seqNoOfDeletion);
        }
        return plan;
    }

    private DeleteResult deleteInLucene(Delete delete, DeletionStrategy plan) throws IOException {
        assert assertMaxSeqNoOfUpdatesIsAdvanced(delete.uid(), plan.seqNoOfDeletion, false, false);
        try {
            if (softDeleteEnabled) {
                final ParsedDocument tombstone = engineConfig.getTombstoneDocSupplier().newDeleteTombstoneDoc(delete.type(), delete.id());
                assert tombstone.docs().size() == 1 : "Tombstone doc should have single doc [" + tombstone + "]";
                tombstone.updateSeqID(plan.seqNoOfDeletion, delete.primaryTerm());
                tombstone.version().setLongValue(plan.versionOfDeletion);
                final ParseContext.Document doc = tombstone.docs().get(0);
                assert doc.getField(SeqNoFieldMapper.TOMBSTONE_NAME) != null :
                    "Delete tombstone document but _tombstone field is not set [" + doc + " ]";
                doc.add(softDeletesField);
                if (plan.addStaleOpToLucene || plan.currentlyDeleted) {
                    indexWriter.addDocument(doc);
                } else {
                    indexWriter.softUpdateDocument(delete.uid(), doc, softDeletesField);
                }
            } else if (plan.currentlyDeleted == false) {
                // any exception that comes from this is a either an ACE or a fatal exception there
                // can't be any document failures  coming from this
                indexWriter.deleteDocuments(delete.uid());
            }
            if (plan.deleteFromLucene) {
                numDocDeletes.inc();
                versionMap.putDeleteUnderLock(delete.uid().bytes(),
                    new DeleteVersionValue(plan.versionOfDeletion, plan.seqNoOfDeletion, delete.primaryTerm(),
                        engineConfig.getThreadPool().relativeTimeInMillis()));
            }
            return new DeleteResult(
                plan.versionOfDeletion, getPrimaryTerm(), plan.seqNoOfDeletion, plan.currentlyDeleted == false);
        } catch (Exception ex) {
            if (indexWriter.getTragicException() == null) {
                // there is no tragic event and such it must be a document level failure
                return new DeleteResult(
                        ex, plan.versionOfDeletion, getPrimaryTerm(), plan.seqNoOfDeletion, plan.currentlyDeleted == false);
            } else {
                throw ex;
            }
        }
    }

    protected static final class DeletionStrategy {
        // of a rare double delete
        final boolean deleteFromLucene;
        final boolean addStaleOpToLucene;
        final boolean currentlyDeleted;
        final long seqNoOfDeletion;
        final long versionOfDeletion;
        final Optional<DeleteResult> earlyResultOnPreflightError;

        private DeletionStrategy(boolean deleteFromLucene, boolean addStaleOpToLucene, boolean currentlyDeleted,
                                 long seqNoOfDeletion, long versionOfDeletion,
                                 DeleteResult earlyResultOnPreflightError) {
            assert (deleteFromLucene && earlyResultOnPreflightError != null) == false :
                "can only delete from lucene or have a preflight result but not both." +
                    "deleteFromLucene: " + deleteFromLucene
                    + "  earlyResultOnPreFlightError:" + earlyResultOnPreflightError;
            this.deleteFromLucene = deleteFromLucene;
            this.addStaleOpToLucene = addStaleOpToLucene;
            this.currentlyDeleted = currentlyDeleted;
            this.seqNoOfDeletion = seqNoOfDeletion;
            this.versionOfDeletion = versionOfDeletion;
            this.earlyResultOnPreflightError = earlyResultOnPreflightError == null ?
                Optional.empty() : Optional.of(earlyResultOnPreflightError);
        }

        public static DeletionStrategy skipDueToVersionConflict(
                VersionConflictEngineException e, long currentVersion, long term, boolean currentlyDeleted) {
            final long unassignedSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            final DeleteResult deleteResult = new DeleteResult(e, currentVersion, term, unassignedSeqNo, currentlyDeleted == false);
            return new DeletionStrategy(false, false, currentlyDeleted, unassignedSeqNo, Versions.NOT_FOUND, deleteResult);
        }

        static DeletionStrategy processNormally(boolean currentlyDeleted, long seqNoOfDeletion, long versionOfDeletion) {
            return new DeletionStrategy(true, false, currentlyDeleted, seqNoOfDeletion, versionOfDeletion, null);

        }

        public static DeletionStrategy processButSkipLucene(boolean currentlyDeleted,
                                                            long seqNoOfDeletion, long versionOfDeletion) {
            return new DeletionStrategy(false, false, currentlyDeleted, seqNoOfDeletion, versionOfDeletion, null);
        }

        static DeletionStrategy processAsStaleOp(boolean addStaleOpToLucene, boolean currentlyDeleted,
                                                        long seqNoOfDeletion, long versionOfDeletion) {
            return new DeletionStrategy(false, addStaleOpToLucene, currentlyDeleted, seqNoOfDeletion, versionOfDeletion, null);
        }
    }

    @Override
    public void maybePruneDeletes() {
        // It's expensive to prune because we walk the deletes map acquiring dirtyLock for each uid so we only do it
        // every 1/4 of gcDeletesInMillis:
        if (engineConfig.isEnableGcDeletes() && engineConfig.getThreadPool().relativeTimeInMillis() - lastDeleteVersionPruneTimeMSec > getGcDeletesInMillis() * 0.25) {
            pruneDeletedTombstones();
        }
    }

    @Override
    public NoOpResult noOp(final NoOp noOp) {
        NoOpResult noOpResult;
        try (ReleasableLock ignored = readLock.acquire()) {
            noOpResult = innerNoOp(noOp);
        } catch (final Exception e) {
            noOpResult = new NoOpResult(getPrimaryTerm(), noOp.seqNo(), e);
        }
        return noOpResult;
    }

    private NoOpResult innerNoOp(final NoOp noOp) throws IOException {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        assert noOp.seqNo() > SequenceNumbers.NO_OPS_PERFORMED;
        final long seqNo = noOp.seqNo();
        try (Releasable ignored = noOpKeyedLock.acquire(seqNo)) {
            final NoOpResult noOpResult;
            final Optional<Exception> preFlightError = preFlightCheckForNoOp(noOp);
            if (preFlightError.isPresent()) {
                noOpResult = new NoOpResult(getPrimaryTerm(), noOp.seqNo(), preFlightError.get());
            } else {
                Exception failure = null;
                if (softDeleteEnabled) {
                    try {
                        final ParsedDocument tombstone = engineConfig.getTombstoneDocSupplier().newNoopTombstoneDoc(noOp.reason());
                        tombstone.updateSeqID(noOp.seqNo(), noOp.primaryTerm());
                        // A noop tombstone does not require a _version but it's added to have a fully dense docvalues for the version field.
                        // 1L is selected to optimize the compression because it might probably be the most common value in version field.
                        tombstone.version().setLongValue(1L);
                        assert tombstone.docs().size() == 1 : "Tombstone should have a single doc [" + tombstone + "]";
                        final ParseContext.Document doc = tombstone.docs().get(0);
                        assert doc.getField(SeqNoFieldMapper.TOMBSTONE_NAME) != null
                            : "Noop tombstone document but _tombstone field is not set [" + doc + " ]";
                        doc.add(softDeletesField);
                        indexWriter.addDocument(doc);
                    } catch (Exception ex) {
                        if (maybeFailEngine("noop", ex)) {
                            throw ex;
                        }
                        failure = ex;
                    }
                }
                if (failure == null) {
                    noOpResult = new NoOpResult(getPrimaryTerm(), noOp.seqNo());
                } else {
                    noOpResult = new NoOpResult(getPrimaryTerm(), noOp.seqNo(), failure);
                }
                if (noOp.origin().isFromTranslog() == false && noOpResult.getResultType() == Result.Type.SUCCESS) {
                    final Translog.Location location = translog.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
                    noOpResult.setTranslogLocation(location);
                }
            }
            noOpResult.setTook(System.nanoTime() - noOp.startTime());
            noOpResult.freeze();
            return noOpResult;
        } finally {
            if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                localCheckpointTracker.markSeqNoAsCompleted(seqNo);
            }
        }
    }

    /**
     * Executes a pre-flight check for a given NoOp.
     * If this method returns a non-empty result, the engine won't process this NoOp and returns a failure.
     */
    protected Optional<Exception> preFlightCheckForNoOp(final NoOp noOp) throws IOException {
        return Optional.empty();
    }

    @Override
    public void refresh(String source) throws EngineException {
        refresh(source, SearcherScope.EXTERNAL);
    }

    final void refresh(String source, SearcherScope scope) throws EngineException {
        // we obtain a read lock here, since we don't want a flush to happen while we are refreshing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        // both refresh types will result in an internal refresh but only the external will also
        // pass the new reader reference to the external reader manager.
        final long localCheckpointBeforeRefresh = getLocalCheckpoint();

        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (store.tryIncRef()) {
                // increment the ref just to ensure nobody closes the store during a refresh
                try {
                    // even though we maintain 2 managers we really do the heavy-lifting only once.
                    // the second refresh will only do the extra work we have to do for warming caches etc.
                    ReferenceManager<IndexSearcher> referenceManager = getReferenceManager(scope);
                    // it is intentional that we never refresh both internal / external together
                    referenceManager.maybeRefreshBlocking();
                } finally {
                    store.decRef();
                }
                lastRefreshedCheckpointListener.updateRefreshedCheckpoint(localCheckpointBeforeRefresh);
            }
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("refresh failed source[" + source + "]", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, e);
        }
        assert lastRefreshedCheckpoint() >= localCheckpointBeforeRefresh : "refresh checkpoint was not advanced; " +
            "local_checkpoint=" + localCheckpointBeforeRefresh + " refresh_checkpoint=" + lastRefreshedCheckpoint();
        // TODO: maybe we should just put a scheduled job in threadPool?
        // We check for pruning in each delete request, but we also prune here e.g. in case a delete burst comes in and then no more deletes
        // for a long time:
        maybePruneDeletes();
        mergeScheduler.refreshConfig();
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
        // we obtain a read lock here, since we don't want a flush to happen while we are writing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        refresh("write indexing buffer", SearcherScope.INTERNAL);
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException {
        // best effort attempt before we acquire locks
        ensureOpen();
        if (indexWriter.hasUncommittedChanges()) {
            logger.trace("can't sync commit [{}]. have pending changes", syncId);
            return SyncedFlushResult.PENDING_OPERATIONS;
        }
        if (expectedCommitId.idsEqual(lastCommittedSegmentInfos.getId()) == false) {
            logger.trace("can't sync commit [{}]. current commit id is not equal to expected.", syncId);
            return SyncedFlushResult.COMMIT_MISMATCH;
        }
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            ensureCanFlush();
            // lets do a refresh to make sure we shrink the version map. This refresh will be either a no-op (just shrink the version map)
            // or we also have uncommitted changes and that causes this syncFlush to fail.
            refresh("sync_flush", SearcherScope.INTERNAL);
            if (indexWriter.hasUncommittedChanges()) {
                logger.trace("can't sync commit [{}]. have pending changes", syncId);
                return SyncedFlushResult.PENDING_OPERATIONS;
            }
            if (expectedCommitId.idsEqual(lastCommittedSegmentInfos.getId()) == false) {
                logger.trace("can't sync commit [{}]. current commit id is not equal to expected.", syncId);
                return SyncedFlushResult.COMMIT_MISMATCH;
            }
            logger.trace("starting sync commit [{}]", syncId);
            commitIndexWriter(indexWriter, translog, syncId);
            logger.debug("successfully sync committed. sync id [{}].", syncId);
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            return SyncedFlushResult.SUCCESS;
        } catch (IOException ex) {
            maybeFailEngine("sync commit", ex);
            throw new EngineException(shardId, "failed to sync commit", ex);
        }
    }

    final boolean tryRenewSyncCommit() {
        boolean renewed = false;
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            ensureCanFlush();
            String syncId = lastCommittedSegmentInfos.getUserData().get(SYNC_COMMIT_ID);
            long translogGenOfLastCommit = Long.parseLong(lastCommittedSegmentInfos.userData.get(Translog.TRANSLOG_GENERATION_KEY));
            if (syncId != null && indexWriter.hasUncommittedChanges() && translog.totalOperationsByMinGen(translogGenOfLastCommit) == 0) {
                logger.trace("start renewing sync commit [{}]", syncId);
                commitIndexWriter(indexWriter, translog, syncId);
                logger.debug("successfully sync committed. sync id [{}].", syncId);
                lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                renewed = true;
            }
        } catch (IOException ex) {
            maybeFailEngine("renew sync commit", ex);
            throw new EngineException(shardId, "failed to renew sync commit", ex);
        }
        if (renewed) {
            // refresh outside of the write lock
            // we have to refresh internal searcher here to ensure we release unreferenced segments.
            refresh("renew sync commit", SearcherScope.INTERNAL);
        }
        return renewed;
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        ensureOpen();
        final long translogGenerationOfLastCommit = Long.parseLong(lastCommittedSegmentInfos.userData.get(Translog.TRANSLOG_GENERATION_KEY));
        final long flushThreshold = config().getIndexSettings().getFlushThresholdSize().getBytes();
        if (translog.sizeInBytesByMinGen(translogGenerationOfLastCommit) < flushThreshold) {
            return false;
        }
        /*
         * We flush to reduce the size of uncommitted translog but strictly speaking the uncommitted size won't always be
         * below the flush-threshold after a flush. To avoid getting into an endless loop of flushing, we only enable the
         * periodically flush condition if this condition is disabled after a flush. The condition will change if the new
         * commit points to the later generation the last commit's(eg. gen-of-last-commit < gen-of-new-commit)[1].
         *
         * When the local checkpoint equals to max_seqno, and translog-gen of the last commit equals to translog-gen of
         * the new commit, we know that the last generation must contain operations because its size is above the flush
         * threshold and the flush-threshold is guaranteed to be higher than an empty translog by the setting validation.
         * This guarantees that the new commit will point to the newly rolled generation. In fact, this scenario only
         * happens when the generation-threshold is close to or above the flush-threshold; otherwise we have rolled
         * generations as the generation-threshold was reached, then the first condition (eg. [1]) is already satisfied.
         *
         * This method is to maintain translog only, thus IndexWriter#hasUncommittedChanges condition is not considered.
         */
        final long translogGenerationOfNewCommit =
            translog.getMinGenerationForSeqNo(localCheckpointTracker.getCheckpoint() + 1).translogFileGeneration;
        return translogGenerationOfLastCommit < translogGenerationOfNewCommit
            || localCheckpointTracker.getCheckpoint() == localCheckpointTracker.getMaxSeqNo();
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        ensureOpen();
        final byte[] newCommitId;
        /*
         * Unfortunately the lock order is important here. We have to acquire the readlock first otherwise
         * if we are flushing at the end of the recovery while holding the write lock we can deadlock if:
         *  Thread 1: flushes via API and gets the flush lock but blocks on the readlock since Thread 2 has the writeLock
         *  Thread 2: flushes at the end of the recovery holding the writeLock and blocks on the flushLock owned by Thread 1
         */
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (flushLock.tryLock() == false) {
                // if we can't get the lock right away we block if needed otherwise barf
                if (waitIfOngoing) {
                    logger.trace("waiting for in-flight flush to finish");
                    flushLock.lock();
                    logger.trace("acquired flush lock after blocking");
                } else {
                    return new CommitId(lastCommittedSegmentInfos.getId());
                }
            } else {
                logger.trace("acquired flush lock immediately");
            }
            try {
                // Only flush if (1) Lucene has uncommitted docs, or (2) forced by caller, or (3) the
                // newly created commit points to a different translog generation (can free translog)
                if (indexWriter.hasUncommittedChanges() || force || shouldPeriodicallyFlush()) {
                    ensureCanFlush();
                    try {
                        translog.rollGeneration();
                        logger.trace("starting commit for flush; commitTranslog=true");
                        commitIndexWriter(indexWriter, translog, null);
                        logger.trace("finished commit for flush");
                        // we need to refresh in order to clear older version values
                        refresh("version_table_flush", SearcherScope.INTERNAL);
                        translog.trimUnreferencedReaders();
                    } catch (AlreadyClosedException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                    refreshLastCommittedSegmentInfos();

                }
                newCommitId = lastCommittedSegmentInfos.getId();
            } catch (FlushFailedEngineException ex) {
                maybeFailEngine("flush", ex);
                throw ex;
            } finally {
                flushLock.unlock();
            }
        }
        // We don't have to do this here; we do it defensively to make sure that even if wall clock time is misbehaving
        // (e.g., moves backwards) we will at least still sometimes prune deleted tombstones:
        if (engineConfig.isEnableGcDeletes()) {
            pruneDeletedTombstones();
        }
        return new CommitId(newCommitId);
    }

    private void refreshLastCommittedSegmentInfos() {
    /*
     * we have to inc-ref the store here since if the engine is closed by a tragic event
     * we don't acquire the write lock and wait until we have exclusive access. This might also
     * dec the store reference which can essentially close the store and unless we can inc the reference
     * we can't use it.
     */
        store.incRef();
        try {
            // reread the last committed segment infos
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        } catch (Exception e) {
            if (isClosed.get() == false) {
                try {
                    logger.warn("failed to read latest segment infos on flush", e);
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                }
                if (Lucene.isCorruptionException(e)) {
                    throw new FlushFailedEngineException(shardId, e);
                }
            }
        } finally {
            store.decRef();
        }
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            translog.rollGeneration();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to roll translog", e);
        }
    }

    @Override
    public void trimUnreferencedTranslogFiles() throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to trim translog", e);
        }
    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return getTranslog().shouldRollGeneration();
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            translog.trimOperations(belowTerm, aboveSeqNo);
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog operations trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to trim translog operations", e);
        }
    }

    private void pruneDeletedTombstones() {
        /*
         * We need to deploy two different trimming strategies for GC deletes on primary and replicas. Delete operations on primary
         * are remembered for at least one GC delete cycle and trimmed periodically. This is, at the moment, the best we can do on
         * primary for user facing APIs but this arbitrary time limit is problematic for replicas. On replicas however we should
         * trim only deletes whose seqno at most the local checkpoint. This requirement is explained as follows.
         *
         * Suppose o1 and o2 are two operations on the same document with seq#(o1) < seq#(o2), and o2 arrives before o1 on the replica.
         * o2 is processed normally since it arrives first; when o1 arrives it should be discarded:
         * - If seq#(o1) <= LCP, then it will be not be added to Lucene, as it was already previously added.
         * - If seq#(o1)  > LCP, then it depends on the nature of o2:
         *   *) If o2 is a delete then its seq# is recorded in the VersionMap, since seq#(o2) > seq#(o1) > LCP,
         *      so a lookup can find it and determine that o1 is stale.
         *   *) If o2 is an indexing then its seq# is either in Lucene (if refreshed) or the VersionMap (if not refreshed yet),
         *      so a real-time lookup can find it and determine that o1 is stale.
         *
         * Here we prefer to deploy a single trimming strategy, which satisfies two constraints, on both primary and replicas because:
         * - It's simpler - no need to distinguish if an engine is running at primary mode or replica mode or being promoted.
         * - If a replica subsequently is promoted, user experience is maintained as that replica remembers deletes for the last GC cycle.
         *
         * However, the version map may consume less memory if we deploy two different trimming strategies for primary and replicas.
         */
        final long timeMSec = engineConfig.getThreadPool().relativeTimeInMillis();
        final long maxTimestampToPrune = timeMSec - engineConfig.getIndexSettings().getGcDeletesInMillis();
        versionMap.pruneTombstones(maxTimestampToPrune, localCheckpointTracker.getCheckpoint());
        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    // testing
    void clearDeletedTombstones() {
        versionMap.pruneTombstones(Long.MAX_VALUE, localCheckpointTracker.getMaxSeqNo());
    }

    // for testing
    final Collection<DeleteVersionValue> getDeletedTombstones() {
        return versionMap.getAllTombstones().values();
    }

    @Override
    public void forceMerge(final boolean flush, int maxNumSegments, boolean onlyExpungeDeletes,
                           final boolean upgrade, final boolean upgradeOnlyAncientSegments) throws EngineException, IOException {
        /*
         * We do NOT acquire the readlock here since we are waiting on the merges to finish
         * that's fine since the IW.rollback should stop all the threads and trigger an IOException
         * causing us to fail the forceMerge
         *
         * The way we implement upgrades is a bit hackish in the sense that we set an instance
         * variable and that this setting will thus apply to the next forced merge that will be run.
         * This is ok because (1) this is the only place we call forceMerge, (2) we have a single
         * thread for optimize, and the 'optimizeLock' guarding this code, and (3) ConcurrentMergeScheduler
         * syncs calls to findForcedMerges.
         */
        assert indexWriter.getConfig().getMergePolicy() instanceof ElasticsearchMergePolicy : "MergePolicy is " + indexWriter.getConfig().getMergePolicy().getClass().getName();
        ElasticsearchMergePolicy mp = (ElasticsearchMergePolicy) indexWriter.getConfig().getMergePolicy();
        optimizeLock.lock();
        try {
            ensureOpen();
            if (upgrade) {
                logger.info("starting segment upgrade upgradeOnlyAncientSegments={}", upgradeOnlyAncientSegments);
                mp.setUpgradeInProgress(true, upgradeOnlyAncientSegments);
            }
            store.incRef(); // increment the ref just to ensure nobody closes the store while we optimize
            try {
                if (onlyExpungeDeletes) {
                    assert upgrade == false;
                    indexWriter.forceMergeDeletes(true /* blocks and waits for merges*/);
                } else if (maxNumSegments <= 0) {
                    assert upgrade == false;
                    indexWriter.maybeMerge();
                } else {
                    indexWriter.forceMerge(maxNumSegments, true /* blocks and waits for merges*/);
                }
                if (flush) {
                    if (tryRenewSyncCommit() == false) {
                        flush(false, true);
                    }
                }
                if (upgrade) {
                    logger.info("finished segment upgrade");
                }
            } finally {
                store.decRef();
            }
        } catch (AlreadyClosedException ex) {
            /* in this case we first check if the engine is still open. If so this exception is just fine
             * and expected. We don't hold any locks while we block on forceMerge otherwise it would block
             * closing the engine as well. If we are not closed we pass it on to failOnTragicEvent which ensures
             * we are handling a tragic even exception here */
            ensureOpen(ex);
            failOnTragicEvent(ex);
            throw ex;
        } catch (Exception e) {
            try {
                maybeFailEngine("force merge", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        } finally {
            try {
                mp.setUpgradeInProgress(false, false); // reset it just to make sure we reset it in a case of an error
            } finally {
                optimizeLock.unlock();
            }
        }
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(final boolean flushFirst) throws EngineException {
        // we have to flush outside of the readlock otherwise we might have a problem upgrading
        // the to a write lock when we fail the engine in this operation
        if (flushFirst) {
            logger.trace("start flush for snapshot");
            flush(false, true);
            logger.trace("finish flush for snapshot");
        }
        final IndexCommit lastCommit = combinedDeletionPolicy.acquireIndexCommit(false);
        return new Engine.IndexCommitRef(lastCommit, () -> releaseIndexCommit(lastCommit));
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        final IndexCommit safeCommit = combinedDeletionPolicy.acquireIndexCommit(true);
        return new Engine.IndexCommitRef(safeCommit, () -> releaseIndexCommit(safeCommit));
    }

    private void releaseIndexCommit(IndexCommit snapshot) throws IOException {
        // Revisit the deletion policy if we can clean up the snapshotting commit.
        if (combinedDeletionPolicy.releaseCommit(snapshot)) {
            ensureOpen();
            // Here we don't have to trim translog because snapshotting an index commit
            // does not lock translog or prevents unreferenced files from trimming.
            indexWriter.deleteUnusedFiles();
        }
    }

    private boolean failOnTragicEvent(AlreadyClosedException ex) {
        final boolean engineFailed;
        // if we are already closed due to some tragic exception
        // we need to fail the engine. it might have already been failed before
        // but we are double-checking it's failed and closed
        if (indexWriter.isOpen() == false && indexWriter.getTragicException() != null) {
            final Exception tragicException;
            if (indexWriter.getTragicException() instanceof Exception) {
                tragicException = (Exception) indexWriter.getTragicException();
            } else {
                tragicException = new RuntimeException(indexWriter.getTragicException());
            }
            failEngine("already closed by tragic event on the index writer", tragicException);
            engineFailed = true;
        } else if (translog.isOpen() == false && translog.getTragicException() != null) {
            failEngine("already closed by tragic event on the translog", translog.getTragicException());
            engineFailed = true;
        } else if (failedEngine.get() == null && isClosed.get() == false) { // we are closed but the engine is not failed yet?
            // this smells like a bug - we only expect ACE if we are in a fatal case ie. either translog or IW is closed by
            // a tragic event or has closed itself. if that is not the case we are in a buggy state and raise an assertion error
            throw new AssertionError("Unexpected AlreadyClosedException", ex);
        } else {
            engineFailed = false;
        }
        return engineFailed;
    }

    @Override
    protected boolean maybeFailEngine(String source, Exception e) {
        boolean shouldFail = super.maybeFailEngine(source, e);
        if (shouldFail) {
            return true;
        }
        // Check for AlreadyClosedException -- ACE is a very special
        // exception that should only be thrown in a tragic event. we pass on the checks to failOnTragicEvent which will
        // throw and AssertionError if the tragic event condition is not met.
        if (e instanceof AlreadyClosedException) {
            return failOnTragicEvent((AlreadyClosedException)e);
        } else if (e != null &&
                ((indexWriter.isOpen() == false && indexWriter.getTragicException() == e)
                        || (translog.isOpen() == false && translog.getTragicException() == e))) {
            // this spot on - we are handling the tragic event exception here so we have to fail the engine
            // right away
            failEngine(source, e);
            return true;
        }
        return false;
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    protected final void writerSegmentStats(SegmentsStats stats) {
        stats.addVersionMapMemoryInBytes(versionMap.ramBytesUsed());
        stats.addIndexWriterMemoryInBytes(indexWriter.ramBytesUsed());
        stats.updateMaxUnsafeAutoIdTimestamp(maxUnsafeAutoIdTimestamp.get());
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        // We don't guard w/ readLock here, so we could throw AlreadyClosedException
        return indexWriter.ramBytesUsed() + versionMap.ramBytesUsedForRefresh();
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        try (ReleasableLock lock = readLock.acquire()) {
            Segment[] segmentsArr = getSegmentInfo(lastCommittedSegmentInfos, verbose);

            // fill in the merges flag
            Set<OnGoingMerge> onGoingMerges = mergeScheduler.onGoingMerges();
            for (OnGoingMerge onGoingMerge : onGoingMerges) {
                for (SegmentCommitInfo segmentInfoPerCommit : onGoingMerge.getMergedSegments()) {
                    for (Segment segment : segmentsArr) {
                        if (segment.getName().equals(segmentInfoPerCommit.info.name)) {
                            segment.mergeId = onGoingMerge.getId();
                            break;
                        }
                    }
                }
            }
            return Arrays.asList(segmentsArr);
        }
    }

    /**
     * Closes the engine without acquiring the write lock. This should only be
     * called while the write lock is hold or in a disaster condition ie. if the engine
     * is failed.
     */
    @Override
    protected final void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread() : "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                this.versionMap.clear();
                if (internalSearcherManager != null) {
                    internalSearcherManager.removeListener(versionMap);
                }
                try {
                    IOUtils.close(externalSearcherManager, internalSearcherManager);
                } catch (Exception e) {
                    logger.warn("Failed to close SearcherManager", e);
                }
                try {
                    IOUtils.close(translog);
                } catch (Exception e) {
                    logger.warn("Failed to close translog", e);
                }
                // no need to commit in this case!, we snapshot before we close the shard, so translog and all sync'ed
                logger.trace("rollback indexWriter");
                try {
                    indexWriter.rollback();
                } catch (AlreadyClosedException ex) {
                    failOnTragicEvent(ex);
                    throw ex;
                }
                logger.trace("rollback indexWriter done");
            } catch (Exception e) {
                logger.warn("failed to rollback writer on close", e);
            } finally {
                try {
                    store.decRef();
                    logger.debug("engine closed [{}]", reason);
                } finally {
                    closedLatch.countDown();
                }
            }
        }
    }

    @Override
    protected final ReferenceManager<IndexSearcher> getReferenceManager(SearcherScope scope) {
        switch (scope) {
            case INTERNAL:
                return internalSearcherManager;
            case EXTERNAL:
                return externalSearcherManager;
            default:
                throw new IllegalStateException("unknown scope: " + scope);
        }
    }

    private long loadCurrentVersionFromIndex(Term uid) throws IOException {
        assert incrementIndexVersionLookup();
        try (Searcher searcher = acquireSearcher("load_version", SearcherScope.INTERNAL)) {
            return VersionsAndSeqNoResolver.loadVersion(searcher.reader(), uid);
        }
    }

    private IndexWriter createWriter() throws IOException {
        try {
            final IndexWriterConfig iwc = getIndexWriterConfig();
            return createWriter(store.directory(), iwc);
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    // pkg-private for testing
    IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
        if (Assertions.ENABLED) {
            return new AssertingIndexWriter(directory, iwc);
        } else {
            return new IndexWriter(directory, iwc);
        }
    }

    private IndexWriterConfig getIndexWriterConfig() {
        final IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
        iwc.setCommitOnClose(false); // we by default don't commit on close
        iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        iwc.setIndexDeletionPolicy(combinedDeletionPolicy);
        // with tests.verbose, lucene sets this up: plumb to align with filesystem stream
        boolean verbose = false;
        try {
            verbose = Boolean.parseBoolean(System.getProperty("tests.verbose"));
        } catch (Exception ignore) {
        }
        iwc.setInfoStream(verbose ? InfoStream.getDefault() : new LoggerInfoStream(logger));
        iwc.setMergeScheduler(mergeScheduler);
        // Give us the opportunity to upgrade old segments while performing
        // background merges
        MergePolicy mergePolicy = config().getMergePolicy();
        if (softDeleteEnabled) {
            iwc.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
            mergePolicy = new RecoverySourcePruneMergePolicy(SourceFieldMapper.RECOVERY_SOURCE_NAME, softDeletesPolicy::getRetentionQuery,
                new SoftDeletesRetentionMergePolicy(Lucene.SOFT_DELETES_FIELD, softDeletesPolicy::getRetentionQuery, mergePolicy));
        }
        iwc.setMergePolicy(new ElasticsearchMergePolicy(mergePolicy));
        iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().getMbFrac());
        iwc.setCodec(engineConfig.getCodec());
        iwc.setUseCompoundFile(true); // always use compound on flush - reduces # of file-handles on refresh
        if (config().getIndexSort() != null) {
            iwc.setIndexSort(config().getIndexSort());
        }
        return iwc;
    }

    /** Extended SearcherFactory that warms the segments if needed when acquiring a new searcher */
    static final class SearchFactory extends EngineSearcherFactory {
        private final Engine.Warmer warmer;
        private final Logger logger;
        private final AtomicBoolean isEngineClosed;

        SearchFactory(Logger logger, AtomicBoolean isEngineClosed, EngineConfig engineConfig) {
            super(engineConfig);
            warmer = engineConfig.getWarmer();
            this.logger = logger;
            this.isEngineClosed = isEngineClosed;
        }

        @Override
        public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) throws IOException {
            IndexSearcher searcher = super.newSearcher(reader, previousReader);
            if (reader instanceof LeafReader && isMergedSegment((LeafReader) reader)) {
                // we call newSearcher from the IndexReaderWarmer which warms segments during merging
                // in that case the reader is a LeafReader and all we need to do is to build a new Searcher
                // and return it since it does it's own warming for that particular reader.
                return searcher;
            }
            if (warmer != null) {
                try {
                    assert searcher.getIndexReader() instanceof ElasticsearchDirectoryReader : "this class needs an ElasticsearchDirectoryReader but got: " + searcher.getIndexReader().getClass();
                    warmer.warm(new Searcher("top_reader_warming", searcher, () -> {}));
                } catch (Exception e) {
                    if (isEngineClosed.get() == false) {
                        logger.warn("failed to prepare/warm", e);
                    }
                }
            }
            return searcher;
        }
    }

    @Override
    public void activateThrottling() {
        int count = throttleRequestCount.incrementAndGet();
        assert count >= 1 : "invalid post-increment throttleRequestCount=" + count;
        if (count == 1) {
            throttle.activate();
        }
    }

    @Override
    public void deactivateThrottling() {
        int count = throttleRequestCount.decrementAndGet();
        assert count >= 0 : "invalid post-decrement throttleRequestCount=" + count;
        if (count == 0) {
            throttle.deactivate();
        }
    }

    @Override
    public boolean isThrottled() {
        return throttle.isThrottled();
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return throttle.getThrottleTimeInMillis();
    }

    long getGcDeletesInMillis() {
        return engineConfig.getIndexSettings().getGcDeletesInMillis();
    }

    LiveIndexWriterConfig getCurrentIndexWriterConfig() {
        return indexWriter.getConfig();
    }

    private final class EngineMergeScheduler extends ElasticsearchConcurrentMergeScheduler {
        private final AtomicInteger numMergesInFlight = new AtomicInteger(0);
        private final AtomicBoolean isThrottling = new AtomicBoolean();

        EngineMergeScheduler(ShardId shardId, IndexSettings indexSettings) {
            super(shardId, indexSettings);
        }

        @Override
        public synchronized void beforeMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMergeCount();
            if (numMergesInFlight.incrementAndGet() > maxNumMerges) {
                if (isThrottling.getAndSet(true) == false) {
                    logger.info("now throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    activateThrottling();
                }
            }
        }

        @Override
        public synchronized void afterMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMergeCount();
            if (numMergesInFlight.decrementAndGet() < maxNumMerges) {
                if (isThrottling.getAndSet(false)) {
                    logger.info("stop throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    deactivateThrottling();
                }
            }
            if (indexWriter.hasPendingMerges() == false && System.nanoTime() - lastWriteNanos >= engineConfig.getFlushMergesAfter().nanos()) {
                // NEVER do this on a merge thread since we acquire some locks blocking here and if we concurrently rollback the writer
                // we deadlock on engine#close for instance.
                engineConfig.getThreadPool().executor(ThreadPool.Names.FLUSH).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        if (isClosed.get() == false) {
                            logger.warn("failed to flush after merge has finished");
                        }
                    }

                    @Override
                    protected void doRun() throws Exception {
                        // if we have no pending merges and we are supposed to flush once merges have finished
                        // we try to renew a sync commit which is the case when we are having a big merge after we
                        // are inactive. If that didn't work we go and do a real flush which is ok since it only doesn't work
                        // if we either have records in the translog or if we don't have a sync ID at all...
                        // maybe even more important, we flush after all merges finish and we are inactive indexing-wise to
                        // free up transient disk usage of the (presumably biggish) segments that were just merged
                        if (tryRenewSyncCommit() == false) {
                            flush();
                        }
                    }
                });

            }
        }

        @Override
        protected void handleMergeException(final Directory dir, final Throwable exc) {
            engineConfig.getThreadPool().generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("merge failure action rejected", e);
                }

                @Override
                protected void doRun() throws Exception {
                    /*
                     * We do this on another thread rather than the merge thread that we are initially called on so that we have complete
                     * confidence that the call stack does not contain catch statements that would cause the error that might be thrown
                     * here from being caught and never reaching the uncaught exception handler.
                     */
                    failEngine("merge failed", new MergePolicy.MergeException(exc, dir));
                }
            });
        }
    }

    /**
     * Commits the specified index writer.
     *
     * @param writer   the index writer to commit
     * @param translog the translog
     * @param syncId   the sync flush ID ({@code null} if not committing a synced flush)
     * @throws IOException if an I/O exception occurs committing the specfied writer
     */
    protected void commitIndexWriter(final IndexWriter writer, final Translog translog, @Nullable final String syncId) throws IOException {
        ensureCanFlush();
        try {
            final long localCheckpoint = localCheckpointTracker.getCheckpoint();
            final Translog.TranslogGeneration translogGeneration = translog.getMinGenerationForSeqNo(localCheckpoint + 1);
            final String translogFileGeneration = Long.toString(translogGeneration.translogFileGeneration);
            final String translogUUID = translogGeneration.translogUUID;
            final String localCheckpointValue = Long.toString(localCheckpoint);

            writer.setLiveCommitData(() -> {
                /*
                 * The user data captured above (e.g. local checkpoint) contains data that must be evaluated *before* Lucene flushes
                 * segments, including the local checkpoint amongst other values. The maximum sequence number is different, we never want
                 * the maximum sequence number to be less than the last sequence number to go into a Lucene commit, otherwise we run the
                 * risk of re-using a sequence number for two different documents when restoring from this commit point and subsequently
                 * writing new documents to the index. Since we only know which Lucene documents made it into the final commit after the
                 * {@link IndexWriter#commit()} call flushes all documents, we defer computation of the maximum sequence number to the time
                 * of invocation of the commit data iterator (which occurs after all documents have been flushed to Lucene).
                 */
                final Map<String, String> commitData = new HashMap<>(6);
                commitData.put(Translog.TRANSLOG_GENERATION_KEY, translogFileGeneration);
                commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
                commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, localCheckpointValue);
                if (syncId != null) {
                    commitData.put(Engine.SYNC_COMMIT_ID, syncId);
                }
                commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));
                commitData.put(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp.get()));
                commitData.put(HISTORY_UUID_KEY, historyUUID);
                if (softDeleteEnabled) {
                    commitData.put(Engine.MIN_RETAINED_SEQNO, Long.toString(softDeletesPolicy.getMinRetainedSeqNo()));
                }
                logger.trace("committing writer with commit data [{}]", commitData);
                return commitData.entrySet().iterator();
            });

            writer.commit();
        } catch (final Exception ex) {
            try {
                failEngine("lucene commit failed", ex);
            } catch (final Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        } catch (final AssertionError e) {
            /*
             * If assertions are enabled, IndexWriter throws AssertionError on commit if any files don't exist, but tests that randomly
             * throw FileNotFoundException or NoSuchFileException can also hit this.
             */
            if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                final EngineException engineException = new EngineException(shardId, "failed to commit engine", e);
                try {
                    failEngine("lucene commit failed", engineException);
                } catch (final Exception inner) {
                    engineException.addSuppressed(inner);
                }
                throw engineException;
            } else {
                throw e;
            }
        }
    }

    private void ensureCanFlush() {
        // translog recover happens after the engine is fully constructed
        // if we are in this stage we have to prevent flushes from this
        // engine otherwise we might loose documents if the flush succeeds
        // and the translog recover fails we we "commit" the translog on flush.
        if (pendingTranslogRecovery.get()) {
            throw new IllegalStateException(shardId.toString() + " flushes are disabled - pending translog recovery");
        }
    }

    public void onSettingsChanged() {
        mergeScheduler.refreshConfig();
        // config().isEnableGcDeletes() or config.getGcDeletesInMillis() may have changed:
        maybePruneDeletes();
        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            // this is an anti-viral settings you can only opt out for the entire index
            // only if a shard starts up again due to relocation or if the index is closed
            // the setting will be re-interpreted if it's set to true
            updateAutoIdTimestamp(Long.MAX_VALUE, true);
        }
        final TranslogDeletionPolicy translogDeletionPolicy = translog.getDeletionPolicy();
        final IndexSettings indexSettings = engineConfig.getIndexSettings();
        translogDeletionPolicy.setRetentionAgeInMillis(indexSettings.getTranslogRetentionAge().getMillis());
        translogDeletionPolicy.setRetentionSizeInBytes(indexSettings.getTranslogRetentionSize().getBytes());
        softDeletesPolicy.setRetentionOperations(indexSettings.getSoftDeleteRetentionOperations());
    }

    public MergeStats getMergeStats() {
        return mergeScheduler.stats();
    }

    // Used only for testing! Package private to prevent anyone else from using it
    LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return getTranslog().getLastSyncedGlobalCheckpoint();
    }

    @Override
    public long getLocalCheckpoint() {
        return localCheckpointTracker.getCheckpoint();
    }

    @Override
    public void waitForOpsToComplete(long seqNo) throws InterruptedException {
        localCheckpointTracker.waitForOpsToComplete(seqNo);
    }

    /**
     * Checks if the given operation has been processed in this engine or not.
     * @return true if the given operation was processed; otherwise false.
     */
    protected final boolean hasBeenProcessedBefore(Operation op) {
        if (Assertions.ENABLED) {
            assert op.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "operation is not assigned seq_no";
            if (op.operationType() == Operation.TYPE.NO_OP) {
                assert noOpKeyedLock.isHeldByCurrentThread(op.seqNo());
            } else {
                assert versionMap.assertKeyedLockHeldByCurrentThread(op.uid().bytes());
            }
        }
        return localCheckpointTracker.contains(op.seqNo());
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return localCheckpointTracker.getStats(globalCheckpoint);
    }

    /**
     * Returns the number of times a version was looked up either from the index.
     * Note this is only available if assertions are enabled
     */
    long getNumIndexVersionsLookups() { // for testing
        return numIndexVersionsLookups.count();
    }

    /**
     * Returns the number of times a version was looked up either from memory or from the index.
     * Note this is only available if assertions are enabled
     */
    long getNumVersionLookups() { // for testing
        return numVersionLookups.count();
    }

    private boolean incrementVersionLookup() { // only used by asserts
        numVersionLookups.inc();
        return true;
    }

    private boolean incrementIndexVersionLookup() {
        numIndexVersionsLookups.inc();
        return true;
    }

    int getVersionMapSize() {
        return versionMap.getAllCurrent().size();
    }

    boolean isSafeAccessRequired() {
        return versionMap.isSafeAccessRequired();
    }

    /**
     * Returns the number of documents have been deleted since this engine was opened.
     * This count does not include the deletions from the existing segments before opening engine.
     */
    long getNumDocDeletes() {
        return numDocDeletes.count();
    }

    /**
     * Returns the number of documents have been appended since this engine was opened.
     * This count does not include the appends from the existing segments before opening engine.
     */
    long getNumDocAppends() {
        return numDocAppends.count();
    }

    /**
     * Returns the number of documents have been updated since this engine was opened.
     * This count does not include the updates from the existing segments before opening engine.
     */
    long getNumDocUpdates() {
        return numDocUpdates.count();
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(String source, MapperService mapperService,
                                                long fromSeqNo, long toSeqNo, boolean requiredFullRange) throws IOException {
        // TODO: Should we defer the refresh until we really need it?
        ensureOpen();
        refreshIfNeeded(source, toSeqNo);
        Searcher searcher = acquireSearcher(source, SearcherScope.INTERNAL);
        try {
            LuceneChangesSnapshot snapshot = new LuceneChangesSnapshot(
                searcher, mapperService, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE, fromSeqNo, toSeqNo, requiredFullRange);
            searcher = null;
            return snapshot;
        } catch (Exception e) {
            try {
                maybeFailEngine("acquire changes snapshot", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        } finally {
            IOUtils.close(searcher);
        }
    }

    @Override
    public boolean hasCompleteOperationHistory(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            return getMinRetainedSeqNo() <= startingSeqNo;
        } else {
            final long currentLocalCheckpoint = getLocalCheckpointTracker().getCheckpoint();
            final LocalCheckpointTracker tracker = new LocalCheckpointTracker(startingSeqNo, startingSeqNo - 1);
            try (Translog.Snapshot snapshot = getTranslog().newSnapshotFromMinSeqNo(startingSeqNo)) {
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    if (operation.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        tracker.markSeqNoAsCompleted(operation.seqNo());
                    }
                }
            }
            return tracker.getCheckpoint() >= currentLocalCheckpoint;
        }
    }

    /**
     * Returns the minimum seqno that is retained in the Lucene index.
     * Operations whose seq# are at least this value should exist in the Lucene index.
     */
    final long getMinRetainedSeqNo() {
        assert softDeleteEnabled : Thread.currentThread().getName();
        return softDeletesPolicy.getMinRetainedSeqNo();
    }

    @Override
    public Closeable acquireRetentionLockForPeerRecovery() {
        if (softDeleteEnabled) {
            return softDeletesPolicy.acquireRetentionLock();
        } else {
            return translog.acquireRetentionLock();
        }
    }

    @Override
    public boolean isRecovering() {
        return pendingTranslogRecovery.get();
    }

    /**
     * Gets the commit data from {@link IndexWriter} as a map.
     */
    private static Map<String, String> commitDataAsMap(final IndexWriter indexWriter) {
        Map<String, String> commitData = new HashMap<>(6);
        for (Map.Entry<String, String> entry : indexWriter.getLiveCommitData()) {
            commitData.put(entry.getKey(), entry.getValue());
        }
        return commitData;
    }

    private final class AssertingIndexWriter extends IndexWriter {
        AssertingIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
            super(d, conf);
        }
        @Override
        public long updateDocument(Term term, Iterable<? extends IndexableField> doc) throws IOException {
            assert softDeleteEnabled == false : "Call #updateDocument but soft-deletes is enabled";
            return super.updateDocument(term, doc);
        }
        @Override
        public long updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
            assert softDeleteEnabled == false : "Call #updateDocuments but soft-deletes is enabled";
            return super.updateDocuments(delTerm, docs);
        }
        @Override
        public long deleteDocuments(Term... terms) throws IOException {
            assert softDeleteEnabled == false : "Call #deleteDocuments but soft-deletes is enabled";
            return super.deleteDocuments(terms);
        }
        @Override
        public long softUpdateDocument(Term term, Iterable<? extends IndexableField> doc, Field... softDeletes) throws IOException {
            assert softDeleteEnabled : "Call #softUpdateDocument but soft-deletes is disabled";
            return super.softUpdateDocument(term, doc, softDeletes);
        }
        @Override
        public long softUpdateDocuments(Term term, Iterable<? extends Iterable<? extends IndexableField>> docs, Field... softDeletes) throws IOException {
            assert softDeleteEnabled : "Call #softUpdateDocuments but soft-deletes is disabled";
            return super.softUpdateDocuments(term, docs, softDeletes);
        }
    }

    /**
     * Returned the last local checkpoint value has been refreshed internally.
     */
    final long lastRefreshedCheckpoint() {
        return lastRefreshedCheckpointListener.refreshedCheckpoint.get();
    }


    private final Object refreshIfNeededMutex = new Object();

    /**
     * Refresh this engine **internally** iff the requesting seq_no is greater than the last refreshed checkpoint.
     */
    protected final void refreshIfNeeded(String source, long requestingSeqNo) {
        if (lastRefreshedCheckpoint() < requestingSeqNo) {
            synchronized (refreshIfNeededMutex) {
                if (lastRefreshedCheckpoint() < requestingSeqNo) {
                    refresh(source, SearcherScope.INTERNAL);
                }
            }
        }
    }

    private final class LastRefreshedCheckpointListener implements ReferenceManager.RefreshListener {
        final AtomicLong refreshedCheckpoint;
        private long pendingCheckpoint;

        LastRefreshedCheckpointListener(long initialLocalCheckpoint) {
            this.refreshedCheckpoint = new AtomicLong(initialLocalCheckpoint);
        }

        @Override
        public void beforeRefresh() {
            // all changes until this point should be visible after refresh
            pendingCheckpoint = localCheckpointTracker.getCheckpoint();
        }

        @Override
        public void afterRefresh(boolean didRefresh) {
            if (didRefresh) {
                updateRefreshedCheckpoint(pendingCheckpoint);
            }
        }

        void updateRefreshedCheckpoint(long checkpoint) {
            refreshedCheckpoint.updateAndGet(curr -> Math.max(curr, checkpoint));
            assert refreshedCheckpoint.get() >= checkpoint : refreshedCheckpoint.get() + " < " + checkpoint;
        }
    }

    @Override
    public final long getMaxSeenAutoIdTimestamp() {
        return maxSeenAutoIdTimestamp.get();
    }

    @Override
    public final void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        updateAutoIdTimestamp(newTimestamp, true);
    }

    private void updateAutoIdTimestamp(long newTimestamp, boolean unsafe) {
        assert newTimestamp >= -1 : "invalid timestamp [" + newTimestamp + "]";
        maxSeenAutoIdTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
        if (unsafe) {
            maxUnsafeAutoIdTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
        }
        assert maxUnsafeAutoIdTimestamp.get() <= maxSeenAutoIdTimestamp.get();
    }

    private boolean assertMaxSeqNoOfUpdatesIsAdvanced(Term id, long seqNo, boolean allowDeleted, boolean relaxIfGapInSeqNo) {
        final long maxSeqNoOfUpdates = getMaxSeqNoOfUpdatesOrDeletes();
        // If the primary is on an old version which does not replicate msu, we need to relax this assertion for that.
        if (maxSeqNoOfUpdates == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            assert config().getIndexSettings().getIndexVersionCreated().before(Version.V_6_5_0);
            return true;
        }
        // We treat a delete on the tombstones on replicas as a regular document, then use updateDocument (not addDocument).
        if (allowDeleted) {
            final VersionValue versionValue = versionMap.getVersionForAssert(id.bytes());
            if (versionValue != null && versionValue.isDelete()) {
                return true;
            }
        }
        // Operations can be processed on a replica in a different order than on the primary. If the order on the primary is index-1,
        // delete-2, index-3, and the order on a replica is index-1, index-3, delete-2, then the msu of index-3 on the replica is 2
        // even though it is an update (overwrites index-1). We should relax this assertion if there is a pending gap in the seq_no.
        if (relaxIfGapInSeqNo && getLocalCheckpoint() < maxSeqNoOfUpdates) {
            return true;
        }
        assert seqNo <= maxSeqNoOfUpdates : "id=" + id + " seq_no=" + seqNo + " msu=" + maxSeqNoOfUpdates;
        return true;
    }

    @Override
    public void initializeMaxSeqNoOfUpdatesOrDeletes() {
        assert getMaxSeqNoOfUpdatesOrDeletes() == SequenceNumbers.UNASSIGNED_SEQ_NO :
            "max_seq_no_of_updates is already initialized [" + getMaxSeqNoOfUpdatesOrDeletes() + "]";
        final long maxSeqNo = SequenceNumbers.max(localCheckpointTracker.getMaxSeqNo(), translog.getMaxSeqNo());
        advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNo);
    }
}
