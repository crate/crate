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

package org.elasticsearch.index;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

public class IndexService extends AbstractIndexComponent implements IndicesClusterStateService.AllocatedIndex<IndexShard> {

    private final IndexEventListener eventListener;
    private final IndexFieldDataService indexFieldData;
    private final BitsetFilterCache bitsetFilterCache;
    private final NodeEnvironment nodeEnv;
    private final ShardStoreDeleter shardStoreDeleter;
    private final IndexStore indexStore;
    private final IndexSearcherWrapper searcherWrapper;
    private final IndexCache indexCache;
    private final MapperService mapperService;
    private final NamedXContentRegistry xContentRegistry;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final EngineFactory engineFactory;
    private final IndexWarmer warmer;
    private volatile Map<Integer, IndexShard> shards = emptyMap();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean deleted = new AtomicBoolean(false);
    private final IndexSettings indexSettings;
    private final List<IndexingOperationListener> indexingOperationListeners;
    private volatile AsyncRefreshTask refreshTask;
    private volatile AsyncTranslogFSync fsyncTask;
    private volatile AsyncGlobalCheckpointTask globalCheckpointTask;

    // don't convert to Setting<> and register... we only set this in tests and register via a plugin
    private final String INDEX_TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING = "index.translog.retention.check_interval";

    private final AsyncTrimTranslogTask trimTranslogTask;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private final CircuitBreakerService circuitBreakerService;
    private Supplier<Sort> indexSortSupplier;

    public IndexService(
            IndexSettings indexSettings,
            NodeEnvironment nodeEnv,
            NamedXContentRegistry xContentRegistry,
            ShardStoreDeleter shardStoreDeleter,
            AnalysisRegistry registry,
            EngineFactory engineFactory,
            CircuitBreakerService circuitBreakerService,
            BigArrays bigArrays,
            ThreadPool threadPool,
            QueryCache queryCache,
            IndexStore indexStore,
            IndexEventListener eventListener,
            IndexModule.IndexSearcherWrapperFactory wrapperFactory,
            MapperRegistry mapperRegistry,
            IndicesFieldDataCache indicesFieldDataCache,
            List<IndexingOperationListener> indexingOperationListeners,
            NamedWriteableRegistry namedWriteableRegistry) throws IOException {
        super(indexSettings);
        this.indexSettings = indexSettings;
        this.xContentRegistry = xContentRegistry;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.circuitBreakerService = circuitBreakerService;
        this.mapperService = new MapperService(indexSettings, registry.build(indexSettings), xContentRegistry,
            mapperRegistry,
            // we parse all percolator queries as they would be parsed on shard 0
            () -> newQueryShardContext(0, null, System::currentTimeMillis, null));
        this.indexFieldData = new IndexFieldDataService(indexSettings, indicesFieldDataCache, circuitBreakerService, mapperService);
        if (indexSettings.getIndexSortConfig().hasIndexSort()) {
            // we delay the actual creation of the sort order for this index because the mapping has not been merged yet.
            // The sort order is validated right after the merge of the mapping later in the process.
            this.indexSortSupplier = () -> indexSettings.getIndexSortConfig().buildIndexSort(
                mapperService::fullName,
                indexFieldData::getForField
            );
        } else {
            this.indexSortSupplier = () -> null;
        }
        this.shardStoreDeleter = shardStoreDeleter;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.eventListener = eventListener;
        this.nodeEnv = nodeEnv;
        this.indexStore = indexStore;
        indexFieldData.setListener(new FieldDataCacheListener(this));
        this.bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetCacheListener(this));
        this.warmer = new IndexWarmer(indexSettings.getSettings(), threadPool, indexFieldData,
            bitsetFilterCache.createListener(threadPool));
        this.indexCache = new IndexCache(indexSettings, queryCache, bitsetFilterCache);
        this.engineFactory = Objects.requireNonNull(engineFactory);
        // initialize this last -- otherwise if the wrapper requires any other member to be non-null we fail with an NPE
        this.searcherWrapper = wrapperFactory.newWrapper(this);
        this.indexingOperationListeners = Collections.unmodifiableList(indexingOperationListeners);
        // kick off async ops for the first shard in this index
        this.refreshTask = new AsyncRefreshTask(this);
        this.trimTranslogTask = new AsyncTrimTranslogTask(this);
        this.globalCheckpointTask = new AsyncGlobalCheckpointTask(this);
        rescheduleFsyncTask(indexSettings.getTranslogDurability());
    }

    public int numberOfShards() {
        return shards.size();
    }

    public IndexEventListener getIndexEventListener() {
        return this.eventListener;
    }

    @Override
    public Iterator<IndexShard> iterator() {
        return shards.values().iterator();
    }

    public boolean hasShard(int shardId) {
        return shards.containsKey(shardId);
    }

    /**
     * Return the shard with the provided id, or null if there is no such shard.
     */
    @Override
    @Nullable
    public IndexShard getShardOrNull(int shardId) {
        return shards.get(shardId);
    }

    /**
     * Return the shard with the provided id, or throw an exception if it doesn't exist.
     */
    public IndexShard getShard(int shardId) {
        IndexShard indexShard = getShardOrNull(shardId);
        if (indexShard == null) {
            throw new ShardNotFoundException(new ShardId(index(), shardId));
        }
        return indexShard;
    }

    public Set<Integer> shardIds() {
        return shards.keySet();
    }

    public IndexCache cache() {
        return indexCache;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.mapperService.getIndexAnalyzers();
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public Supplier<Sort> getIndexSortSupplier() {
        return indexSortSupplier;
    }

    public synchronized void close(final String reason, boolean delete) throws IOException {
        if (closed.compareAndSet(false, true)) {
            deleted.compareAndSet(false, delete);
            try {
                final Set<Integer> shardIds = shardIds();
                for (final int shardId : shardIds) {
                    try {
                        removeShard(shardId, reason);
                    } catch (Exception e) {
                        logger.warn("failed to close shard", e);
                    }
                }
            } finally {
                IOUtils.close(
                        bitsetFilterCache,
                        indexCache,
                        indexFieldData,
                        mapperService,
                        refreshTask,
                        fsyncTask,
                        trimTranslogTask,
                        globalCheckpointTask);
            }
        }
    }


    public String indexUUID() {
        return indexSettings.getUUID();
    }

    // NOTE: O(numShards) cost, but numShards should be smallish?
    private long getAvgShardSizeInBytes() throws IOException {
        long sum = 0;
        int count = 0;
        for (IndexShard indexShard : this) {
            sum += indexShard.store().stats().sizeInBytes();
            count++;
        }
        if (count == 0) {
            return -1L;
        } else {
            return sum / count;
        }
    }

    public synchronized IndexShard createShard(ShardRouting routing, Consumer<ShardId> globalCheckpointSyncer) throws IOException {
        /*
         * TODO: we execute this in parallel but it's a synced method. Yet, we might
         * be able to serialize the execution via the cluster state in the future. for now we just
         * keep it synced.
         */
        if (closed.get()) {
            throw new IllegalStateException("Can't create shard " + routing.shardId() + ", closed");
        }
        final Settings indexSettings = this.indexSettings.getSettings();
        final ShardId shardId = routing.shardId();
        boolean success = false;
        Store store = null;
        IndexShard indexShard = null;
        ShardLock lock = null;
        try {
            lock = nodeEnv.shardLock(shardId, TimeUnit.SECONDS.toMillis(5));
            eventListener.beforeIndexShardCreated(shardId, indexSettings);
            ShardPath path;
            try {
                path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings);
            } catch (IllegalStateException ex) {
                logger.warn("{} failed to load shard path, trying to remove leftover", shardId);
                try {
                    ShardPath.deleteLeftoverShardDirectory(logger, nodeEnv, lock, this.indexSettings);
                    path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings);
                } catch (Exception inner) {
                    ex.addSuppressed(inner);
                    throw ex;
                }
            }

            if (path == null) {
                // TODO: we should, instead, hold a "bytes reserved" of how large we anticipate this shard will be, e.g. for a shard
                // that's being relocated/replicated we know how large it will become once it's done copying:
                // Count up how many shards are currently on each data path:
                Map<Path, Integer> dataPathToShardCount = new HashMap<>();
                for (IndexShard shard : this) {
                    Path dataPath = shard.shardPath().getRootStatePath();
                    Integer curCount = dataPathToShardCount.get(dataPath);
                    if (curCount == null) {
                        curCount = 0;
                    }
                    dataPathToShardCount.put(dataPath, curCount + 1);
                }
                path = ShardPath.selectNewPathForShard(nodeEnv, shardId, this.indexSettings,
                    routing.getExpectedShardSize() == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                        ? getAvgShardSizeInBytes() : routing.getExpectedShardSize(),
                    dataPathToShardCount);
                logger.debug("{} creating using a new path [{}]", shardId, path);
            } else {
                logger.debug("{} creating using an existing path [{}]", shardId, path);
            }

            if (shards.containsKey(shardId.id())) {
                throw new IllegalStateException(shardId + " already exists");
            }

            logger.debug("creating shard_id {}", shardId);
            // if we are on a shared FS we only own the shard (ie. we can safely delete it) if we are the primary.
            final Engine.Warmer engineWarmer = (searcher) -> {
                IndexShard shard =  getShardOrNull(shardId.getId());
                if (shard != null) {
                    warmer.warm(searcher, shard, IndexService.this.indexSettings);
                }
            };
            // TODO we can remove either IndexStore or DirectoryService. All we need is a simple Supplier<Directory>
            DirectoryService directoryService = indexStore.newDirectoryService(path);
            store = new Store(shardId, this.indexSettings, directoryService.newDirectory(), lock,
                    new StoreCloseListener(shardId, () -> eventListener.onStoreClosed(shardId)));
            indexShard = new IndexShard(routing, this.indexSettings, path, store, indexSortSupplier,
                indexCache, mapperService, engineFactory,
                eventListener, searcherWrapper, threadPool, bigArrays, engineWarmer,
                indexingOperationListeners, () -> globalCheckpointSyncer.accept(shardId),
                circuitBreakerService);
            eventListener.indexShardStateChanged(indexShard, null, indexShard.state(), "shard created");
            eventListener.afterIndexShardCreated(indexShard);
            shards = newMapBuilder(shards).put(shardId.id(), indexShard).immutableMap();
            success = true;
            return indexShard;
        } catch (ShardLockObtainFailedException e) {
            throw new IOException("failed to obtain in-memory shard lock", e);
        } finally {
            if (success == false) {
                if (lock != null) {
                    IOUtils.closeWhileHandlingException(lock);
                }
                closeShard("initialization failed", shardId, indexShard, store, eventListener);
            }
        }
    }

    @Override
    public synchronized void removeShard(int shardId, String reason) {
        final ShardId sId = new ShardId(index(), shardId);
        final IndexShard indexShard;
        if (shards.containsKey(shardId) == false) {
            return;
        }
        logger.debug("[{}] closing... (reason: [{}])", shardId, reason);
        HashMap<Integer, IndexShard> newShards = new HashMap<>(shards);
        indexShard = newShards.remove(shardId);
        shards = unmodifiableMap(newShards);
        closeShard(reason, sId, indexShard, indexShard.store(), indexShard.getIndexEventListener());
        logger.debug("[{}] closed (reason: [{}])", shardId, reason);
    }

    private void closeShard(String reason, ShardId sId, IndexShard indexShard, Store store, IndexEventListener listener) {
        final int shardId = sId.id();
        final Settings indexSettings = this.getIndexSettings().getSettings();
        try {
            try {
                listener.beforeIndexShardClosed(sId, indexShard, indexSettings);
            } finally {
                // this logic is tricky, we want to close the engine so we rollback the changes done to it
                // and close the shard so no operations are allowed to it
                if (indexShard != null) {
                    try {
                        // only flush we are we closed (closed index or shutdown) and if we are not deleted
                        final boolean flushEngine = deleted.get() == false && closed.get();
                        indexShard.close(reason, flushEngine);
                    } catch (Exception e) {
                        logger.debug(() -> new ParameterizedMessage("[{}] failed to close index shard", shardId), e);
                        // ignore
                    }
                }
                // call this before we close the store, so we can release resources for it
                listener.afterIndexShardClosed(sId, indexShard, indexSettings);
            }
        } finally {
            try {
                if (store != null) {
                    store.close();
                } else {
                    logger.trace("[{}] store not initialized prior to closing shard, nothing to close", shardId);
                }
            } catch (Exception e) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "[{}] failed to close store on shard removal (reason: [{}])", shardId, reason), e);
            }
        }
    }


    private void onShardClose(ShardLock lock) {
        if (deleted.get()) { // we remove that shards content if this index has been deleted
            try {
                try {
                    eventListener.beforeIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                } finally {
                    shardStoreDeleter.deleteShardStore("delete index", lock, indexSettings);
                    eventListener.afterIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                }
            } catch (IOException e) {
                shardStoreDeleter.addPendingDelete(lock.getShardId(), indexSettings);
                logger.debug(
                    () -> new ParameterizedMessage(
                        "[{}] failed to delete shard content - scheduled a retry", lock.getShardId().id()), e);
            }
        }
    }

    @Override
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Creates a new QueryShardContext. The context has not types set yet, if types are required set them via
     * {@link QueryShardContext#setTypes(String...)}.
     *
     * Passing a {@code null} {@link IndexReader} will return a valid context, however it won't be able to make
     * {@link IndexReader}-specific optimizations, such as rewriting containing range queries.
     */
    public QueryShardContext newQueryShardContext(int shardId, IndexReader indexReader, LongSupplier nowInMillis, String clusterAlias) {
        return new QueryShardContext(
            shardId,
            indexSettings,
            indexCache.bitsetFilterCache(),
            indexFieldData::getForField,
            mapperService(),
            xContentRegistry,
            namedWriteableRegistry,
            indexReader,
            nowInMillis,
            clusterAlias
        );
    }

    /**
     * The {@link ThreadPool} to use for this index.
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    /**
     * The {@link BigArrays} to use for this index.
     */
    public BigArrays getBigArrays() {
        return bigArrays;
    }

    List<IndexingOperationListener> getIndexOperationListeners() { // pkg private for testing
        return indexingOperationListeners;
    }

    @Override
    public boolean updateMapping(final IndexMetaData currentIndexMetaData, final IndexMetaData newIndexMetaData) throws IOException {
        return mapperService().updateMapping(currentIndexMetaData, newIndexMetaData);
    }

    public IndexFieldDataService fieldData() {
        return indexFieldData;
    }

    private class StoreCloseListener implements Store.OnClose {
        private final ShardId shardId;
        private final Closeable[] toClose;

        StoreCloseListener(ShardId shardId, Closeable... toClose) {
            this.shardId = shardId;
            this.toClose = toClose;
        }

        @Override
        public void accept(ShardLock lock) {
            try {
                assert lock.getShardId().equals(shardId) : "shard id mismatch, expected: " + shardId + " but got: " + lock.getShardId();
                onShardClose(lock);
            } finally {
                try {
                    IOUtils.close(toClose);
                } catch (IOException ex) {
                    logger.debug("failed to close resource", ex);
                }
            }

        }
    }

    private static final class BitsetCacheListener implements BitsetFilterCache.Listener {
        final IndexService indexService;

        private BitsetCacheListener(IndexService indexService) {
            this.indexService = indexService;
        }

        @Override
        public void onCache(ShardId shardId, Accountable accountable) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0L;
                    shard.shardBitsetFilterCache().onCached(ramBytesUsed);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, Accountable accountable) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0L;
                    shard.shardBitsetFilterCache().onRemoval(ramBytesUsed);
                }
            }
        }
    }

    private final class FieldDataCacheListener implements IndexFieldDataCache.Listener {
        final IndexService indexService;

        FieldDataCacheListener(IndexService indexService) {
            this.indexService = indexService;
        }

        @Override
        public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onCache(shardId, fieldName, ramUsage);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onRemoval(shardId, fieldName, wasEvicted, sizeInBytes);
                }
            }
        }
    }

    public IndexMetaData getMetaData() {
        return indexSettings.getIndexMetaData();
    }

    @Override
    public synchronized void updateMetaData(final IndexMetaData currentIndexMetaData, final IndexMetaData newIndexMetaData) {
        final Translog.Durability oldTranslogDurability = indexSettings.getTranslogDurability();

        final boolean updateIndexMetaData = indexSettings.updateIndexMetaData(newIndexMetaData);

        if (Assertions.ENABLED
                && currentIndexMetaData != null
                && currentIndexMetaData.getCreationVersion().onOrAfter(Version.ES_V_6_5_1)) {
            final long currentSettingsVersion = currentIndexMetaData.getSettingsVersion();
            final long newSettingsVersion = newIndexMetaData.getSettingsVersion();
            if (currentSettingsVersion == newSettingsVersion) {
                assert updateIndexMetaData == false;
            } else {
                assert updateIndexMetaData;
                assert currentSettingsVersion < newSettingsVersion :
                        "expected current settings version [" + currentSettingsVersion + "] "
                                + "to be less than new settings version [" + newSettingsVersion + "]";
            }
        }

        if (updateIndexMetaData) {
            for (final IndexShard shard : this.shards.values()) {
                try {
                    shard.onSettingsChanged();
                } catch (Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "[{}] failed to notify shard about setting change", shard.shardId().id()), e);
                }
            }
            if (refreshTask.getInterval().equals(indexSettings.getRefreshInterval()) == false) {
                rescheduleRefreshTasks();
            }
            final Translog.Durability durability = indexSettings.getTranslogDurability();
            if (durability != oldTranslogDurability) {
                rescheduleFsyncTask(durability);
            }
        }
    }

    private void rescheduleFsyncTask(Translog.Durability durability) {
        try {
            if (fsyncTask != null) {
                fsyncTask.close();
            }
        } finally {
            fsyncTask = durability == Translog.Durability.REQUEST ? null : new AsyncTranslogFSync(this);
        }
    }

    private void rescheduleRefreshTasks() {
        try {
            refreshTask.close();
        } finally {
            refreshTask = new AsyncRefreshTask(this);
        }
    }

    public interface ShardStoreDeleter {
        void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException;

        void addPendingDelete(ShardId shardId, IndexSettings indexSettings);
    }

    public final EngineFactory getEngineFactory() {
        return engineFactory;
    }

    final IndexSearcherWrapper getSearcherWrapper() {
        return searcherWrapper;
    } // pkg private for testing

    final IndexStore getIndexStore() {
        return indexStore;
    } // pkg private for testing

    private void maybeFSyncTranslogs() {
        if (indexSettings.getTranslogDurability() == Translog.Durability.ASYNC) {
            for (IndexShard shard : this.shards.values()) {
                try {
                    if (shard.isSyncNeeded()) {
                        shard.sync();
                    }
                } catch (AlreadyClosedException ex) {
                    // fine - continue;
                } catch (IOException e) {
                    logger.warn("failed to sync translog", e);
                }
            }
        }
    }

    private void maybeRefreshEngine() {
        if (indexSettings.getRefreshInterval().millis() > 0) {
            for (IndexShard shard : this.shards.values()) {
                if (shard.isReadAllowed()) {
                    try {
                        if (shard.isRefreshNeeded()) {
                            shard.refresh("schedule");
                        }
                    } catch (IndexShardClosedException | AlreadyClosedException ex) {
                        // fine - continue;
                    }
                }
            }
        }
    }

    private void maybeTrimTranslog() {
        for (IndexShard shard : this.shards.values()) {
            switch (shard.state()) {
                case CREATED:
                case RECOVERING:
                case CLOSED:
                    continue;
                case POST_RECOVERY:
                case STARTED:
                    try {
                        shard.trimTranslog();
                    } catch (IndexShardClosedException | AlreadyClosedException ex) {
                        // fine - continue;
                    }
                    continue;
                default:
                    throw new IllegalStateException("unknown state: " + shard.state());
            }
        }
    }

    private void maybeSyncGlobalCheckpoints() {
        for (final IndexShard shard : this.shards.values()) {
            if (shard.routingEntry().active() && shard.routingEntry().primary()) {
                switch (shard.state()) {
                    case CLOSED:
                    case CREATED:
                    case RECOVERING:
                        continue;
                    case POST_RECOVERY:
                        assert false : "shard " + shard.shardId() + " is in post-recovery but marked as active";
                        continue;
                    case STARTED:
                        try {
                            shard.acquirePrimaryOperationPermit(
                                    ActionListener.wrap(
                                            releasable -> {
                                                try (Releasable ignored = releasable) {
                                                        shard.maybeSyncGlobalCheckpoint("background");
                                                }
                                            },
                                            e -> {
                                                if (!(e instanceof AlreadyClosedException || e instanceof IndexShardClosedException)) {
                                                    logger.info(
                                                            new ParameterizedMessage(
                                                                    "{} failed to execute background global checkpoint sync",
                                                                    shard.shardId()),
                                                            e);
                                                }
                                            }),
                                    ThreadPool.Names.SAME, "background global checkpoint sync");
                        } catch (final AlreadyClosedException | IndexShardClosedException e) {
                            // the shard was closed concurrently, continue
                        }
                        continue;
                    default:
                        throw new IllegalStateException("unknown state [" + shard.state() + "]");
                }
            }
        }
    }

    abstract static class BaseAsyncTask implements Runnable, Closeable {
        protected final IndexService indexService;
        protected final ThreadPool threadPool;
        private final TimeValue interval;
        private ScheduledFuture<?> scheduledFuture;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private volatile Exception lastThrownException;

        BaseAsyncTask(IndexService indexService, TimeValue interval) {
            this.indexService = indexService;
            this.threadPool = indexService.getThreadPool();
            this.interval = interval;
            onTaskCompletion();
        }

        boolean mustReschedule() {
            // don't re-schedule if its closed or if we don't have a single shard here..., we are done
            return indexService.closed.get() == false
                && closed.get() == false && interval.millis() > 0;
        }

        private synchronized void onTaskCompletion() {
            if (mustReschedule()) {
                if (indexService.logger.isTraceEnabled()) {
                    indexService.logger.trace("scheduling {} every {}", toString(), interval);
                }
                this.scheduledFuture = threadPool.schedule(interval, getThreadPool(), BaseAsyncTask.this);
            } else {
                indexService.logger.trace("scheduled {} disabled", toString());
                this.scheduledFuture = null;
            }
        }

        boolean isScheduled() {
            return scheduledFuture != null;
        }

        @Override
        public final void run() {
            try {
                runInternal();
            } catch (Exception ex) {
                if (lastThrownException == null || sameException(lastThrownException, ex) == false) {
                    // prevent the annoying fact of logging the same stuff all the time with an interval of 1 sec will spam all your logs
                    indexService.logger.warn(
                        () -> new ParameterizedMessage(
                            "failed to run task {} - suppressing re-occurring exceptions unless the exception changes",
                            toString()),
                        ex);
                    lastThrownException = ex;
                }
            } finally {
                onTaskCompletion();
            }
        }

        private static boolean sameException(Exception left, Exception right) {
            if (left.getClass() == right.getClass()) {
                if (Objects.equals(left.getMessage(), right.getMessage())) {
                    StackTraceElement[] stackTraceLeft = left.getStackTrace();
                    StackTraceElement[] stackTraceRight = right.getStackTrace();
                    if (stackTraceLeft.length == stackTraceRight.length) {
                        for (int i = 0; i < stackTraceLeft.length; i++) {
                            if (stackTraceLeft[i].equals(stackTraceRight[i]) == false) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        protected abstract void runInternal();

        protected String getThreadPool() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public synchronized void close() {
            if (closed.compareAndSet(false, true)) {
                FutureUtils.cancel(scheduledFuture);
                scheduledFuture = null;
            }
        }

        TimeValue getInterval() {
            return interval;
        }

        boolean isClosed() {
            return this.closed.get();
        }
    }

    /**
     * FSyncs the translog for all shards of this index in a defined interval.
     */
    static final class AsyncTranslogFSync extends BaseAsyncTask {

        AsyncTranslogFSync(IndexService indexService) {
            super(indexService, indexService.getIndexSettings().getTranslogSyncInterval());
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.FLUSH;
        }

        @Override
        protected void runInternal() {
            indexService.maybeFSyncTranslogs();
        }

        @Override
        public String toString() {
            return "translog_sync";
        }
    }

    final class AsyncRefreshTask extends BaseAsyncTask {

        AsyncRefreshTask(IndexService indexService) {
            super(indexService, indexService.getIndexSettings().getRefreshInterval());
        }

        @Override
        protected void runInternal() {
            indexService.maybeRefreshEngine();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.REFRESH;
        }

        @Override
        public String toString() {
            return "refresh";
        }
    }

    final class AsyncTrimTranslogTask extends BaseAsyncTask {

        AsyncTrimTranslogTask(IndexService indexService) {
            super(indexService, indexService.getIndexSettings()
                .getSettings().getAsTime(INDEX_TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING, TimeValue.timeValueMinutes(10)));
        }

        @Override
        protected void runInternal() {
            indexService.maybeTrimTranslog();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "trim_translog";
        }
    }

    // this setting is intentionally not registered, it is only used in tests
    public static final Setting<TimeValue> GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING =
            Setting.timeSetting(
                    "index.global_checkpoint_sync.interval",
                    new TimeValue(30, TimeUnit.SECONDS),
                    new TimeValue(0, TimeUnit.MILLISECONDS),
                    Property.Dynamic,
                    Property.IndexScope);

    /**
     * Background task that syncs the global checkpoint to replicas.
     */
    final class AsyncGlobalCheckpointTask extends BaseAsyncTask {

        AsyncGlobalCheckpointTask(final IndexService indexService) {
            // index.global_checkpoint_sync_interval is not a real setting, it is only registered in tests
            super(indexService, GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.get(indexService.getIndexSettings().getSettings()));
        }

        @Override
        protected void runInternal() {
            indexService.maybeSyncGlobalCheckpoints();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "global_checkpoint_sync";
        }
    }

    AsyncRefreshTask getRefreshTask() { // for tests
        return refreshTask;
    }

    AsyncTranslogFSync getFsyncTask() { // for tests
        return fsyncTask;
    }

    AsyncGlobalCheckpointTask getGlobalCheckpointTask() {
        return globalCheckpointTask;
    }

    /**
     * Clears the caches for the given shard id if the shard is still allocated on this node
     */
    public boolean clearCaches(boolean queryCache, boolean fieldDataCache, String...fields) {
        boolean clearedAtLeastOne = false;
        if (queryCache) {
            clearedAtLeastOne = true;
            indexCache.query().clear("api");
        }
        if (fieldDataCache) {
            clearedAtLeastOne = true;
            if (fields.length == 0) {
                indexFieldData.clear();
            } else {
                for (String field : fields) {
                    indexFieldData.clearField(field);
                }
            }
        }
        if (clearedAtLeastOne == false) {
            if (fields.length ==  0) {
                indexCache.clear("api");
                indexFieldData.clear();
            } else {
                // only clear caches relating to the specified fields
                for (String field : fields) {
                    indexFieldData.clearField(field);
                }
            }
        }
        return clearedAtLeastOne;
    }

}
