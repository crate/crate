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

import static io.crate.common.collections.MapBuilder.newMapBuilder;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Assertions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.gateway.WriteStateException;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;

import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;

public class IndexService extends AbstractIndexComponent implements IndicesClusterStateService.AllocatedIndex<IndexShard> {

    private final IndexEventListener eventListener;
    private final NodeEnvironment nodeEnv;
    private final ShardStoreDeleter shardStoreDeleter;
    private final QueryCache queryCache;
    private final IndexStorePlugin.DirectoryFactory directoryFactory;
    private final MapperService mapperService;
    private final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders;
    private volatile Map<Integer, IndexShard> shards = emptyMap();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean deleted = new AtomicBoolean(false);
    private final IndexSettings indexSettings;
    private final List<IndexingOperationListener> indexingOperationListeners;
    private volatile AsyncRefreshTask refreshTask;
    private volatile AsyncTranslogFSync fsyncTask;
    private volatile AsyncGlobalCheckpointTask globalCheckpointTask;
    private volatile AsyncRetentionLeaseSyncTask retentionLeaseSyncTask;

    // don't convert to Setting<> and register... we only set this in tests and register via a plugin
    public static final String INDEX_TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING = "index.translog.retention.check_interval";

    private final AsyncTrimTranslogTask trimTranslogTask;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private final CircuitBreakerService circuitBreakerService;

    public IndexService(
            IndexSettings indexSettings,
            IndexCreationContext indexCreationContext,
            NodeEnvironment nodeEnv,
            NamedXContentRegistry xContentRegistry,
            ShardStoreDeleter shardStoreDeleter,
            AnalysisRegistry registry,
            Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders,
            CircuitBreakerService circuitBreakerService,
            BigArrays bigArrays,
            ThreadPool threadPool,
            QueryCache queryCache,
            IndexStorePlugin.DirectoryFactory directoryFactory,
            IndexEventListener eventListener,
            MapperRegistry mapperRegistry,
            List<IndexingOperationListener> indexingOperationListeners) throws IOException {
        super(indexSettings);
        this.indexSettings = indexSettings;
        this.circuitBreakerService = circuitBreakerService;
        if (indexSettings.getIndexMetadata().getState() == IndexMetadata.State.CLOSE &&
                indexCreationContext == IndexCreationContext.CREATE_INDEX) { // metadata verification needs a mapper service
            this.mapperService = null;
            this.queryCache = null;
        } else {
            this.mapperService = new MapperService(
                indexSettings,
                registry.build(indexSettings),
                xContentRegistry,
                mapperRegistry
            );
            this.queryCache = queryCache;
        }
        this.shardStoreDeleter = shardStoreDeleter;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.eventListener = eventListener;
        this.nodeEnv = nodeEnv;
        this.directoryFactory = directoryFactory;
        this.engineFactoryProviders = engineFactoryProviders;
        this.indexingOperationListeners = Collections.unmodifiableList(indexingOperationListeners);
        // kick off async ops for the first shard in this index
        this.refreshTask = new AsyncRefreshTask(this);
        this.trimTranslogTask = new AsyncTrimTranslogTask(this);
        this.globalCheckpointTask = new AsyncGlobalCheckpointTask(this);
        this.retentionLeaseSyncTask = new AsyncRetentionLeaseSyncTask(this);
        updateFsyncTaskIfNecessary();
    }

    public enum IndexCreationContext {
        CREATE_INDEX,
        METADATA_VERIFICATION
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

    @Nullable
    public QueryCache cache() {
        return queryCache;
    }

    public MapperService mapperService() {
        return mapperService;
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
                    mapperService,
                    refreshTask,
                    fsyncTask,
                    trimTranslogTask,
                    globalCheckpointTask,
                    retentionLeaseSyncTask);
            }
        }
    }

    // method is synchronized so that IndexService can't be closed while we're writing out dangling indices information
    public synchronized void writeDanglingIndicesInfo() {
        if (closed.get()) {
            return;
        }
        try {
            IndexMetadata.FORMAT.writeAndCleanup(getMetadata(), nodeEnv.indexPaths(index()));
        } catch (WriteStateException e) {
            logger.warn(() -> new ParameterizedMessage("failed to write dangling indices state for index {}", index()), e);
        }
    }

    // method is synchronized so that IndexService can't be closed while we're deleting dangling indices information
    public synchronized void deleteDanglingIndicesInfo() {
        if (closed.get()) {
            return;
        }
        try {
            MetadataStateFormat.deleteMetaState(nodeEnv.indexPaths(index()));
        } catch (IOException e) {
            logger.warn(() -> new ParameterizedMessage("failed to delete dangling indices state for index {}", index()), e);
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
            sum += indexShard.store().stats(0L).sizeInBytes();
            count++;
        }
        if (count == 0) {
            return -1L;
        } else {
            return sum / count;
        }
    }

    public synchronized IndexShard createShard(
            final ShardRouting routing,
            final Consumer<ShardId> globalCheckpointSyncer,
            final RetentionLeaseSyncer retentionLeaseSyncer) throws IOException {
        Objects.requireNonNull(retentionLeaseSyncer);
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
            lock = nodeEnv.shardLock(shardId, "starting shard");
            eventListener.beforeIndexShardCreated(shardId, indexSettings);
            ShardPath path;
            try {
                path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings.customDataPath());
            } catch (IllegalStateException ex) {
                logger.warn("{} failed to load shard path, trying to remove leftover", shardId);
                try {
                    ShardPath.deleteLeftoverShardDirectory(logger, nodeEnv, lock, this.indexSettings);
                    path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings.customDataPath());
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
            Directory directory = directoryFactory.newDirectory(this.indexSettings, path);
            store = new Store(
                shardId,
                this.indexSettings,
                directory,
                lock,
                new StoreCloseListener(shardId, () -> eventListener.onStoreClosed(shardId))
            );
            eventListener.onStoreCreated(shardId);
            indexShard = new IndexShard(
                routing,
                this.indexSettings,
                path,
                store,
                queryCache,
                mapperService,
                engineFactoryProviders,
                eventListener,
                threadPool,
                bigArrays,
                indexingOperationListeners,
                () -> globalCheckpointSyncer.accept(shardId),
                retentionLeaseSyncer,
                circuitBreakerService
            );
            eventListener.indexShardStateChanged(indexShard, null, indexShard.state(), "shard created");
            eventListener.afterIndexShardCreated(indexShard);
            shards = newMapBuilder(shards).put(shardId.id(), indexShard).immutableMap();
            success = true;
            return indexShard;
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
        if (store != null) {
            store.beforeClose();
        }
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
     * Creates a new QueryShardContext
     */
    public QueryShardContext newQueryShardContext() {
        return new QueryShardContext(indexSettings, mapperService());
    }

    @Override
    public boolean updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
        if (mapperService == null) {
            return false;
        }
        return mapperService.updateMapping(currentIndexMetadata, newIndexMetadata);
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

    public IndexMetadata getMetadata() {
        return indexSettings.getIndexMetadata();
    }

    private final CopyOnWriteArrayList<Consumer<IndexMetadata>> metadataListeners = new CopyOnWriteArrayList<>();

    public void addMetadataListener(Consumer<IndexMetadata> listener) {
        metadataListeners.add(listener);
    }

    @Override
    public synchronized void updateMetadata(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) {
        final boolean updateIndexSettings = indexSettings.updateIndexMetadata(newIndexMetadata);

        if (Assertions.ENABLED && currentIndexMetadata != null) {
            final long currentSettingsVersion = currentIndexMetadata.getSettingsVersion();
            final long newSettingsVersion = newIndexMetadata.getSettingsVersion();
            if (currentSettingsVersion == newSettingsVersion) {
                assert updateIndexSettings == false;
            } else {
                assert updateIndexSettings;
                assert currentSettingsVersion < newSettingsVersion :
                        "expected current settings version [" + currentSettingsVersion + "] "
                                + "to be less than new settings version [" + newSettingsVersion + "]";
            }
        }

        if (updateIndexSettings) {
            for (final IndexShard shard : this.shards.values()) {
                try {
                    shard.onSettingsChanged(currentIndexMetadata.getSettings());
                } catch (Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "[{}] failed to notify shard about setting change", shard.shardId().id()), e);
                }
            }
            if (refreshTask.getInterval().equals(indexSettings.getRefreshInterval()) == false) {
                // once we change the refresh interval we schedule yet another refresh
                // to ensure we are in a clean and predictable state.
                // it doesn't matter if we move from or to <code>-1</code>  in both cases we want
                // docs to become visible immediately. This also flushes all pending indexing / search requests
                // that are waiting for a refresh.
                threadPool.executor(ThreadPool.Names.REFRESH).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("forced refresh failed after interval change", e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        maybeRefreshEngine(true);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return true;
                    }
                });
                rescheduleRefreshTasks();
            }
            updateFsyncTaskIfNecessary();
        }

        metadataListeners.forEach(c -> c.accept(newIndexMetadata));
    }

    private void updateFsyncTaskIfNecessary() {
        if (indexSettings.getTranslogDurability() == Translog.Durability.REQUEST) {
            try {
                if (fsyncTask != null) {
                    fsyncTask.close();
                }
            } finally {
                fsyncTask = null;
            }
        } else if (fsyncTask == null) {
            fsyncTask = new AsyncTranslogFSync(this);
        } else {
            fsyncTask.updateIfNeeded();
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

    private void maybeRefreshEngine(boolean force) {
        if (indexSettings.getRefreshInterval().millis() > 0 || force) {
            for (IndexShard shard : this.shards.values()) {
                try {
                    shard.scheduledRefresh();
                } catch (IndexShardClosedException | AlreadyClosedException ex) {
                    // fine - continue;
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
        sync(is -> is.maybeSyncGlobalCheckpoint("background"), "global checkpoint");
    }

    private void syncRetentionLeases() {
        sync(IndexShard::syncRetentionLeases, "retention lease");
    }

    private void sync(final Consumer<IndexShard> sync, final String source) {
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
                            shard.runUnderPrimaryPermit(
                                () -> sync.accept(shard),
                                e -> {
                                    if (e instanceof AlreadyClosedException == false
                                            && e instanceof IndexShardClosedException == false) {
                                        logger.warn(new ParameterizedMessage(
                                            "{} failed to execute {} sync", shard.shardId(), source), e);
                                    }
                                },
                                ThreadPool.Names.SAME,
                                source + " sync"
                            );
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

    abstract static class BaseAsyncTask extends AbstractAsyncTask {

        protected final IndexService indexService;

        BaseAsyncTask(final IndexService indexService, final TimeValue interval) {
            super(indexService.logger, indexService.threadPool, interval, true);
            this.indexService = indexService;
            rescheduleIfNecessary();
        }

        @Override
        protected boolean mustReschedule() {
            // don't re-schedule if the IndexService instance is closed or if the index is closed
            return indexService.closed.get() == false
                && indexService.indexSettings.getIndexMetadata().getState() == IndexMetadata.State.OPEN;
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

        void updateIfNeeded() {
            final TimeValue newInterval = indexService.getIndexSettings().getTranslogSyncInterval();
            if (newInterval.equals(getInterval()) == false) {
                setInterval(newInterval);
            }
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
            indexService.maybeRefreshEngine(false);
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
        protected boolean mustReschedule() {
            return indexService.closed.get() == false;
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
                    Property.IndexScope,
                    Property.ReplicatedIndexScope);

    // this setting is intentionally not registered, it is only used in tests
    public static final Setting<TimeValue> RETENTION_LEASE_SYNC_INTERVAL_SETTING =
            Setting.timeSetting(
                    "index.soft_deletes.retention_lease.sync_interval",
                    new TimeValue(5, TimeUnit.MINUTES),
                    new TimeValue(0, TimeUnit.MILLISECONDS),
                    Property.Dynamic,
                    Property.IndexScope,
                    Property.ReplicatedIndexScope);

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

    final class AsyncRetentionLeaseSyncTask extends BaseAsyncTask {

        AsyncRetentionLeaseSyncTask(final IndexService indexService) {
            super(indexService, RETENTION_LEASE_SYNC_INTERVAL_SETTING.get(indexService.getIndexSettings().getSettings()));
        }

        @Override
        protected void runInternal() {
            indexService.syncRetentionLeases();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        public String toString() {
            return "retention_lease_sync";
        }

    }

    AsyncRefreshTask getRefreshTask() { // for tests
        return refreshTask;
    }

    AsyncTranslogFSync getFsyncTask() { // for tests
        return fsyncTask;
    }

    AsyncTrimTranslogTask getTrimTranslogTask() { // for tests
        return trimTranslogTask;
    }
}
