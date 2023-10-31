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

import static io.crate.common.collections.MapBuilder.newMapBuilder;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexReader.CacheHelper;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterApplier.ClusterApplyListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;

import io.crate.action.LimitedExponentialBackoff;
import io.crate.common.collections.Iterables;
import io.crate.common.collections.Sets;
import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;

public class IndicesService extends AbstractLifecycleComponent
    implements IndicesClusterStateService.AllocatedIndices<IndexShard, IndexService>, IndexService.ShardStoreDeleter {

    private static final Logger LOGGER = LogManager.getLogger(IndicesService.class);

    public static final String INDICES_SHARDS_CLOSED_TIMEOUT = "indices.shards_closed_timeout";

    public static final Setting<Boolean> WRITE_DANGLING_INDICES_INFO_SETTING = Setting.boolSetting(
        "gateway.write_dangling_indices_info",
        true,
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final PluginsService pluginsService;
    private final NodeEnvironment nodeEnv;
    private final NamedXContentRegistry xContentRegistry;
    private final TimeValue shardsClosedTimeout;
    private final AnalysisRegistry analysisRegistry;
    private final IndexScopedSettings indexScopedSettings;
    private final ThreadPool threadPool;
    private final CircuitBreakerService circuitBreakerService;
    private final BigArrays bigArrays;
    private final Client client;
    private final Settings settings;
    private volatile Map<String, IndexService> indices = emptyMap();
    private final Map<Index, List<PendingDelete>> pendingDeletes = new HashMap<>();
    private final AtomicInteger numUncompletedDeletes = new AtomicInteger();
    private final MapperRegistry mapperRegistry;
    private final IndexingMemoryController indexingMemoryController;
    private final QueryCache indicesQueryCache;
    private final MetaStateService metaStateService;
    private final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders;
    private final Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories;
    final AbstractRefCounted indicesRefCount; // pkg-private for testing
    private final CountDownLatch closeLatch = new CountDownLatch(1);

    @Nullable
    private final EsThreadPoolExecutor danglingIndicesThreadPoolExecutor;
    private final Set<Index> danglingIndicesToWrite = Sets.newConcurrentHashSet();
    private final boolean nodeWriteDanglingIndicesInfo;

    private final Map<ShardId, CompletableFuture<IndexShard>> pendingShardCreations = new ConcurrentHashMap<>();


    @Override
    protected void doStart() {


    }

    public IndicesService(Settings settings,
                          ClusterService clusterService,
                          PluginsService pluginsService,
                          NodeEnvironment nodeEnv,
                          NamedXContentRegistry xContentRegistry,
                          AnalysisRegistry analysisRegistry,
                          MapperRegistry mapperRegistry,
                          NamedWriteableRegistry namedWriteableRegistry,
                          ThreadPool threadPool,
                          IndexScopedSettings indexScopedSettings,
                          CircuitBreakerService circuitBreakerService,
                          BigArrays bigArrays,
                          Client client,
                          MetaStateService metaStateService,
                          Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders,
                          Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.pluginsService = pluginsService;
        this.nodeEnv = nodeEnv;
        this.xContentRegistry = xContentRegistry;
        this.shardsClosedTimeout = settings.getAsTime(INDICES_SHARDS_CLOSED_TIMEOUT, new TimeValue(1, TimeUnit.DAYS));
        this.analysisRegistry = analysisRegistry;
        this.indicesQueryCache = IndicesQueryCache.createCache(settings);
        this.mapperRegistry = mapperRegistry;
        indexingMemoryController = new IndexingMemoryController(
            settings,
            threadPool,
            // ensure we pull an iter with new shards - flatten makes a copy
            () -> Iterables.concat(this).iterator()
        );
        this.indexScopedSettings = indexScopedSettings;
        this.circuitBreakerService = circuitBreakerService;
        this.bigArrays = bigArrays;
        this.client = client;
        this.metaStateService = metaStateService;
        this.engineFactoryProviders = engineFactoryProviders;

        // do not allow any plugin-provided index store type to conflict with a built-in type
        for (final String indexStoreType : directoryFactories.keySet()) {
            if (IndexModule.isBuiltinType(indexStoreType)) {
                throw new IllegalStateException("registered index store type [" + indexStoreType + "] conflicts with a built-in type");
            }
        }

        this.directoryFactories = directoryFactories;
        // doClose() is called when shutting down a node, yet there might still be ongoing requests
        // that we need to wait for before closing some resources such as the caches. In order to
        // avoid closing these resources while ongoing requests are still being processed, we use a
        // ref count which will only close them when both this service and all index services are
        // actually closed
        indicesRefCount = new AbstractRefCounted("indices") {
            @Override
            protected void closeInternal() {
                try {
                    IOUtils.close(analysisRegistry, indexingMemoryController);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    closeLatch.countDown();
                }
            }
        };

        final String nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(settings));
        nodeWriteDanglingIndicesInfo = WRITE_DANGLING_INDICES_INFO_SETTING.get(settings);
        danglingIndicesThreadPoolExecutor = nodeWriteDanglingIndicesInfo ? EsExecutors.newScaling(
            nodeName + "/" + DANGLING_INDICES_UPDATE_THREAD_NAME,
            1, 1,
            0, TimeUnit.MILLISECONDS,
            daemonThreadFactory(nodeName, DANGLING_INDICES_UPDATE_THREAD_NAME)) : null;
    }

    private static final String DANGLING_INDICES_UPDATE_THREAD_NAME = "DanglingIndices#updateTask";

    @Override
    protected void doStop() {
        ThreadPool.terminate(danglingIndicesThreadPoolExecutor, 10, TimeUnit.SECONDS);

        ExecutorService indicesStopExecutor = Executors.newFixedThreadPool(5, daemonThreadFactory(settings, "indices_shutdown"));

        // Copy indices because we modify it asynchronously in the body of the loop
        final Set<Index> indices = this.indices.values().stream().map(AbstractIndexComponent::index).collect(Collectors.toSet());
        final CountDownLatch latch = new CountDownLatch(indices.size());
        for (final Index index : indices) {
            indicesStopExecutor.execute(() -> {
                try {
                    removeIndex(index, IndexRemovalReason.NO_LONGER_ASSIGNED, "shutdown");
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            if (latch.await(shardsClosedTimeout.seconds(), TimeUnit.SECONDS) == false) {
                LOGGER.warn("Not all shards are closed yet, waited {}sec - stopping service", shardsClosedTimeout.seconds());
            }
        } catch (InterruptedException e) {
            // ignore
        } finally {
            indicesStopExecutor.shutdown();
        }
    }

    @Override
    protected void doClose() {
        indicesRefCount.decRef();
    }

    /**
     * Wait for this {@link IndicesService} to be effectively closed. When this returns {@code true}, all shards and shard stores
     * are closed and all shard {@link CacheHelper#addClosedListener(org.apache.lucene.index.IndexReader.ClosedListener) closed
     * listeners} have run. However some {@link IndexEventListener#onStoreClosed(ShardId) shard closed listeners} might not have
     * run.
     * @returns true if all shards closed within the given timeout, false otherwise
     * @throws InterruptedException if the current thread got interrupted while waiting for shards to close
     */
    public boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return closeLatch.await(timeout, timeUnit);
    }

    /**
     * Checks if changes (adding / removing) indices, shards and so on are allowed.
     *
     * @throws IllegalStateException if no changes allowed.
     */
    private void ensureChangesAllowed() {
        if (lifecycle.started() == false) {
            throw new IllegalStateException("Can't make changes to indices service, node is closed");
        }
    }

    @Override
    public Iterator<IndexService> iterator() {
        return indices.values().iterator();
    }

    public boolean hasIndex(Index index) {
        return indices.containsKey(index.getUUID());
    }

    /**
     * Returns an IndexService for the specified index if exists otherwise returns <code>null</code>.
     */
    @Override
    @Nullable
    public IndexService indexService(Index index) {
        return indices.get(index.getUUID());
    }

    /**
     * Returns an IndexService for the specified index if exists otherwise a {@link IndexNotFoundException} is thrown.
     */
    public IndexService indexServiceSafe(Index index) {
        IndexService indexService = indices.get(index.getUUID());
        if (indexService == null) {
            throw new IndexNotFoundException(index);
        }
        assert indexService.indexUUID().equals(index.getUUID()) : "uuid mismatch local: " + indexService.indexUUID() + " incoming: " + index.getUUID();
        return indexService;
    }

    /**
     * Creates a new {@link IndexService} for the given metadata.
     *
     * @param indexMetadata          the index metadata to create the index for
     * @param builtInListeners       a list of built-in lifecycle {@link IndexEventListener} that should should be used along side with the
     *                               per-index listeners
     * @throws ResourceAlreadyExistsException if the index already exists.
     */
    @Override
    public synchronized IndexService createIndex(
            final IndexMetadata indexMetadata, final List<IndexEventListener> builtInListeners, final boolean writeDanglingIndices) throws IOException {

        ensureChangesAllowed();
        if (indexMetadata.getIndexUUID().equals(IndexMetadata.INDEX_UUID_NA_VALUE)) {
            throw new IllegalArgumentException("index must have a real UUID found value: [" + indexMetadata.getIndexUUID() + "]");
        }
        final Index index = indexMetadata.getIndex();
        if (hasIndex(index)) {
            throw new ResourceAlreadyExistsException(index);
        }
        List<IndexEventListener> finalListeners = new ArrayList<>(builtInListeners);
        final IndexEventListener onStoreClose = new IndexEventListener() {

            @Override
            public void onStoreCreated(ShardId shardId) {
                indicesRefCount.incRef();
            }

            @Override
            public void onStoreClosed(ShardId shardId) {
                indicesRefCount.decRef();
            }
        };
        finalListeners.add(onStoreClose);
        final IndexService indexService = createIndexService(
            IndexCreationContext.CREATE_INDEX,
            indexMetadata,
            indicesQueryCache,
            finalListeners,
            indexingMemoryController
        );
        boolean success = false;
        try {
            if (writeDanglingIndices && nodeWriteDanglingIndicesInfo) {
                indexService.addMetadataListener(imd -> updateDanglingIndicesInfo(index));
            }
            indexService.getIndexEventListener().afterIndexCreated(indexService);
            indices = newMapBuilder(indices).put(index.getUUID(), indexService).immutableMap();
            if (writeDanglingIndices) {
                if (nodeWriteDanglingIndicesInfo) {
                    updateDanglingIndicesInfo(index);
                } else {
                    indexService.deleteDanglingIndicesInfo();
                }
            }
            success = true;
            return indexService;
        } finally {
            if (success == false) {
                indexService.close("plugins_failed", true);
            }
        }
    }

    /**
     * This creates a new IndexService without registering itcreateIndex
     */
    private synchronized IndexService createIndexService(IndexCreationContext indexCreationContext,
                                                         IndexMetadata indexMetadata,
                                                         QueryCache indicesQueryCache,
                                                         List<IndexEventListener> builtInListeners,
                                                         IndexingOperationListener... indexingOperationListeners) throws IOException {
        final IndexSettings idxSettings = new IndexSettings(indexMetadata, this.settings, indexScopedSettings);
        // we ignore private settings since they are not registered settings
        indexScopedSettings.validate(indexMetadata.getSettings(), true, true, true);
        LOGGER.debug("creating Index [{}], shards [{}]/[{}] - reason [{}]",
            indexMetadata.getIndex(),
            idxSettings.getNumberOfShards(),
            idxSettings.getNumberOfReplicas(),
            indexCreationContext);

        final IndexModule indexModule = new IndexModule(idxSettings, analysisRegistry,engineFactoryProviders, directoryFactories);
        for (IndexingOperationListener operationListener : indexingOperationListeners) {
            indexModule.addIndexOperationListener(operationListener);
        }
        pluginsService.onIndexModule(indexModule);
        for (IndexEventListener listener : builtInListeners) {
            indexModule.addIndexEventListener(listener);
        }
        return indexModule.newIndexService(
            indexCreationContext,
            nodeEnv,
            xContentRegistry,
            this,
            circuitBreakerService,
            bigArrays,
            threadPool,
            indicesQueryCache,
            mapperRegistry
        );
    }

    /**
     * creates a new mapper service for the given index, in order to do administrative work like mapping updates.
     * This *should not* be used for document parsing. Doing so will result in an exception.
     *
     * Note: the returned {@link MapperService} should be closed when unneeded.
     */
    public synchronized MapperService createIndexMapperService(IndexMetadata indexMetadata) throws IOException {
        final IndexSettings idxSettings = new IndexSettings(indexMetadata, this.settings, indexScopedSettings);
        final IndexModule indexModule = new IndexModule(idxSettings, analysisRegistry, engineFactoryProviders, directoryFactories);
        pluginsService.onIndexModule(indexModule);
        return indexModule.newIndexMapperService(xContentRegistry, mapperRegistry);
    }

    /**
     * This method verifies that the given {@code metadata} holds sane values to create an {@link IndexService}.
     * This method tries to update the meta data of the created {@link IndexService} if the given {@code metadataUpdate} is different from the given {@code metadata}.
     * This method will throw an exception if the creation or the update fails.
     * The created {@link IndexService} will not be registered and will be closed immediately.
     */
    public synchronized void verifyIndexMetadata(IndexMetadata metadata, IndexMetadata metadataUpdate) throws IOException {
        final List<Closeable> closeables = new ArrayList<>();
        try {
            // this will also fail if some plugin fails etc. which is nice since we can verify that early
            final IndexService service = createIndexService(
                IndexCreationContext.METADATA_VERIFICATION,
                metadata,
                DisabledQueryCache.instance(),
                emptyList()
            );
            closeables.add(() -> service.close("metadata verification", false));
            service.mapperService().merge(metadata, MapperService.MergeReason.MAPPING_RECOVERY);
            if (metadata.equals(metadataUpdate) == false) {
                service.updateMetadata(metadata, metadataUpdate);
            }
        } finally {
            IOUtils.close(closeables);
        }
    }


    private void createShard(CompletableFuture<IndexShard> result,
                             ClusterState originalState,
                             IndexService indexService,
                             ShardRouting shardRouting,
                             Consumer<ShardId> globalCheckpointSyncer,
                             RetentionLeaseSyncer retentionLeaseSyncer,
                             @Nullable Iterator<TimeValue> backoff) {
        ShardId shardId = shardRouting.shardId();
        try {
            IndexShard indexShard = indexService.createShard(shardRouting, globalCheckpointSyncer, retentionLeaseSyncer);
            result.complete(indexShard);
        } catch (ShardLockObtainFailedException e) {
            if (e.getCause() instanceof InterruptedException || Thread.currentThread().isInterrupted()) {
                result.completeExceptionally(e);
                return;
            }

            // ~1.1hours in total; Retries take never more than 5000ms
            int firstDelayInMS = 50;
            int maxRetries = 1000;
            int maxDelayInMS = 5000;
            Iterator<TimeValue> backoffIt = backoff == null
                ? new LimitedExponentialBackoff(firstDelayInMS, maxRetries, maxDelayInMS).iterator()
                : backoff;
            TimeValue delay = backoffIt.next();
            if (LOGGER.isWarnEnabled() && delay.millis() == maxDelayInMS) {
                LOGGER.warn("Repeated attempts to acquire shardLock for {}. Retrying again in {}", shardId, delay);
            } else if (LOGGER.isDebugEnabled() && delay.millis() > 150) {
                LOGGER.debug("Repeated attempts to acquire shardLock for {}. Retrying again in {}", shardId, delay);
            }
            Runnable retry = () -> {
                clusterService.getClusterApplierService().runOnApplierThread(
                    "create-shard",
                    (ClusterState state) -> {
                        if (!state.stateUUID().equals(originalState.stateUUID())) {
                            LOGGER.debug(
                                "Cluster state changed from {} to {} before shard creation finished",
                                originalState.stateUUID(),
                                state.stateUUID()
                            );
                            result.completeExceptionally(new IllegalStateException("Cluster state changed during shard creation"));
                            return;
                        }
                        createShard(
                            result,
                            originalState,
                            indexService,
                            shardRouting,
                            globalCheckpointSyncer,
                            retentionLeaseSyncer,
                            backoffIt
                        );
                    },
                    new ClusterApplyListener() {

                        @Override
                        public void onFailure(String source, Exception e) {
                            result.completeExceptionally(e);
                        }
                    },
                    Priority.NORMAL
                );
            };
            threadPool.schedule(retry, delay, ThreadPool.Names.SAME);
        } catch (IOException e) {
            result.completeExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<IndexShard> createShard(ClusterState state,
                                                     ShardRouting shardRouting,
                                                     RecoveryState recoveryState,
                                                     PeerRecoveryTargetService recoveryTargetService,
                                                     PeerRecoveryTargetService.RecoveryListener recoveryListener,
                                                     RepositoriesService repositoriesService,
                                                     Consumer<IndexShard.ShardFailure> onShardFailure,
                                                     Consumer<ShardId> globalCheckpointSyncer,
                                                     RetentionLeaseSyncer retentionLeaseSyncer) throws IOException {
        Objects.requireNonNull(retentionLeaseSyncer);
        ensureChangesAllowed();
        IndexService indexService = indexService(shardRouting.index());
        ShardId shardId = shardRouting.shardId();
        CompletableFuture<IndexShard> future = new CompletableFuture<>();
        CompletableFuture<IndexShard> pending = pendingShardCreations.putIfAbsent(shardId, future);
        if (pending == null) {
            try {
                createShard(future, state, indexService, shardRouting, globalCheckpointSyncer, retentionLeaseSyncer, null);
            } catch (Throwable t) {
                pendingShardCreations.remove(shardId);
                throw t;
            }
            future.whenComplete((ignored, err) -> pendingShardCreations.remove(shardId));
        } else {
            future = pending;
        }
        return future.thenCompose(indexShard -> {
            indexShard.addShardFailureCallback(onShardFailure);
            CompletableFuture<IndexShard> mappingDone = new CompletableFuture<>();
            indexShard.startRecovery(recoveryState, recoveryTargetService, recoveryListener, repositoriesService,
                mapping -> {
                    assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS :
                        "mapping update consumer only required by local shards recovery";

                    var request = new PutMappingRequest()
                        .indices(new String[0])
                        .setConcreteIndex(shardRouting.index()) // concrete index - no name clash, it uses uuid
                        .source(mapping.source().string());
                    client.admin().indices().putMapping(request).whenComplete((ignored, err) -> {
                        if (err == null) {
                            mappingDone.complete(indexShard);
                        } else {
                            try {
                                indexShard.close("Failure during startRecovery mapping update", false);
                            } catch (IOException e) {
                                err.addSuppressed(e);
                            }
                            mappingDone.completeExceptionally(err);
                        }
                    });
                }, this);
            return mappingDone;
        });
    }

    @Override
    public void removeIndex(final Index index, final IndexRemovalReason reason, final String extraInfo) {
        final String indexName = index.getName();
        try {
            final IndexService indexService;
            final IndexEventListener listener;
            synchronized (this) {
                if (hasIndex(index) == false) {
                    return;
                }

                LOGGER.debug("[{}] closing ... (reason [{}])", indexName, reason);
                Map<String, IndexService> newIndices = new HashMap<>(indices);
                indexService = newIndices.remove(index.getUUID());
                assert indexService != null : "IndexService is null for index: " + index;
                indices = unmodifiableMap(newIndices);
                listener = indexService.getIndexEventListener();
            }

            listener.beforeIndexRemoved(indexService, reason);
            LOGGER.debug("{} closing index service (reason [{}][{}])", index, reason, extraInfo);
            indexService.close(extraInfo, reason == IndexRemovalReason.DELETED);
            LOGGER.debug("{} closed... (reason [{}][{}])", index, reason, extraInfo);
            final IndexSettings indexSettings = indexService.getIndexSettings();
            listener.afterIndexRemoved(indexService.index(), indexSettings, reason);
            if (reason == IndexRemovalReason.DELETED) {
                // now we are done - try to wipe data on disk if possible
                deleteIndexStore(extraInfo, indexService.index(), indexSettings);
            }
        } catch (Exception e) {
            LOGGER.warn(() -> new ParameterizedMessage("failed to remove index {} ([{}][{}])", index, reason, extraInfo), e);
        }
    }

    public CircuitBreakerService getCircuitBreakerService() {
        return circuitBreakerService;
    }

    /**
     * Deletes an index that is not assigned to this node. This method cleans up all disk folders relating to the index
     * but does not deal with in-memory structures. For those call {@link #removeIndex(Index, IndexRemovalReason, String)}
     */
    @Override
    public void deleteUnassignedIndex(String reason, IndexMetadata metadata, ClusterState clusterState) {
        if (nodeEnv.hasNodeFile()) {
            Index index = metadata.getIndex();
            try {
                if (clusterState.metadata().hasIndex(index)) {
                    final IndexMetadata indexMetadata = clusterState.metadata().index(index);
                    throw new IllegalStateException("Can't delete unassigned index store for [" + indexMetadata.getIndex().getName() + "] - it's still part of " +
                                                    "the cluster state [" + indexMetadata.getIndexUUID() + "] [" + metadata.getIndexUUID() + "]");
                }
                deleteIndexStore(reason, metadata);
            } catch (Exception e) {
                LOGGER.warn(() -> new ParameterizedMessage("[{}] failed to delete unassigned index (reason [{}])", metadata.getIndex(), reason), e);
            }
        }
    }

    /**
     * Deletes the index store trying to acquire all shards locks for this index.
     * This method will delete the metadata for the index even if the actual shards can't be locked.
     *
     * Package private for testing
     */
    void deleteIndexStore(String reason, IndexMetadata metadata) throws IOException {
        if (nodeEnv.hasNodeFile()) {
            synchronized (this) {
                Index index = metadata.getIndex();
                if (hasIndex(index)) {
                    String localUUid = indexService(index).indexUUID();
                    throw new IllegalStateException("Can't delete index store for [" + index.getName() + "] - it's still part of the indices service [" + localUUid + "] [" + metadata.getIndexUUID() + "]");
                }
            }
            final IndexSettings indexSettings = buildIndexSettings(metadata);
            deleteIndexStore(reason, indexSettings.getIndex(), indexSettings);
        }
    }

    private void deleteIndexStore(String reason, Index index, IndexSettings indexSettings) throws IOException {
        deleteIndexStoreIfDeletionAllowed(reason, index, indexSettings, DEFAULT_INDEX_DELETION_PREDICATE);
    }

    private void deleteIndexStoreIfDeletionAllowed(final String reason, final Index index, final IndexSettings indexSettings,
                                                   final IndexDeletionAllowedPredicate predicate) throws IOException {
        boolean success = false;
        try {
            // we are trying to delete the index store here - not a big deal if the lock can't be obtained
            // the store metadata gets wiped anyway even without the lock this is just best effort since
            // every shards deletes its content under the shard lock it owns.
            LOGGER.debug("{} deleting index store reason [{}]", index, reason);
            if (predicate.apply(index, indexSettings)) {
                // its safe to delete all index metadata and shard data
                nodeEnv.deleteIndexDirectorySafe(index, 0, indexSettings);
            }
            success = true;
        } catch (ShardLockObtainFailedException ex) {
            LOGGER.debug(() -> new ParameterizedMessage("{} failed to delete index store - at least one shards is still locked", index), ex);
        } catch (Exception ex) {
            LOGGER.warn(() -> new ParameterizedMessage("{} failed to delete index", index), ex);
        } finally {
            if (success == false) {
                addPendingDelete(index, indexSettings);
            }
            // this is a pure protection to make sure this index doesn't get re-imported as a dangling index.
            // we should in the future rather write a tombstone rather than wiping the metadata.
            MetadataStateFormat.deleteMetaState(nodeEnv.indexPaths(index));
        }
    }

    /**
     * Deletes the shard with an already acquired shard lock.
     * @param reason the reason for the shard deletion
     * @param lock the lock of the shard to delete
     * @param indexSettings the shards index settings.
     * @throws IOException if an IOException occurs
     */
    @Override
    public void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException {
        ShardId shardId = lock.getShardId();
        LOGGER.trace("{} deleting shard reason [{}]", shardId, reason);
        nodeEnv.deleteShardDirectoryUnderLock(lock, indexSettings);
    }

    /**
     * This method deletes the shard contents on disk for the given shard ID. This method will fail if the shard deleting
     * is prevented by {@link #canDeleteShardContent(ShardId, IndexSettings)}
     * of if the shards lock can not be acquired.
     *
     * On data nodes, if the deleted shard is the last shard folder in its index, the method will attempt to remove the index folder as well.
     *
     * @param reason the reason for the shard deletion
     * @param shardId the shards ID to delete
     * @param clusterState . This is required to access the indexes settings etc.
     * @throws IOException if an IOException occurs
     */
    public void deleteShardStore(String reason, ShardId shardId, ClusterState clusterState)
            throws IOException, ShardLockObtainFailedException {
        final IndexMetadata metadata = clusterState.metadata().indices().get(shardId.getIndexName());

        final IndexSettings indexSettings = buildIndexSettings(metadata);
        ShardDeletionCheckResult shardDeletionCheckResult = canDeleteShardContent(shardId, indexSettings);
        if (shardDeletionCheckResult != ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE) {
            throw new IllegalStateException("Can't delete shard " + shardId + " (cause: " + shardDeletionCheckResult + ")");
        }
        nodeEnv.deleteShardDirectorySafe(shardId, indexSettings);
        LOGGER.debug("{} deleted shard reason [{}]", shardId, reason);

        if (canDeleteIndexContents(shardId.getIndex(), indexSettings)) {
            if (nodeEnv.findAllShardIds(shardId.getIndex()).isEmpty()) {
                try {
                    // note that deleteIndexStore have more safety checks and may throw an exception if index was concurrently created.
                    deleteIndexStore("no longer used", metadata);
                } catch (Exception e) {
                    // wrap the exception to indicate we already deleted the shard
                    throw new ElasticsearchException("failed to delete unused index after deleting its last shard (" + shardId + ")", e);
                }
            } else {
                LOGGER.trace("[{}] still has shard stores, leaving as is", shardId.getIndex());
            }
        }
    }

    /**
     * This method returns true if the current node is allowed to delete the given index.
     * This is the case if the index is deleted in the metadata or there is no allocation
     * on the local node and the index isn't on a shared file system.
     * @param index {@code Index} to check whether deletion is allowed
     * @param indexSettings {@code IndexSettings} for the given index
     * @return true if the index can be deleted on this node
     */
    public boolean canDeleteIndexContents(Index index, IndexSettings indexSettings) {
        // index contents can be deleted if its an already closed index (so all its resources have
        // already been relinquished)
        final IndexService indexService = indexService(index);
        if (indexService == null && nodeEnv.hasNodeFile()) {
            return true;
        }
        return false;
    }

    /**
     * Verify that the contents on disk for the given index is deleted; if not, delete the contents.
     * This method assumes that an index is already deleted in the cluster state and/or explicitly
     * through index tombstones.
     * @param index {@code Index} to make sure its deleted from disk
     * @param clusterState {@code ClusterState} to ensure the index is not part of it
     * @return IndexMetadata for the index loaded from disk
     */
    @Override
    @Nullable
    public IndexMetadata verifyIndexIsDeleted(final Index index, final ClusterState clusterState) {
        // this method should only be called when we know the index (name + uuid) is not part of the cluster state
        if (clusterState.metadata().index(index) != null) {
            throw new IllegalStateException("Cannot delete index [" + index + "], it is still part of the cluster state.");
        }
        if (nodeEnv.hasNodeFile() && FileSystemUtils.exists(nodeEnv.indexPaths(index))) {
            final IndexMetadata metadata;
            try {
                metadata = metaStateService.loadIndexState(index);
                if (metadata == null) {
                    return null;
                }
            } catch (Exception e) {
                LOGGER.warn(() -> new ParameterizedMessage("[{}] failed to load state file from a stale deleted index, folders will be left on disk", index), e);
                return null;
            }
            final IndexSettings indexSettings = buildIndexSettings(metadata);
            try {
                deleteIndexStoreIfDeletionAllowed("stale deleted index", index, indexSettings, ALWAYS_TRUE);
            } catch (Exception e) {
                // we just warn about the exception here because if deleteIndexStoreIfDeletionAllowed
                // throws an exception, it gets added to the list of pending deletes to be tried again
                LOGGER.warn(() -> new ParameterizedMessage("[{}] failed to delete index on disk", metadata.getIndex()), e);
            }
            return metadata;
        }
        return null;
    }

    /**
     * result type returned by {@link #canDeleteShardContent signaling different reasons why a shard can / cannot be deleted}
     */
    public enum ShardDeletionCheckResult {
        FOLDER_FOUND_CAN_DELETE, // shard data exists and can be deleted
        STILL_ALLOCATED, // the shard is still allocated / active on this node
        NO_FOLDER_FOUND, // the shards data locations do not exist
        NO_LOCAL_STORAGE // node does not have local storage (see DiscoveryNode.nodeRequiresLocalStorage)
    }

    /**
     * Returns <code>ShardDeletionCheckResult</code> signaling whether the shards content for the given shard can be deleted.
     *
     * @param shardId the shard to delete.
     * @param indexSettings the shards's relevant {@link IndexSettings}. This is required to access the indexes settings etc.
     */
    public ShardDeletionCheckResult canDeleteShardContent(ShardId shardId, IndexSettings indexSettings) {
        assert shardId.getIndex().equals(indexSettings.getIndex());
        final IndexService indexService = indexService(shardId.getIndex());
        if (nodeEnv.hasNodeFile()) {
            final boolean isAllocated = indexService != null && indexService.hasShard(shardId.id());
            if (isAllocated) {
                return ShardDeletionCheckResult.STILL_ALLOCATED; // we are allocated - can't delete the shard
            } else if (indexSettings.hasCustomDataPath()) {
                // lets see if it's on a custom path (return false if the shared doesn't exist)
                // we don't need to delete anything that is not there
                return Files.exists(nodeEnv.resolveCustomLocation(indexSettings.customDataPath(), shardId)) ?
                        ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE :
                        ShardDeletionCheckResult.NO_FOLDER_FOUND;
            } else {
                // lets see if it's path is available (return false if the shared doesn't exist)
                // we don't need to delete anything that is not there
                return FileSystemUtils.exists(nodeEnv.availableShardPaths(shardId)) ?
                        ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE :
                        ShardDeletionCheckResult.NO_FOLDER_FOUND;
            }
        } else {
            return ShardDeletionCheckResult.NO_LOCAL_STORAGE;
        }
    }

    private IndexSettings buildIndexSettings(IndexMetadata metadata) {
        // play safe here and make sure that we take node level settings into account.
        // we might run on nodes where we use shard FS and then in the future don't delete
        // actual content.
        return new IndexSettings(metadata, settings);
    }

    /**
     * Adds a pending delete for the given index shard.
     */
    @Override
    public void addPendingDelete(ShardId shardId, IndexSettings settings) {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (settings == null) {
            throw new IllegalArgumentException("settings must not be null");
        }
        PendingDelete pendingDelete = new PendingDelete(shardId, settings);
        addPendingDelete(shardId.getIndex(), pendingDelete);
    }

    /**
     * Adds a pending delete for the given index.
     */
    public void addPendingDelete(Index index, IndexSettings settings) {
        PendingDelete pendingDelete = new PendingDelete(index, settings);
        addPendingDelete(index, pendingDelete);
    }

    private void addPendingDelete(Index index, PendingDelete pendingDelete) {
        synchronized (pendingDeletes) {
            List<PendingDelete> list = pendingDeletes.get(index);
            if (list == null) {
                list = new ArrayList<>();
                pendingDeletes.put(index, list);
            }
            list.add(pendingDelete);
            numUncompletedDeletes.incrementAndGet();
        }
    }

    private static final class PendingDelete implements Comparable<PendingDelete> {
        final Index index;
        final int shardId;
        final IndexSettings settings;
        final boolean deleteIndex;

        /**
         * Creates a new pending delete of an index
         */
        PendingDelete(ShardId shardId, IndexSettings settings) {
            this.index = shardId.getIndex();
            this.shardId = shardId.id();
            this.settings = settings;
            this.deleteIndex = false;
        }

        /**
         * Creates a new pending delete of a shard
         */
        PendingDelete(Index index, IndexSettings settings) {
            this.index = index;
            this.shardId = -1;
            this.settings = settings;
            this.deleteIndex = true;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(index).append("]");
            if (shardId != -1) {
                sb.append("[").append(shardId).append("]");
            }
            return sb.toString();
        }

        @Override
        public int compareTo(PendingDelete o) {
            return Integer.compare(shardId, o.shardId);
        }
    }

    /**
     * Processes all pending deletes for the given index. This method will acquire all locks for the given index and will
     * process all pending deletes for this index. Pending deletes might occur if the OS doesn't allow deletion of files because
     * they are used by a different process ie. on Windows where files might still be open by a virus scanner. On a shared
     * filesystem a replica might not have been closed when the primary is deleted causing problems on delete calls so we
     * schedule there deletes later.
     * @param index the index to process the pending deletes for
     * @param timeout the timeout used for processing pending deletes
     */
    @Override
    public void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeout)
            throws IOException, InterruptedException, ShardLockObtainFailedException {
        LOGGER.debug("{} processing pending deletes", index);
        final long startTimeNS = System.nanoTime();
        final List<ShardLock> shardLocks = nodeEnv.lockAllForIndex(index, indexSettings, "process pending deletes", timeout.millis());
        int numRemoved = 0;
        try {
            Map<ShardId, ShardLock> locks = new HashMap<>();
            for (ShardLock lock : shardLocks) {
                locks.put(lock.getShardId(), lock);
            }
            final List<PendingDelete> remove;
            synchronized (pendingDeletes) {
                remove = pendingDeletes.remove(index);
            }
            if (remove != null && remove.isEmpty() == false) {
                numRemoved = remove.size();
                CollectionUtil.timSort(remove); // make sure we delete indices first
                final long maxSleepTimeMs = 10 * 1000; // ensure we retry after 10 sec
                long sleepTime = 10;
                do {
                    if (remove.isEmpty()) {
                        break;
                    }
                    Iterator<PendingDelete> iterator = remove.iterator();
                    while (iterator.hasNext()) {
                        PendingDelete delete = iterator.next();

                        if (delete.deleteIndex) {
                            assert delete.shardId == -1;
                            LOGGER.debug("{} deleting index store reason [{}]", index, "pending delete");
                            try {
                                nodeEnv.deleteIndexDirectoryUnderLock(index, indexSettings);
                                iterator.remove();
                            } catch (IOException ex) {
                                LOGGER.debug(() -> new ParameterizedMessage("{} retry pending delete", index), ex);
                            }
                        } else {
                            assert delete.shardId != -1;
                            ShardLock shardLock = locks.get(new ShardId(delete.index, delete.shardId));
                            if (shardLock != null) {
                                try {
                                    deleteShardStore("pending delete", shardLock, delete.settings);
                                    iterator.remove();
                                } catch (IOException ex) {
                                    LOGGER.debug(() -> new ParameterizedMessage("{} retry pending delete", shardLock.getShardId()), ex);
                                }
                            } else {
                                LOGGER.warn("{} no shard lock for pending delete", delete.shardId);
                                iterator.remove();
                            }
                        }
                    }
                    if (remove.isEmpty() == false) {
                        LOGGER.warn("{} still pending deletes present for shards {} - retrying", index, remove.toString());
                        Thread.sleep(sleepTime);
                        sleepTime = Math.min(maxSleepTimeMs, sleepTime * 2); // increase the sleep time gradually
                        LOGGER.debug("{} schedule pending delete retry after {} ms", index, sleepTime);
                    }
                } while ((System.nanoTime() - startTimeNS) < timeout.nanos());
            }
        } finally {
            IOUtils.close(shardLocks);
            if (numRemoved > 0) {
                int remainingUncompletedDeletes = numUncompletedDeletes.addAndGet(-numRemoved);
                assert remainingUncompletedDeletes >= 0;
            }
        }
    }

    int numPendingDeletes(Index index) {
        synchronized (pendingDeletes) {
            List<PendingDelete> deleteList = pendingDeletes.get(index);
            if (deleteList == null) {
                return 0;
            }
            return deleteList.size();
        }
    }

    /**
     * Checks if all pending deletes have completed. Used by tests to ensure we don't check directory contents while deletion still ongoing.
     * The reason is that, on Windows, browsing the directory contents can interfere with the deletion process and delay it unnecessarily.
     */
    public boolean hasUncompletedPendingDeletes() {
        return numUncompletedDeletes.get() > 0;
    }

    public AnalysisRegistry getAnalysis() {
        return analysisRegistry;
    }

    public ByteSizeValue getTotalIndexingBufferBytes() {
        return indexingMemoryController.indexingBufferSize();
    }

    @FunctionalInterface
    interface IndexDeletionAllowedPredicate {
        boolean apply(Index index, IndexSettings indexSettings);
    }

    private final IndexDeletionAllowedPredicate DEFAULT_INDEX_DELETION_PREDICATE =
        (Index index, IndexSettings indexSettings) -> canDeleteIndexContents(index, indexSettings);
    private final IndexDeletionAllowedPredicate ALWAYS_TRUE = (Index index, IndexSettings indexSettings) -> true;

    /**
     * Returns a function which given an index name, returns a predicate which fields must match in order to be returned by get mappings,
     * get index, get field mappings and field capabilities API. Useful to filter the fields that such API return.
     * The predicate receives the the field name as input argument. In case multiple plugins register a field filter through
     * {@link org.elasticsearch.plugins.MapperPlugin#getFieldFilter()}, only fields that match all the registered filters will be
     * returned by get mappings, get index, get field mappings and field capabilities API.
     */
    public Function<String, Predicate<String>> getFieldFilter() {
        return mapperRegistry.getFieldFilter();
    }

    /**
     * Returns true if the provided field is a registered metadata field (including ones registered via plugins), false otherwise.
     */
    public boolean isMetadataField(String field) {
        return mapperRegistry.isMetadataField(field);
    }

    public MapperRegistry getMapperRegistry() {
        return mapperRegistry;
    }

    private void updateDanglingIndicesInfo(Index index) {
        assert DiscoveryNode.isDataNode(settings) : "dangling indices information should only be persisted on data nodes";
        assert nodeWriteDanglingIndicesInfo : "writing dangling indices info is not enabled";
        assert danglingIndicesThreadPoolExecutor != null : "executor for dangling indices info is not available";
        if (danglingIndicesToWrite.add(index)) {
            LOGGER.trace("triggered dangling indices update for {}", index);
            final long triggeredTimeMillis = threadPool.relativeTimeInMillis();
            try {
                danglingIndicesThreadPoolExecutor.execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.warn(() -> new ParameterizedMessage("failed to write dangling indices state for index {}", index), e);
                    }

                    @Override
                    protected void doRun() {
                        final boolean exists = danglingIndicesToWrite.remove(index);
                        assert exists : "removed non-existing item for " + index;
                        final IndexService indexService = indices.get(index.getUUID());
                        if (indexService != null) {
                            final long executedTimeMillis = threadPool.relativeTimeInMillis();
                            LOGGER.trace("writing out dangling indices state for index {}, triggered {} ago", index,
                                         TimeValue.timeValueMillis(Math.min(0L, executedTimeMillis - triggeredTimeMillis)));
                            indexService.writeDanglingIndicesInfo();
                            final long completedTimeMillis = threadPool.relativeTimeInMillis();
                            LOGGER.trace("writing out of dangling indices state for index {} completed after {}", index,
                                         TimeValue.timeValueMillis(Math.min(0L, completedTimeMillis - executedTimeMillis)));
                        } else {
                            LOGGER.trace("omit writing dangling indices state for index {} as index is deallocated on this node", index);
                        }
                    }
                });
            } catch (EsRejectedExecutionException e) {
                // ignore cases where we are shutting down..., there is really nothing interesting to be done here...
                assert danglingIndicesThreadPoolExecutor.isShutdown();
            }
        } else {
            LOGGER.trace("dangling indices update already pending for {}", index);
        }
    }

    // visible for testing
    public boolean allPendingDanglingIndicesWritten() {
        return nodeWriteDanglingIndicesInfo == false ||
               (danglingIndicesToWrite.isEmpty() && danglingIndicesThreadPoolExecutor.getActiveCount() == 0);
    }
}
