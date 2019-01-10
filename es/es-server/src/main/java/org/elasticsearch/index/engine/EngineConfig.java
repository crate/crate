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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.function.LongSupplier;

/*
 * Holds all the configuration that is used to create an {@link Engine}.
 * Once {@link Engine} has been created with this object, changes to this
 * object will affect the {@link Engine} instance.
 */
public final class EngineConfig {
    private final ShardId shardId;
    private final String allocationId;
    private final IndexSettings indexSettings;
    private final ByteSizeValue indexingBufferSize;
    private volatile boolean enableGcDeletes = true;
    private final TimeValue flushMergesAfter;
    private final String codecName;
    private final ThreadPool threadPool;
    private final Engine.Warmer warmer;
    private final Store store;
    private final MergePolicy mergePolicy;
    private final Analyzer analyzer;
    private final Similarity similarity;
    private final CodecService codecService;
    private final Engine.EventListener eventListener;
    private final QueryCache queryCache;
    private final QueryCachingPolicy queryCachingPolicy;
    @Nullable
    private final List<ReferenceManager.RefreshListener> externalRefreshListener;
    @Nullable
    private final List<ReferenceManager.RefreshListener> internalRefreshListener;
    @Nullable
    private final Sort indexSort;
    @Nullable
    private final CircuitBreakerService circuitBreakerService;
    private final LongSupplier globalCheckpointSupplier;
    private final LongSupplier primaryTermSupplier;
    private final TombstoneDocSupplier tombstoneDocSupplier;

    /**
     * Index setting to change the low level lucene codec used for writing new segments.
     * This setting is <b>not</b> realtime updateable.
     * This setting is also settable on the node and the index level, it's commonly used in hot/cold node archs where index is likely
     * allocated on both `kind` of nodes.
     */
    public static final Setting<String> INDEX_CODEC_SETTING = new Setting<>("index.codec", "default", s -> {
        switch (s) {
            case "default":
            case "best_compression":
            case "lucene_default":
                return s;
            default:
                if (Codec.availableCodecs().contains(s) == false) { // we don't error message the not officially supported ones
                    throw new IllegalArgumentException(
                        "unknown value for [index.codec] must be one of [default, best_compression] but was: " + s);
                }
                return s;
        }
    }, Property.IndexScope, Property.NodeScope);

    /**
     * Configures an index to optimize documents with auto generated ids for append only. If this setting is updated from <code>false</code>
     * to <code>true</code> might not take effect immediately. In other words, disabling the optimization will be immediately applied while
     * re-enabling it might not be applied until the engine is in a safe state to do so. Depending on the engine implementation a change to
     * this setting won't be reflected re-enabled optimization until the engine is restarted or the index is closed and reopened.
     * The default is <code>true</code>
     */
    public static final Setting<Boolean> INDEX_OPTIMIZE_AUTO_GENERATED_IDS = Setting.boolSetting("index.optimize_auto_generated_id", true,
        Property.IndexScope, Property.Dynamic);

    private final TranslogConfig translogConfig;

    /**
     * Creates a new {@link org.elasticsearch.index.engine.EngineConfig}
     */
    public EngineConfig(ShardId shardId, String allocationId, ThreadPool threadPool,
                        IndexSettings indexSettings, Engine.Warmer warmer, Store store,
                        MergePolicy mergePolicy, Analyzer analyzer,
                        Similarity similarity, CodecService codecService, Engine.EventListener eventListener,
                        QueryCache queryCache, QueryCachingPolicy queryCachingPolicy,
                        TranslogConfig translogConfig, TimeValue flushMergesAfter,
                        List<ReferenceManager.RefreshListener> externalRefreshListener,
                        List<ReferenceManager.RefreshListener> internalRefreshListener, Sort indexSort,
                        CircuitBreakerService circuitBreakerService, LongSupplier globalCheckpointSupplier,
                        LongSupplier primaryTermSupplier, TombstoneDocSupplier tombstoneDocSupplier) {
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.indexSettings = indexSettings;
        this.threadPool = threadPool;
        this.warmer = warmer == null ? (a) -> {} : warmer;
        this.store = store;
        this.mergePolicy = mergePolicy;
        this.analyzer = analyzer;
        this.similarity = similarity;
        this.codecService = codecService;
        this.eventListener = eventListener;
        codecName = indexSettings.getValue(INDEX_CODEC_SETTING);
        // We need to make the indexing buffer for this shard at least as large
        // as the amount of memory that is available for all engines on the
        // local node so that decisions to flush segments to disk are made by
        // IndexingMemoryController rather than Lucene.
        // Add an escape hatch in case this change proves problematic - it used
        // to be a fixed amound of RAM: 256 MB.
        // TODO: Remove this escape hatch in 8.x
        final String escapeHatchProperty = "es.index.memory.max_index_buffer_size";
        String maxBufferSize = System.getProperty(escapeHatchProperty);
        if (maxBufferSize != null) {
            indexingBufferSize = MemorySizeValue.parseBytesSizeValueOrHeapRatio(maxBufferSize, escapeHatchProperty);
        } else {
            indexingBufferSize = IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING.get(indexSettings.getNodeSettings());
        }
        this.queryCache = queryCache;
        this.queryCachingPolicy = queryCachingPolicy;
        this.translogConfig = translogConfig;
        this.flushMergesAfter = flushMergesAfter;
        this.externalRefreshListener = externalRefreshListener;
        this.internalRefreshListener = internalRefreshListener;
        this.indexSort = indexSort;
        this.circuitBreakerService = circuitBreakerService;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.primaryTermSupplier = primaryTermSupplier;
        this.tombstoneDocSupplier = tombstoneDocSupplier;
    }

    /**
     * Enables / disables gc deletes
     *
     * @see #isEnableGcDeletes()
     */
    public void setEnableGcDeletes(boolean enableGcDeletes) {
        this.enableGcDeletes = enableGcDeletes;
    }

    /**
     * Returns the initial index buffer size. This setting is only read on startup and otherwise controlled
     * by {@link IndexingMemoryController}
     */
    public ByteSizeValue getIndexingBufferSize() {
        return indexingBufferSize;
    }

    /**
     * Returns <code>true</code> iff delete garbage collection in the engine should be enabled. This setting is updateable
     * in realtime and forces a volatile read. Consumers can safely read this value directly go fetch it's latest value.
     * The default is <code>true</code>
     * <p>
     *     Engine GC deletion if enabled collects deleted documents from in-memory realtime data structures after a certain amount of
     *     time ({@link IndexSettings#getGcDeletesInMillis()} if enabled. Before deletes are GCed they will cause re-adding the document
     *     that was deleted to fail.
     * </p>
     */
    public boolean isEnableGcDeletes() {
        return enableGcDeletes;
    }

    /**
     * Returns the {@link Codec} used in the engines {@link org.apache.lucene.index.IndexWriter}
     * <p>
     *     Note: this settings is only read on startup.
     * </p>
     */
    public Codec getCodec() {
        return codecService.codec(codecName);
    }

    /**
     * Returns a thread-pool mainly used to get estimated time stamps from
     * {@link org.elasticsearch.threadpool.ThreadPool#relativeTimeInMillis()} and to schedule
     * async force merge calls on the {@link org.elasticsearch.threadpool.ThreadPool.Names#FORCE_MERGE} thread-pool
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    /**
     * Returns an {@link org.elasticsearch.index.engine.Engine.Warmer} used to warm new searchers before they are used for searching.
     */
    public Engine.Warmer getWarmer() {
        return warmer;
    }

    /**
     * Returns the {@link org.elasticsearch.index.store.Store} instance that provides access to the
     * {@link org.apache.lucene.store.Directory} used for the engines {@link org.apache.lucene.index.IndexWriter} to write it's index files
     * to.
     * <p>
     * Note: In order to use this instance the consumer needs to increment the stores reference before it's used the first time and hold
     * it's reference until it's not needed anymore.
     * </p>
     */
    public Store getStore() {
        return store;
    }

    /**
     * Returns the global checkpoint tracker
     */
    public LongSupplier getGlobalCheckpointSupplier() {
        return globalCheckpointSupplier;
    }

    /**
     * Returns the {@link org.apache.lucene.index.MergePolicy} for the engines {@link org.apache.lucene.index.IndexWriter}
     */
    public MergePolicy getMergePolicy() {
        return mergePolicy;
    }

    /**
     * Returns a listener that should be called on engine failure
     */
    public Engine.EventListener getEventListener() {
        return eventListener;
    }

    /**
     * Returns the index settings for this index.
     */
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Returns the engines shard ID
     */
    public ShardId getShardId() { return shardId; }

    /**
     * Returns the allocation ID for the shard.
     *
     * @return the allocation ID
     */
    public String getAllocationId() {
        return allocationId;
    }

    /**
     * Returns the analyzer as the default analyzer in the engines {@link org.apache.lucene.index.IndexWriter}
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * Returns the {@link org.apache.lucene.search.similarities.Similarity} used for indexing and searching.
     */
    public Similarity getSimilarity() {
        return similarity;
    }

    /**
     * Return the cache to use for queries.
     */
    public QueryCache getQueryCache() {
        return queryCache;
    }

    /**
     * Return the policy to use when caching queries.
     */
    public QueryCachingPolicy getQueryCachingPolicy() {
        return queryCachingPolicy;
    }

    /**
     * Returns the translog config for this engine
     */
    public TranslogConfig getTranslogConfig() {
        return translogConfig;
    }

    /**
     * Returns a {@link TimeValue} at what time interval after the last write modification to the engine finished merges
     * should be automatically flushed. This is used to free up transient disk usage of potentially large segments that
     * are written after the engine became inactive from an indexing perspective.
     */
    public TimeValue getFlushMergesAfter() { return flushMergesAfter; }

    /**
     * The refresh listeners to add to Lucene for externally visible refreshes
     */
    public List<ReferenceManager.RefreshListener> getExternalRefreshListener() {
        return externalRefreshListener;
    }

    /**
     * The refresh listeners to add to Lucene for internally visible refreshes. These listeners will also be invoked on external refreshes
     */
    public List<ReferenceManager.RefreshListener> getInternalRefreshListener() { return internalRefreshListener;}


    /**
     * returns true if the engine is allowed to optimize indexing operations with an auto-generated ID
     */
    public boolean isAutoGeneratedIDsOptimizationEnabled() {
        return indexSettings.getValue(INDEX_OPTIMIZE_AUTO_GENERATED_IDS);
    }

    /**
     * Return the sort order of this index, or null if the index has no sort.
     */
    public Sort getIndexSort() {
        return indexSort;
    }

    /**
     * Returns the circuit breaker service for this engine, or {@code null} if none is to be used.
     */
    @Nullable
    public CircuitBreakerService getCircuitBreakerService() {
        return this.circuitBreakerService;
    }

    /**
     * Returns a supplier that supplies the latest primary term value of the associated shard.
     */
    public LongSupplier getPrimaryTermSupplier() {
        return primaryTermSupplier;
    }

    /**
     * A supplier supplies tombstone documents which will be used in soft-update methods.
     * The returned document consists only _uid, _seqno, _term and _version fields; other metadata fields are excluded.
     */
    public interface TombstoneDocSupplier {
        /**
         * Creates a tombstone document for a delete operation.
         */
        ParsedDocument newDeleteTombstoneDoc(String type, String id);

        /**
         * Creates a tombstone document for a noop operation.
         * @param reason the reason of an a noop
         */
        ParsedDocument newNoopTombstoneDoc(String reason);
    }

    public TombstoneDocSupplier getTombstoneDocSupplier() {
        return tombstoneDocSupplier;
    }
}
