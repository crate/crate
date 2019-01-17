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

package org.elasticsearch.action.admin.indices.stats;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.IndicesQueryCache;

import java.io.IOException;

public class CommonStats implements Writeable {

    @Nullable
    public DocsStats docs;

    @Nullable
    public StoreStats store;

    @Nullable
    public IndexingStats indexing;

    @Nullable
    public MergeStats merge;

    @Nullable
    public RefreshStats refresh;

    @Nullable
    public FlushStats flush;

    @Nullable
    public WarmerStats warmer;

    @Nullable
    public QueryCacheStats queryCache;

    @Nullable
    public FieldDataStats fieldData;

    @Nullable
    public SegmentsStats segments;

    @Nullable
    public TranslogStats translog;

    @Nullable
    public RequestCacheStats requestCache;

    @Nullable
    public RecoveryStats recoveryStats;

    public CommonStats() {
        this(CommonStatsFlags.NONE);
    }

    public CommonStats(CommonStatsFlags flags) {
        CommonStatsFlags.Flag[] setFlags = flags.getFlags();

        for (CommonStatsFlags.Flag flag : setFlags) {
            switch (flag) {
                case Docs:
                    docs = new DocsStats();
                    break;
                case Store:
                    store = new StoreStats();
                    break;
                case Indexing:
                    indexing = new IndexingStats();
                    break;
                case Merge:
                    merge = new MergeStats();
                    break;
                case Refresh:
                    refresh = new RefreshStats();
                    break;
                case Flush:
                    flush = new FlushStats();
                    break;
                case Warmer:
                    warmer = new WarmerStats();
                    break;
                case QueryCache:
                    queryCache = new QueryCacheStats();
                    break;
                case FieldData:
                    fieldData = new FieldDataStats();
                    break;
                case Segments:
                    segments = new SegmentsStats();
                    break;
                case Translog:
                    translog = new TranslogStats();
                    break;
                case RequestCache:
                    requestCache = new RequestCacheStats();
                    break;
                case Recovery:
                    recoveryStats = new RecoveryStats();
                    break;
                default:
                    throw new IllegalStateException("Unknown Flag: " + flag);
            }
        }
    }

    public CommonStats(IndicesQueryCache indicesQueryCache, IndexShard indexShard, CommonStatsFlags flags) {
        CommonStatsFlags.Flag[] setFlags = flags.getFlags();
        for (CommonStatsFlags.Flag flag : setFlags) {
            try {
                switch (flag) {
                    case Docs:
                        docs = indexShard.docStats();
                        break;
                    case Store:
                        store = indexShard.storeStats();
                        break;
                    case Indexing:
                        indexing = indexShard.indexingStats(flags.types());
                        break;
                    case Merge:
                        merge = indexShard.mergeStats();
                        break;
                    case Refresh:
                        refresh = indexShard.refreshStats();
                        break;
                    case Flush:
                        flush = indexShard.flushStats();
                        break;
                    case Warmer:
                        warmer = indexShard.warmerStats();
                        break;
                    case QueryCache:
                        queryCache = indicesQueryCache.getStats(indexShard.shardId());
                        break;
                    case FieldData:
                        fieldData = indexShard.fieldDataStats(flags.fieldDataFields());
                        break;
                    case Segments:
                        segments = indexShard.segmentStats(flags.includeSegmentFileSizes());
                        break;
                    case Translog:
                        translog = indexShard.translogStats();
                        break;
                    case RequestCache:
                        requestCache = indexShard.requestCache().stats();
                        break;
                    case Recovery:
                        recoveryStats = indexShard.recoveryStats();
                        break;
                    default:
                        throw new IllegalStateException("Unknown Flag: " + flag);
                }
            } catch (AlreadyClosedException e) {
                // shard is closed - no stats is fine
            }
        }
    }

    public CommonStats(StreamInput in) throws IOException {
        docs = in.readOptionalStreamable(DocsStats::new);
        store = in.readOptionalStreamable(StoreStats::new);
        indexing = in.readOptionalStreamable(IndexingStats::new);
        merge = in.readOptionalStreamable(MergeStats::new);
        refresh =  in.readOptionalStreamable(RefreshStats::new);
        flush =  in.readOptionalStreamable(FlushStats::new);
        warmer =  in.readOptionalStreamable(WarmerStats::new);
        queryCache = in.readOptionalStreamable(QueryCacheStats::new);
        fieldData =  in.readOptionalStreamable(FieldDataStats::new);
        segments =  in.readOptionalStreamable(SegmentsStats::new);
        translog = in.readOptionalStreamable(TranslogStats::new);
        requestCache = in.readOptionalStreamable(RequestCacheStats::new);
        recoveryStats = in.readOptionalStreamable(RecoveryStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStreamable(docs);
        out.writeOptionalStreamable(store);
        out.writeOptionalStreamable(indexing);
        out.writeOptionalStreamable(merge);
        out.writeOptionalStreamable(refresh);
        out.writeOptionalStreamable(flush);
        out.writeOptionalStreamable(warmer);
        out.writeOptionalStreamable(queryCache);
        out.writeOptionalStreamable(fieldData);
        out.writeOptionalStreamable(segments);
        out.writeOptionalStreamable(translog);
        out.writeOptionalStreamable(requestCache);
        out.writeOptionalStreamable(recoveryStats);
    }

    public void add(CommonStats stats) {
        if (docs == null) {
            if (stats.getDocs() != null) {
                docs = new DocsStats();
                docs.add(stats.getDocs());
            }
        } else {
            docs.add(stats.getDocs());
        }
        if (store == null) {
            if (stats.getStore() != null) {
                store = new StoreStats();
                store.add(stats.getStore());
            }
        } else {
            store.add(stats.getStore());
        }
        if (indexing == null) {
            if (stats.getIndexing() != null) {
                indexing = new IndexingStats();
                indexing.add(stats.getIndexing());
            }
        } else {
            indexing.add(stats.getIndexing());
        }
        if (merge == null) {
            if (stats.getMerge() != null) {
                merge = new MergeStats();
                merge.add(stats.getMerge());
            }
        } else {
            merge.add(stats.getMerge());
        }
        if (refresh == null) {
            if (stats.getRefresh() != null) {
                refresh = new RefreshStats();
                refresh.add(stats.getRefresh());
            }
        } else {
            refresh.add(stats.getRefresh());
        }
        if (flush == null) {
            if (stats.getFlush() != null) {
                flush = new FlushStats();
                flush.add(stats.getFlush());
            }
        } else {
            flush.add(stats.getFlush());
        }
        if (warmer == null) {
            if (stats.getWarmer() != null) {
                warmer = new WarmerStats();
                warmer.add(stats.getWarmer());
            }
        } else {
            warmer.add(stats.getWarmer());
        }
        if (queryCache == null) {
            if (stats.getQueryCache() != null) {
                queryCache = new QueryCacheStats();
                queryCache.add(stats.getQueryCache());
            }
        } else {
            queryCache.add(stats.getQueryCache());
        }

        if (fieldData == null) {
            if (stats.getFieldData() != null) {
                fieldData = new FieldDataStats();
                fieldData.add(stats.getFieldData());
            }
        } else {
            fieldData.add(stats.getFieldData());
        }
        if (segments == null) {
            if (stats.getSegments() != null) {
                segments = new SegmentsStats();
                segments.add(stats.getSegments());
            }
        } else {
            segments.add(stats.getSegments());
        }
        if (translog == null) {
            if (stats.getTranslog() != null) {
                translog = new TranslogStats();
                translog.add(stats.getTranslog());
            }
        } else {
            translog.add(stats.getTranslog());
        }
        if (requestCache == null) {
            if (stats.getRequestCache() != null) {
                requestCache = new RequestCacheStats();
                requestCache.add(stats.getRequestCache());
            }
        } else {
            requestCache.add(stats.getRequestCache());
        }
        if (recoveryStats == null) {
            if (stats.getRecoveryStats() != null) {
                recoveryStats = new RecoveryStats();
                recoveryStats.add(stats.getRecoveryStats());
            }
        } else {
            recoveryStats.add(stats.getRecoveryStats());
        }
    }

    @Nullable
    public DocsStats getDocs() {
        return this.docs;
    }

    @Nullable
    public StoreStats getStore() {
        return store;
    }

    @Nullable
    public IndexingStats getIndexing() {
        return indexing;
    }

    @Nullable
    public MergeStats getMerge() {
        return merge;
    }

    @Nullable
    public RefreshStats getRefresh() {
        return refresh;
    }

    @Nullable
    public FlushStats getFlush() {
        return flush;
    }

    @Nullable
    public WarmerStats getWarmer() {
        return this.warmer;
    }

    @Nullable
    public QueryCacheStats getQueryCache() {
        return this.queryCache;
    }

    @Nullable
    public FieldDataStats getFieldData() {
        return this.fieldData;
    }

    @Nullable
    public SegmentsStats getSegments() {
        return segments;
    }

    @Nullable
    public TranslogStats getTranslog() {
        return translog;
    }

    @Nullable
    public RequestCacheStats getRequestCache() {
        return requestCache;
    }

    @Nullable
    public RecoveryStats getRecoveryStats() {
        return recoveryStats;
    }

    /**
     * Utility method which computes total memory by adding
     * FieldData, PercolatorCache, Segments (memory, index writer, version map)
     */
    public ByteSizeValue getTotalMemory() {
        long size = 0;
        if (this.getFieldData() != null) {
            size += this.getFieldData().getMemorySizeInBytes();
        }
        if (this.getQueryCache() != null) {
            size += this.getQueryCache().getMemorySizeInBytes();
        }
        if (this.getSegments() != null) {
            size += this.getSegments().getMemoryInBytes() +
                    this.getSegments().getIndexWriterMemoryInBytes() +
                    this.getSegments().getVersionMapMemoryInBytes();
        }

        return new ByteSizeValue(size);
    }
}
