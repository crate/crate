/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.reference.sys.shard;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;
import org.jetbrains.annotations.Nullable;

import io.crate.blob.v2.BlobShard;
import io.crate.common.Suppliers;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class ShardRowContext {

    private final IndexShard indexShard;
    @Nullable
    private final BlobShard blobShard;
    private final ClusterService clusterService;
    private final Supplier<Long> sizeSupplier;
    private final IndexParts indexParts;
    private final String partitionIdent;
    private final int id;
    private final String path;
    @Nullable
    private final String blobPath;
    @Nullable
    private final String aliasName;
    @Nullable
    private final String templateName;

    public ShardRowContext(IndexShard indexShard, ClusterService clusterService) {
        this(indexShard, null, clusterService, Suppliers.memoizeWithExpiration(() -> {
            try {
                StoreStats storeStats = indexShard.storeStats();
                return storeStats.getSizeInBytes();
            } catch (AlreadyClosedException e) {
                return 0L;
            }
        }, 10, TimeUnit.SECONDS));
    }

    public ShardRowContext(BlobShard blobShard, ClusterService clusterService) {
        this(blobShard.indexShard(), blobShard, clusterService, blobShard::getTotalSize);
    }

    private ShardRowContext(IndexShard indexShard,
                            @Nullable BlobShard blobShard,
                            ClusterService clusterService,
                            Supplier<Long> sizeSupplier) {
        this.indexShard = indexShard;
        this.blobShard = blobShard;
        this.clusterService = clusterService;
        this.sizeSupplier = sizeSupplier;
        ShardId shardId = indexShard.shardId();
        String indexName = shardId.getIndexName();
        this.id = shardId.id();
        this.indexParts = new IndexParts(indexName);
        if (indexParts.isPartitioned()) {
            partitionIdent = indexParts.getPartitionIdent();
            RelationName relationName = indexParts.toRelationName();
            aliasName = relationName.indexNameOrAlias();
            templateName = PartitionName.templateName(relationName.schema(), relationName.name());
        } else {
            partitionIdent = "";
            aliasName = null;
            templateName = null;
        }
        path = indexShard.shardPath().getDataPath().toString();
        blobPath = blobShard == null ? null : blobShard.blobContainer().getBaseDirectory().toString();
    }

    public IndexShard indexShard() {
        return indexShard;
    }

    public ClusterService clusterService() {
        return clusterService;
    }

    public IndexParts indexParts() {
        return indexParts;
    }

    public Long size() {
        return sizeSupplier.get();
    }

    public String partitionIdent() {
        return partitionIdent;
    }

    public int id() {
        return id;
    }

    public String path() {
        return path;
    }

    @Nullable
    public String blobPath() {
        return blobPath;
    }

    @Nullable
    public Long numDocs() {
        if (blobShard == null) {
            try {
                return indexShard.docStats().getCount();
            } catch (IllegalIndexShardStateException e) {
                return null;
            }
        } else {
            return blobShard.getBlobsCount();
        }
    }

    public boolean isOrphanedPartition() {
        if (aliasName != null && templateName != null) {
            Metadata metadata = clusterService.state().metadata();
            return !(metadata.templates().containsKey(templateName) && metadata.hasConcreteIndex(aliasName));
        } else {
            return false;
        }
    }

    public boolean isClosed() {
        return indexShard.isClosed();
    }

    @Nullable
    public String minLuceneVersion() {
        long numDocs;
        try {
            numDocs = indexShard.docStats().getCount();
        } catch (IllegalIndexShardStateException e) {
            return null;
        }
        if (numDocs == 0) {
            // If there are no documents we've no segments and `indexShard.minimumCompatibleVersion`
            // will return the version the index was created with.
            // That would cause `TableNeedsUpgradeSysCheck` to trigger a warning
            //
            // If a new segment is created in an empty shard it will use the newer lucene version
            return Version.CURRENT.luceneVersion.toString();
        }
        try {
            return indexShard.minimumCompatibleVersion().toString();
        } catch (AlreadyClosedException e) {
            return null;
        }
    }

    @Nullable
    public Long maxSeqNo() {
        try {
            var stats = indexShard.seqNoStats();
            return stats == null ? null : stats.getMaxSeqNo();
        } catch (AlreadyClosedException e) {
            return 0L;
        }
    }

    @Nullable
    public Long localSeqNoCheckpoint() {
        try {
            var stats = indexShard.seqNoStats();
            return stats == null ? null : stats.getLocalCheckpoint();
        } catch (AlreadyClosedException e) {
            return 0L;
        }
    }

    @Nullable
    public Long globalSeqNoCheckpoint() {
        try {
            var stats = indexShard.seqNoStats();
            return stats == null ? null : stats.getGlobalCheckpoint();
        } catch (AlreadyClosedException e) {
            return 0L;
        }
    }

    @Nullable
    public Long translogSizeInBytes() {
        try {
            var stats = indexShard.translogStats();
            return stats == null ? null : stats.getTranslogSizeInBytes();
        } catch (AlreadyClosedException e) {
            return 0L;
        }
    }

    @Nullable
    public Long translogUncommittedSizeInBytes() {
        try {
            var stats = indexShard.translogStats();
            return stats == null ? null : stats.getUncommittedSizeInBytes();
        } catch (AlreadyClosedException e) {
            return 0L;
        }
    }

    @Nullable
    public Integer translogEstimatedNumberOfOperations() {
        try {
            var stats = indexShard.translogStats();
            return stats == null ? null : stats.estimatedNumberOfOperations();
        } catch (AlreadyClosedException e) {
            return 0;
        }
    }

    @Nullable
    public Integer translogUncommittedOperations() {
        try {
            var stats = indexShard.translogStats();
            return stats == null ? null : stats.getUncommittedOperations();
        } catch (AlreadyClosedException e) {
            return 0;
        }
    }

    @Nullable
    public String recoveryStage() {
        var recoveryState = indexShard.recoveryState();
        return recoveryState == null
            ? null
            : recoveryState.getStage().name();
    }

    @Nullable
    public String recoveryType() {
        var recoveryState = indexShard.recoveryState();
        return recoveryState == null
            ? null
            : recoveryState.getRecoverySource().getType().name();
    }

    @Nullable
    public Long recoveryTotalTime() {
        var recoveryState = indexShard.recoveryState();
        return recoveryState == null
            ? null
            : recoveryState.getTimer().time();
    }

    @Nullable
    public Long recoverySizeUsed() {
        var recoveryState = indexShard.recoveryState();
        return recoveryState == null
            ? null
            : recoveryState.getIndex().totalBytes();
    }

    @Nullable
    public Long recoverySizeReused() {
        var recoveryState = indexShard.recoveryState();
        return recoveryState == null
            ? null
            : recoveryState.getIndex().reusedBytes();
    }

    @Nullable
    public Long recoverySizeRecoveredBytes() {
        var recoveryState = indexShard.recoveryState();
        return recoveryState == null
            ? null
            : recoveryState.getIndex().recoveredBytes();
    }

    @Nullable
    public Float recoverySizeRecoveredBytesPercent() {
        var recoveryState = indexShard.recoveryState();
        return recoveryState == null
            ? null
            : recoveryState.getIndex().recoveredBytesPercent();
    }

    @Nullable
    public Integer recoveryFilesUsed() {
        var recoveryState = indexShard.recoveryState();
        return recoveryState == null
            ? null
            : recoveryState.getIndex().totalFileCount();
    }

    @Nullable
    public Integer recoveryFilesReused() {
        var recoveryState = indexShard.recoveryState();
        return recoveryState == null
            ? null
            : recoveryState.getIndex().reusedFileCount();
    }

    @Nullable
    public Integer recoveryFilesRecovered() {
        var recoveryState = indexShard.recoveryState();
        return recoveryState == null
            ? null
            : recoveryState.getIndex().recoveredFileCount();
    }

    @Nullable
    public Float recoveryFilesPercent() {
        var recoveryState = indexShard.recoveryState();
        return recoveryState == null
            ? null
            : recoveryState.getIndex().recoveredFilesPercent();
    }

    public Long retentionLeasesPrimaryTerm() {
        try {
            return indexShard.getRetentionLeaseStats().leases().primaryTerm();
        } catch (AlreadyClosedException | IndexShardClosedException e) {
            return null;
        }
    }

    public Long retentionLeasesVersion() {
        try {
            return indexShard.getRetentionLeaseStats().leases().version();
        } catch (AlreadyClosedException | IndexShardClosedException e) {
            return null;
        }
    }

    public Collection<RetentionLease> retentionLeases() {
        try {
            return indexShard.getRetentionLeaseStats().leases().leases();
        } catch (AlreadyClosedException | IndexShardClosedException e) {
            return List.of();
        }
    }

    public long flushCount() {
        return indexShard.getFlushMetric().count();
    }

    public long flushTotalTimeNs() {
        return indexShard.getFlushMetric().sum();
    }

    public long flushPeriodicCount() {
        return indexShard.periodicFlushCount();
    }
}
