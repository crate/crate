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
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.jetbrains.annotations.Nullable;

import io.crate.blob.v2.BlobShard;
import io.crate.common.Suppliers;
import io.crate.metadata.IndexName;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public record ShardRowContext(Long size,
                              RelationName relationName,
                              List<String> partitionValues,
                              @Nullable String partitionIdent,
                              String partitionUUID,
                              int id,
                              String path,
                              @Nullable String blobPath,
                              boolean primary,
                              String state,
                              String routingState,
                              String nodeId,
                              String nodeName,
                              String relocatingNodeId,
                              boolean isOrphanedPartition,
                              @Nullable Long numDocs,
                              boolean isClosed,
                              @Nullable String minLuceneVersion,
                              long maxSeqNo,
                              long localSeqNoCheckpoint,
                              long globalSeqNoCheckpoint,
                              @Nullable Long lastWriteTimestamp,
                              @Nullable Long translogSizeInBytes,
                              @Nullable Long translogUncommittedSizeInBytes,
                              @Nullable Integer translogEstimatedNumberOfOperations,
                              @Nullable Integer translogUncommittedOperations,
                              @Nullable String recoveryStage,
                              @Nullable String recoveryType,
                              @Nullable Long recoveryTotalTime,
                              @Nullable Long recoverySizeUsed,
                              @Nullable Long recoverySizeReused,
                              @Nullable Long recoverySizeRecoveredBytes,
                              @Nullable Float recoverySizeRecoveredBytesPercent,
                              @Nullable Integer recoveryFilesUsed,
                              @Nullable Integer recoveryFilesReused,
                              @Nullable Integer recoveryFilesRecovered,
                              @Nullable Float recoveryFilesPercent,
                              @Nullable Long retentionLeasesPrimaryTerm,
                              @Nullable Long retentionLeasesVersion,
                              Collection<RetentionLease> retentionLeases,
                              long flushCount,
                              long flushTotalTimeNs,
                              long flushPeriodicCount) {

    public static ShardRowContext.Builder builder(IndexShard indexShard, ClusterService clusterService) {
        return new Builder(indexShard, null, clusterService, Suppliers.memoizeWithExpiration(() -> {
            try {
                StoreStats storeStats = indexShard.storeStats();
                return storeStats.sizeInBytes();
            } catch (AlreadyClosedException e) {
                return 0L;
            }
        }, 10, TimeUnit.SECONDS));
    }

    public static ShardRowContext.Builder builder(BlobShard blobShard, ClusterService clusterService) {
        return new Builder(blobShard.indexShard(), blobShard, clusterService, blobShard::getTotalSize);
    }


    public static class Builder {

        private final Supplier<Long> sizeSupplier;

        private final RelationName relationName;
        private final IndexShard indexShard;
        private final String nodeId;
        private final String nodeName;
        @Nullable
        private final BlobShard blobShard;
        private final String partitionIdent;
        private final List<String> partitionValues;
        private final int id;
        private final String path;
        @Nullable
        private final String blobPath;
        private final boolean orphanedPartition;

        private Builder(IndexShard indexShard,
                        @Nullable BlobShard blobShard,
                        ClusterService clusterService,
                        Supplier<Long> sizeSupplier) {
            this.indexShard = indexShard;
            this.nodeId = clusterService.localNode().getId();
            this.nodeName = clusterService.localNode().getName();
            this.blobShard = blobShard;
            this.sizeSupplier = sizeSupplier;
            ShardId shardId = indexShard.shardId();
            this.id = shardId.id();
            Metadata metadata = clusterService.state().metadata();
            IndexMetadata index = metadata.index(shardId.getIndexUUID());
            if (index == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "IndexMetadata for shard %s does not exist in the cluster state", shardId));
            }
            @Nullable
            RelationMetadata relation = metadata.getRelation(shardId.getIndexUUID());
            if (relation == null) {
                orphanedPartition = true;
                relationName = IndexName.decode(shardId.getIndexName()).toRelationName();
            } else {
                orphanedPartition = false;
                relationName = relation.name();
            }
            partitionValues = index.partitionValues();
            partitionIdent = partitionValues.isEmpty() ? "" : PartitionName.encodeIdent(index.partitionValues());
            path = indexShard.shardPath().getDataPath().toString();
            blobPath = blobShard == null ? null : blobShard.blobContainer().getBaseDirectory().toString();
        }

        public Index index() {
            return indexShard.shardId().getIndex();
        }

        public RelationName relationName() {
            return relationName;
        }

        public List<String> partitionValues() {
            return partitionValues;
        }

        public ShardRowContext build() {
            Long numDocs = null;
            SeqNoStats seqNoStats = null;
            TranslogStats translogStats = null;
            RetentionLeaseStats retentionLeaseStats = null;

            try {
                DocsStats docsStats = indexShard.docStats();
                numDocs = docsStats.getCount();
                seqNoStats = indexShard.seqNoStats();
                translogStats = indexShard.translogStats();
                retentionLeaseStats = indexShard.getRetentionLeaseStats();
            } catch (AlreadyClosedException | IndexShardClosedException _) {
            }

            RecoveryState recoveryState = indexShard.recoveryState();

            return new ShardRowContext(
                sizeSupplier.get(),
                relationName,
                partitionValues,
                partitionIdent,
                indexShard.shardId().getIndex().getUUID(),
                id,
                path,
                blobPath,
                indexShard.routingEntry().primary(),
                indexShard.state().name(),
                indexShard.routingEntry().state().name(),
                nodeId,
                nodeName,
                indexShard.routingEntry().relocatingNodeId(),
                orphanedPartition,
                blobShard != null ? Long.valueOf(blobShard.getBlobsCount()) : numDocs,
                indexShard.isClosed(),
                minLuceneVersion(numDocs),
                seqNoStats != null ? seqNoStats.getMaxSeqNo() : 0L,
                seqNoStats != null ? seqNoStats.getLocalCheckpoint() : 0L,
                seqNoStats != null ? seqNoStats.getGlobalCheckpoint() : 0L,
                indexShard.lastWriteTimestamp(),
                translogStats != null ? translogStats.getTranslogSizeInBytes() : 0L,
                translogStats != null ? translogStats.getUncommittedSizeInBytes() : 0L,
                translogStats != null ? translogStats.estimatedNumberOfOperations() : 0,
                translogStats != null ? translogStats.getUncommittedOperations() : 0,
                recoveryState != null ? recoveryState.getStage().name() : null,
                recoveryState != null ? recoveryState.getRecoverySource().getType().name() : null,
                recoveryState != null ? recoveryState.getTimer().time() : null,
                recoveryState != null ? recoveryState.getIndex().totalBytes() : null,
                recoveryState != null ? recoveryState.getIndex().reusedBytes() : null,
                recoveryState != null ? recoveryState.getIndex().recoveredBytes() : null,
                recoveryState != null ? recoveryState.getIndex().recoveredBytesPercent() : null,
                recoveryState != null ? recoveryState.getIndex().totalFileCount() : null,
                recoveryState != null ? recoveryState.getIndex().reusedFileCount() : null,
                recoveryState != null ? recoveryState.getIndex().recoveredFileCount() : null,
                recoveryState != null ? recoveryState.getIndex().recoveredFilesPercent() : null,
                retentionLeaseStats != null ? retentionLeaseStats.leases().primaryTerm() : null,
                retentionLeaseStats != null ? retentionLeaseStats.leases().version() : null,
                retentionLeaseStats != null ? retentionLeaseStats.leases().leases() : List.of(),
                indexShard.getFlushMetric().count(),
                indexShard.getFlushMetric().sum(),
                indexShard.periodicFlushCount()
            );
        }

        @Nullable
        private String minLuceneVersion(@Nullable Long numDocs) {
            if (numDocs == null) {
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
    }
}
