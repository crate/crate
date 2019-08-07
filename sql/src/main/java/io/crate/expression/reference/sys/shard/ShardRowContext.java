/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.reference.sys.shard;

import com.google.common.base.Suppliers;
import io.crate.blob.v2.BlobShard;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
        this.id = shardId.getId();
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

    @Nullable
    BlobShard blobShard() {
        return blobShard;
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
    String aliasName() {
        return aliasName;
    }

    @Nullable
    public String templateName() {
        return templateName;
    }
}
