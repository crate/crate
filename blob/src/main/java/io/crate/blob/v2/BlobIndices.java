/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.blob.v2;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.blob.BlobShardFuture;
import io.crate.core.NumberOfReplicas;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;

import java.util.List;

public class BlobIndices extends AbstractComponent {

    public static final String SETTING_BLOBS_ENABLED = "index.blobs.enabled";
    public static final String INDEX_PREFIX = ".blob_";
    private static final int DEFAULT_NUMBER_OF_SHARDS = 5;
    private static final int DEFAULT_NUMBER_OF_REPLICAS = 1;

    private final TransportCreateIndexAction transportCreateIndexAction;
    private final TransportDeleteIndexAction transportDeleteIndexAction;
    private final IndicesService indicesService;
    private final IndicesLifecycle indicesLifecycle;

    public static final Predicate<String> indicesFilter = new Predicate<String>() {
        @Override
        public boolean apply(String indexName) {
            return indexName.startsWith(INDEX_PREFIX);
        }
    };

    public static final Function<String, String> stripPrefix = new Function<String, String>() {
        @Override
        public String apply(String indexName) {
            return indexName.substring(BlobIndices.INDEX_PREFIX.length());
        }
    };

    @Inject
    public BlobIndices(Settings settings,
                       TransportCreateIndexAction transportCreateIndexAction,
                       TransportDeleteIndexAction transportDeleteIndexAction,
                       IndicesService indicesService,
                       IndicesLifecycle indicesLifecycle) {
        super(settings);
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.transportDeleteIndexAction = transportDeleteIndexAction;
        this.indicesService = indicesService;
        this.indicesLifecycle = indicesLifecycle;
    }

    /**
     * check if this index is a blob table
     *
     * This only works for indices that were created via SQL.
     */
    public static boolean isBlobShard(String index) {
        return index.startsWith(INDEX_PREFIX);
    }

    /**
     * check if this shards is part of an index that is a blob table
     *
     * This only works for indices that were created via SQL.
     */
    public static boolean isBlobShard(ShardId shardId) {
        return isBlobShard(shardId.getIndex());
    }

    public BlobShard blobShardSafe(ShardId shardId) {
        return blobShardSafe(shardId.getIndex(), shardId.id());
    }

    /**
     * check if this index is a blob index
     *
     * this method is deprecated.
     * Will be removed when being blob table is determined by index name.
     */
    @Deprecated
    public boolean blobsEnabled(String index) {
        return indicesService.indexServiceSafe(index)
                .settingsService().getSettings().getAsBoolean(SETTING_BLOBS_ENABLED, false);
    }

    @Deprecated
    public boolean blobsEnabled(ShardId shardId) {
        return blobsEnabled(shardId.getIndex());
    }

    public ListenableFuture<Void> createBlobTable(String tableName,
                                                  @Nullable NumberOfReplicas numberOfReplicas,
                                                  @Nullable Integer numberOfShards ) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        builder.put(SETTING_BLOBS_ENABLED, true);
        builder.put("number_of_shards", Objects.firstNonNull(numberOfShards, DEFAULT_NUMBER_OF_SHARDS));
        if (numberOfReplicas == null) {
            builder.put("number_of_replicas", DEFAULT_NUMBER_OF_REPLICAS);
        } else {
            builder.put(numberOfReplicas.esSettingKey(), numberOfReplicas.esSettingValue());
        }

        tableName = INDEX_PREFIX + tableName;
        final SettableFuture<Void> result = SettableFuture.create();
        transportCreateIndexAction.execute(new CreateIndexRequest(tableName, builder.build()), new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                assert createIndexResponse.isAcknowledged();
                result.set(null);
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });
        return result;
    }

    public ListenableFuture<Void> dropBlobTable(String tableName) {
        final SettableFuture<Void> result = SettableFuture.create();
        transportDeleteIndexAction.execute(new DeleteIndexRequest(INDEX_PREFIX + tableName), new ActionListener<DeleteIndexResponse>() {
            @Override
            public void onResponse(DeleteIndexResponse deleteIndexResponse) {
                result.set(null);
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });
        return result;
    }

    public BlobShard blobShard(String index, int shardId) {
        IndexService indexService = indicesService.indexService(index);
        if (indexService != null) {
            Injector injector = indexService.shardInjector(shardId);
            if (injector != null) {
                return injector.getInstance(BlobShard.class);
            }
        }

        return null;
    }

    public BlobShard blobShardSafe(String index, int shardId) {
        if (blobsEnabled(index)) {
            return indicesService.indexServiceSafe(index).shardInjectorSafe(shardId).getInstance(BlobShard.class);
        }
        throw new BlobsDisabledException(index);
    }

    public BlobIndex blobIndex(String index) {
        return indicesService.indexServiceSafe(index).injector().getInstance(BlobIndex.class);
    }

    public ShardId localShardId(String index, String digest) {
        return blobIndex(index).shardId(digest);
    }

    public BlobShard localBlobShard(String index, String digest) {
        return blobShardSafe(localShardId(index, digest));
    }

    public BlobShardFuture blobShardFuture(String index, int shardId) {
        return new BlobShardFuture(this, indicesLifecycle, index, shardId);

    }

    public List<String> indices() {
        return FluentIterable.from(indicesService.indices())
                .filter(indicesFilter).toList();
    }

}
