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
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.blob.BlobEnvironment;
import io.crate.blob.BlobShardFuture;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;

import java.io.File;
import java.io.IOException;

public class BlobIndices extends AbstractComponent implements ClusterStateListener {

    public static final String SETTING_INDEX_BLOBS_ENABLED = "index.blobs.enabled";
    public static final String SETTING_INDEX_BLOBS_PATH = "index.blobs.path";
    public static final String INDEX_PREFIX = ".blob_";

    private final Provider<TransportUpdateSettingsAction> transportUpdateSettingsActionProvider;
    private final Provider<TransportCreateIndexAction> transportCreateIndexActionProvider;
    private final Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider;
    private final IndicesService indicesService;
    private final IndicesLifecycle indicesLifecycle;
    private final BlobEnvironment blobEnvironment;

    public static final Predicate<String> indicesFilter = new Predicate<String>() {
        @Override
        public boolean apply(String indexName) {
            return isBlobIndex(indexName);
        }
    };

    public static final Function<String, String> STRIP_PREFIX = new Function<String, String>() {
        @Override
        public String apply(String indexName) {
            return indexName(indexName);
        }
    };

    @Inject
    public BlobIndices(Settings settings,
                       Provider<TransportCreateIndexAction> transportCreateIndexActionProvider,
                       Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider,
                       Provider<TransportUpdateSettingsAction> transportUpdateSettingsActionProvider,
                       IndicesService indicesService,
                       IndicesLifecycle indicesLifecycle,
                       BlobEnvironment blobEnvironment,
                       ClusterService clusterService) {
        super(settings);
        this.transportCreateIndexActionProvider = transportCreateIndexActionProvider;
        this.transportDeleteIndexActionProvider = transportDeleteIndexActionProvider;
        this.transportUpdateSettingsActionProvider = transportUpdateSettingsActionProvider;
        this.indicesService = indicesService;
        this.indicesLifecycle = indicesLifecycle;
        this.blobEnvironment = blobEnvironment;
        clusterService.addFirst(this);
        logger.setLevel("debug");
    }

    public BlobShard blobShardSafe(ShardId shardId) {
        return blobShardSafe(shardId.getIndex(), shardId.id());
    }

    /**
     * can be used to alter the number of replicas.
     *
     * @param tableName name of the blob table
     * @param indexSettings updated index settings
     */
    public ListenableFuture<Void> alterBlobTable(String tableName, Settings indexSettings) {
        final SettableFuture<Void> result = SettableFuture.create();
        transportUpdateSettingsActionProvider.get().execute(
                new UpdateSettingsRequest(indexSettings, fullIndexName(tableName)),
                new ActionListener<UpdateSettingsResponse>() {
                    @Override
                    public void onResponse(UpdateSettingsResponse updateSettingsResponse) {
                        result.set(null);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        result.setException(e);

                    }
                });
        return result;
    }

    public ListenableFuture<Void> createBlobTable(String tableName,
                                                  Settings indexSettings) {
        Settings.Builder builder = Settings.builder();
        builder.put(indexSettings);
        builder.put(SETTING_INDEX_BLOBS_ENABLED, true);

        final SettableFuture<Void> result = SettableFuture.create();
        transportCreateIndexActionProvider.get().execute(new CreateIndexRequest(fullIndexName(tableName), builder.build()), new ActionListener<CreateIndexResponse>() {
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

    public ListenableFuture<Void> dropBlobTable(final String tableName) {
        final SettableFuture<Void> result = SettableFuture.create();
        transportDeleteIndexActionProvider.get().execute(new DeleteIndexRequest(fullIndexName(tableName)), new ActionListener<DeleteIndexResponse>() {
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
            try {
                Injector injector = indexService.shardInjectorSafe(shardId);
                return injector.getInstance(BlobShard.class);
            } catch (ShardNotFoundException e) {
                return null;
            }
        }
        return null;
    }

    public BlobShard blobShardSafe(String index, int shardId) {
        if (isBlobIndex(index)) {
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

    /**
     * check if this index is a blob table
     *
     * This only works for indices that were created via SQL.
     */
    public static boolean isBlobIndex(String indexName) {
        return indexName.startsWith(INDEX_PREFIX);
    }

    /**
     * check if given shard is part of an index that is a blob table
     *
     * This only works for indices that were created via SQL.
     */
    public static boolean isBlobShard(ShardId shardId) {
        return isBlobIndex(shardId.getIndex());
    }

    /**
     * Returns the full index name, adds blob index prefix.
     */
    public static String fullIndexName(String indexName) {
        if (isBlobIndex(indexName)) {
            return indexName;
        }
        return INDEX_PREFIX + indexName;
    }

    /**
     * Strips the blob index prefix from a full index name
     */
    public static String indexName(String indexName) {
        if (!isBlobIndex(indexName)) {
            return indexName;
        }
        return indexName.substring(INDEX_PREFIX.length());
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().disableStatePersistence()) {
            return;
        }
        MetaData currentMetaData = event.previousState().metaData();
        // only delete indices when we already received a state
        if (currentMetaData == null) {
            return;
        }
        removeIndexLocationsForDeletedIndices(event, currentMetaData);
    }

    private void removeIndexLocationsForDeletedIndices(ClusterChangedEvent event, MetaData currentMetaData) {
        MetaData newMetaData = event.state().metaData();
        for (IndexMetaData current : currentMetaData) {
            String index = current.getIndex();
            if (!newMetaData.hasIndex(index) && isBlobIndex(index)) {
                deleteBlobIndexLocation(current, index);
            }
        }
    }

    private void deleteBlobIndexLocation(IndexMetaData current, String index) {
        File indexLocation = null;
        File customBlobsPath = null;
        if (current.getSettings().get(BlobIndices.SETTING_INDEX_BLOBS_PATH) != null) {
            customBlobsPath = new File(current.getSettings().get(BlobIndices.SETTING_INDEX_BLOBS_PATH));
            indexLocation = blobEnvironment.indexLocation(new Index(index), customBlobsPath);
        } else if (blobEnvironment.blobsPath() != null) {
            indexLocation = blobEnvironment.indexLocation(new Index(index));
        }

        if (indexLocation == null) {
            // default shard location - ES logic deletes everything in this case
            return;
        }

        String absolutePath = indexLocation.getAbsolutePath();
        if (indexLocation.exists()) {
            logger.debug("[{}] Deleting blob index directory '{}'", index, absolutePath);
            try {
                IOUtils.rm(indexLocation.toPath());
            } catch (IOException e) {
                logger.warn("Could not delete blob index directory {}", absolutePath);
            }
        } else {
            logger.warn("wanted to delete blob index directory {} but it was already gone", absolutePath);
        }

        // check if custom index blobs path is empty, if so delete whole path
        if (customBlobsPath != null && blobEnvironment.isCustomBlobPathEmpty(customBlobsPath)) {
            logger.debug("[{}] Empty per table defined blobs path found, deleting leftover folders inside {}",
                    index, customBlobsPath.getAbsolutePath());
            try {
                FileSystemUtils.deleteSubDirectories(customBlobsPath.toPath());
            } catch (IOException e) {
                logger.warn("Could not delete custom blob path {}", customBlobsPath.getAbsolutePath());
            }
        }
    }
}
