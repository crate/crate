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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.FutureActionListener;
import io.crate.blob.BlobShardFuture;
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
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesLifecycle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BlobIndicesService extends AbstractLifecycleComponent<BlobIndicesService> {

    public static final String SETTING_INDEX_BLOBS_ENABLED = "index.blobs.enabled";
    public static final String SETTING_INDEX_BLOBS_PATH = "index.blobs.path";
    public static final String SETTING_BLOBS_PATH = "blobs.path";
    private static final String INDEX_PREFIX = ".blob_";

    private final Provider<TransportUpdateSettingsAction> transportUpdateSettingsActionProvider;
    private final Provider<TransportCreateIndexAction> transportCreateIndexActionProvider;
    private final Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider;
    private final ClusterService clusterService;
    private final IndicesLifecycle indicesLifecycle;

    @VisibleForTesting
    final Map<String, BlobIndex> indices = new ConcurrentHashMap<>();

    public static final Predicate<String> indicesFilter = BlobIndicesService::isBlobIndex;
    public static final Function<String, String> STRIP_PREFIX = BlobIndicesService::indexName;
    private final LifecycleListener listener;

    @Nullable
    private final Path globalBlobPath;

    @Inject
    public BlobIndicesService(Settings settings,
                              Provider<TransportCreateIndexAction> transportCreateIndexActionProvider,
                              Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider,
                              Provider<TransportUpdateSettingsAction> transportUpdateSettingsActionProvider,
                              ClusterService clusterService,
                              IndicesLifecycle indicesLifecycle) {
        super(settings);
        this.transportCreateIndexActionProvider = transportCreateIndexActionProvider;
        this.transportDeleteIndexActionProvider = transportDeleteIndexActionProvider;
        this.transportUpdateSettingsActionProvider = transportUpdateSettingsActionProvider;
        this.clusterService = clusterService;
        this.indicesLifecycle = indicesLifecycle;
        this.listener = new LifecycleListener();
        globalBlobPath = getGlobalBlobPath(settings);
        logger.setLevel("debug");
    }

    @Nullable
    public static Path getGlobalBlobPath(Settings settings) {
        String customGlobalBlobPathSetting = settings.get(SETTING_BLOBS_PATH);
        if (customGlobalBlobPathSetting == null) {
            return null;
        }
        Path globalBlobPath = PathUtils.get(customGlobalBlobPathSetting);
        ensureExistsAndWritable(globalBlobPath);
        return globalBlobPath;
    }

    @Override
    protected void doStart() {
        // add listener here to avoid guice proxy errors if the ClusterService could not be build
        indicesLifecycle.addListener(listener);
    }

    @Override
    protected void doStop() {
        indicesLifecycle.removeListener(listener);
    }

    @Override
    protected void doClose() {
    }

    public BlobShard blobShardSafe(ShardId shardId) {
        return blobShardSafe(shardId.getIndex(), shardId.id());
    }

    private class LifecycleListener extends IndicesLifecycle.Listener {

        @Override
        public void afterIndexCreated(IndexService indexService) {
            String indexName = indexService.index().getName();
            if (isBlobIndex(indexName)) {
                BlobIndex oldBlobIndex = indices.put(indexName, new BlobIndex(globalBlobPath));
                assert oldBlobIndex == null : "There must not be an index present if a new index is created";
            }
        }

        @Override
        public void afterIndexClosed(Index index, Settings indexSettings) {
            String indexName = index.getName();
            if (isBlobIndex(indexName)) {
                BlobIndex blobIndex = indices.remove(indexName);
                assert blobIndex != null : "BlobIndex not found on afterIndexDeleted";
            }
        }

        @Override
        public void afterIndexShardCreated(IndexShard indexShard) {
            String index = indexShard.indexService().index().getName();
            if (isBlobIndex(index)) {
                BlobIndex blobIndex = indices.get(index);
                assert blobIndex != null : "blobIndex must exists if a shard is created in it";
                blobIndex.createShard(indexShard);
            }
        }

        @Override
        public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
            String index = shardId.getIndex();
            if (isBlobIndex(index)) {
                BlobIndex blobIndex = indices.get(index);
                assert blobIndex != null : "blobIndex must exists if a shard is deleted from it";
                blobIndex.removeShard(shardId);
            }
        }
    }

    /**
     * can be used to alter the number of replicas.
     *
     * @param tableName     name of the blob table
     * @param indexSettings updated index settings
     */
    public ListenableFuture<Void> alterBlobTable(String tableName, Settings indexSettings) {
        FutureActionListener<UpdateSettingsResponse, Void> listener =
            new FutureActionListener<>(Functions.<Void>constant(null));

        transportUpdateSettingsActionProvider.get().execute(
            new UpdateSettingsRequest(indexSettings, fullIndexName(tableName)), listener);
        return listener;
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
        FutureActionListener<DeleteIndexResponse, Void> listener = new FutureActionListener<>(Functions.<Void>constant(null));
        transportDeleteIndexActionProvider.get().execute(new DeleteIndexRequest(fullIndexName(tableName)), listener);
        return listener;
    }

    @Nullable
    public BlobShard blobShard(String index, int shardId) {
        BlobIndex blobIndex = indices.get(index);
        if (blobIndex == null) {
            return null;
        }
        return blobIndex.getShard(shardId);
    }

    public BlobShard blobShardSafe(String index, int shardId) {
        if (isBlobIndex(index)) {
            BlobShard blobShard = blobShard(index, shardId);
            if (blobShard == null) {
                throw new ShardNotFoundException(new ShardId(index, shardId));
            }
            return blobShard;
        }
        throw new BlobsDisabledException(index);
    }


    public BlobShard localBlobShard(String index, String digest) {
        return blobShardSafe(localShardId(index, digest));
    }

    private ShardId localShardId(String index, String digest) {
        ShardIterator si = clusterService.operationRouting().getShards(
            clusterService.state(), index, null, null, digest, "_only_local");
        return si.shardId();
    }

    public BlobShardFuture blobShardFuture(String index, int shardId) {
        return new BlobShardFuture(this, indicesLifecycle, index, shardId);

    }

    /**
     * check if this index is a blob table
     * <p>
     * This only works for indices that were created via SQL.
     */
    public static boolean isBlobIndex(String indexName) {
        return indexName.startsWith(INDEX_PREFIX);
    }

    /**
     * check if given shard is part of an index that is a blob table
     * <p>
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

    static boolean ensureExistsAndWritable(Path blobsPath) {
        if (Files.exists(blobsPath)) {
            if (!Files.isDirectory(blobsPath)) {
                throw new SettingsException(
                    String.format(Locale.ENGLISH, "blobs path '%s' is a file, must be a directory", blobsPath));
            }
            if (!Files.isWritable(blobsPath)) {
                throw new SettingsException(
                    String.format(Locale.ENGLISH, "blobs path '%s' is not writable", blobsPath));
            }
        } else {
            try {
                Files.createDirectories(blobsPath);
            } catch (IOException e) {
                throw new SettingsException(
                    String.format(Locale.ENGLISH, "blobs path '%s' could not be created", blobsPath));
            }
        }
        return true;
    }
}
