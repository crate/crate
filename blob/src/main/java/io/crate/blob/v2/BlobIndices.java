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

import io.crate.blob.BlobShardFuture;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;

public class BlobIndices extends AbstractComponent {

    public static final String SETTING_BLOBS_ENABLED = "index.blobs.enabled";

    private final IndicesService indicesService;
    private final IndicesLifecycle indicesLifecycle;

    @Inject
    public BlobIndices(Settings settings, IndicesService indicesService,
                       IndicesLifecycle indicesLifecycle) {
        super(settings);
        this.indicesService = indicesService;
        this.indicesLifecycle = indicesLifecycle;
    }

    public boolean blobsEnabled(String index) {
        return indicesService.indexServiceSafe(index)
                .settingsService().getSettings().getAsBoolean(SETTING_BLOBS_ENABLED,
                        false);
    }

    public boolean blobsEnabled(ShardId shardId) {
        return blobsEnabled(shardId.getIndex());
    }

    public BlobShard blobShardSafe(ShardId shardId) {
        return blobShardSafe(shardId.getIndex(), shardId.id());
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
}
