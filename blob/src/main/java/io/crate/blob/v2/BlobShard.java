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

import com.google.common.base.Throwables;
import io.crate.blob.BlobContainer;
import io.crate.blob.BlobEnvironment;
import io.crate.blob.stats.BlobStats;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class BlobShard extends AbstractIndexShardComponent {

    private final BlobContainer blobContainer;
    private final IndexShard indexShard;

    @Inject
    public BlobShard(ShardId shardId,
                     IndexSettingsService indexSettingsService,
                     BlobEnvironment blobEnvironment,
                     IndexShard indexShard) {
        super(shardId, indexSettingsService.getSettings());
        this.indexShard = indexShard;
        Path blobDir = blobDir(blobEnvironment);
        logger.info("creating BlobContainer at {}", blobDir);
        this.blobContainer = new BlobContainer(blobDir);
    }

    public byte[][] currentDigests(byte prefix) {
        return blobContainer.cleanAndReturnDigests(prefix);
    }

    public boolean delete(String digest) {
        try {
            return Files.deleteIfExists(blobContainer.getFile(digest).toPath());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public BlobContainer blobContainer() {
        return blobContainer;
    }

    public ShardRouting shardRouting() {
        return indexShard.routingEntry();
    }

    public BlobStats blobStats() {
        final BlobStats stats = new BlobStats();
        stats.location(blobContainer().getBaseDirectory().toString());
        for (File file : blobContainer().getFiles()) {
            stats.totalUsage(stats.totalUsage() + file.length());
            stats.count(stats.count() + 1);
        }
        return stats;
    }

    private Path blobDir(BlobEnvironment blobEnvironment) {
        String customBlobPath = indexSettings.get(BlobIndicesService.SETTING_INDEX_BLOBS_PATH);
        if (customBlobPath != null) {
            Path blobPath = PathUtils.get(customBlobPath);
            BlobEnvironment.ensureExistsAndWritable(blobPath);
            return blobEnvironment.shardLocation(shardId, blobPath);
        } else {
            return blobEnvironment.shardLocation(shardId);
        }
    }
}
