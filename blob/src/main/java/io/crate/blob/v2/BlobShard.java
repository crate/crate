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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.io.File;
import java.io.IOException;

public class BlobShard extends AbstractIndexShardComponent {

    private final BlobContainer blobContainer;
    private final IndexShard indexShard;

    @Inject
    protected BlobShard(ShardId shardId, @IndexSettings Settings indexSettings,
                        BlobEnvironment blobEnvironment,
                        IndexShard indexShard) {
        super(shardId, indexSettings);
        this.indexShard = indexShard;
        File blobDir = blobDir(blobEnvironment);
        logger.info("creating BlobContainer at {}", blobDir);
        this.blobContainer = new BlobContainer(blobDir);
    }

    public byte[][] currentDigests(byte prefix) {
        return blobContainer.cleanAndReturnDigests(prefix);
    }

    public boolean delete(String digest) {
        return blobContainer.getFile(digest).delete();
    }

    public BlobContainer blobContainer() {
        return blobContainer;
    }

    public ShardRouting shardRouting() {
        return indexShard.routingEntry();
    }

    public BlobStats blobStats() {
        final BlobStats stats = new BlobStats();

        stats.location(blobContainer().getBaseDirectory().getAbsolutePath());
        stats.availableSpace(blobContainer().getBaseDirectory().getFreeSpace());
        try {
            blobContainer().walkFiles(null, new BlobContainer.FileVisitor() {
                @Override
                public boolean visit(File file) {
                    stats.totalUsage(stats.totalUsage() + file.length());
                    stats.count(stats.count() + 1);
                    return true;
                }
            });
        } catch (IOException e) {
            logger.error("error getting blob stats", e);
            Throwables.propagate(e);
        }
        return stats;
    }

    private File blobDir(BlobEnvironment blobEnvironment) {
        if (indexSettings.get(BlobIndices.SETTING_INDEX_BLOBS_PATH) != null) {
            File blobPath = new File(indexSettings.get(BlobIndices.SETTING_INDEX_BLOBS_PATH));
            blobEnvironment.validateBlobsPath(blobPath);
            return blobEnvironment.shardLocation(shardId, blobPath);
        } else {
            return blobEnvironment.shardLocation(shardId);
        }
    }
}
