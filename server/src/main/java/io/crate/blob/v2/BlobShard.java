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

package io.crate.blob.v2;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardPath;
import org.jetbrains.annotations.Nullable;

import io.crate.blob.BlobContainer;

public class BlobShard {

    private static final String BLOBS_SUB_PATH = "blobs";

    private final BlobContainer blobContainer;
    private final IndexShard indexShard;
    private final Logger logger;
    private final Path blobDir;

    private long totalSize = 0;
    private long blobsCount = 0;

    public BlobShard(IndexShard indexShard, @Nullable Path globalBlobPath) {
        this.indexShard = indexShard;
        logger = Loggers.getLogger(BlobShard.class, indexShard.shardId());
        blobDir = resolveBlobDir(indexShard.indexSettings(), indexShard.shardPath(), globalBlobPath);
        logger.info("creating BlobContainer at {}", blobDir);
        this.blobContainer = new BlobContainer(blobDir);
    }

    void initialize() {
        try {
            blobContainer.visitBlobs(new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    blobsCount += 1;
                    long fileSize = attrs.size();
                    totalSize += fileSize;
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            logger.error("Unable to compute initial blob shard size and count", e);
            throw new RuntimeException(e);
        }
    }

    Path getBlobDir() {
        return blobDir;
    }

    public IndexShard indexShard() {
        return indexShard;
    }

    public byte[][] currentDigests(byte prefix) {
        return blobContainer.cleanAndReturnDigests(prefix);
    }

    public boolean delete(String digest) {
        try {
            Path blobPath = blobContainer.getFile(digest).toPath();
            long blobSize = 0;
            if (Files.exists(blobPath)) {
                blobSize = Files.size(blobPath);
            }
            boolean deleted = Files.deleteIfExists(blobPath);
            if (deleted) {
                decrementStats(blobSize);
            }
            return deleted;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void incrementStats(long size) {
        totalSize += size;
        blobsCount++;
    }

    private void decrementStats(long size) {
        totalSize -= size;
        blobsCount--;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getBlobsCount() {
        return blobsCount;
    }

    public BlobContainer blobContainer() {
        return blobContainer;
    }

    public ShardRouting shardRouting() {
        return indexShard.routingEntry();
    }

    void deleteShard() {
        Path baseDirectory = blobContainer.getBaseDirectory();
        try {
            IOUtils.rm(baseDirectory);
        } catch (IOException e) {
            logger.warn("Could not delete blob directory: {} {}", baseDirectory, e);
        }
    }

    private Path resolveBlobDir(IndexSettings indexSettings, ShardPath shardPath, @Nullable Path globalBlobPath) {
        String tableBlobPath = BlobIndicesService.SETTING_INDEX_BLOBS_PATH.get(indexSettings.getSettings());

        Path blobPath;
        if (Strings.isNullOrEmpty(tableBlobPath)) {
            if (globalBlobPath == null) {
                return shardPath.getDataPath().resolve(BLOBS_SUB_PATH);
            }
            assert BlobIndicesService.ensureExistsAndWritable(globalBlobPath) : "global blob path must exist and be writable";
            blobPath = globalBlobPath;
        } else {
            blobPath = PathUtils.get(tableBlobPath);
        }
        // rootDataPath is /<path.data>/<clusterName>/nodes/<nodeLock>/
        Path rootDataPath = shardPath.getRootDataPath();
        Path clusterDataDir = rootDataPath.getParent().getParent();
        Path pathToShard = clusterDataDir.relativize(shardPath.getShardStatePath());
        // this generates <blobs.path>/nodes/<nodeLock>/<path-to-shard>/blobs
        return blobPath.resolve(pathToShard).resolve(BLOBS_SUB_PATH);
    }
}
