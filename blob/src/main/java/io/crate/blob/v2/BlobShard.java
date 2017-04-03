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
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.*;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class BlobShard {

    private static final String BLOBS_SUB_PATH = "blobs";

    private final BlobContainer blobContainer;
    private final IndexShard indexShard;
    private final ESLogger logger;
    private long totalSize = 0;
    private long count = 0;
    private final Path blobDir;
    private Map<String, Long> deletedBlobAndSize = new ConcurrentHashMap<>();

    public BlobShard(IndexShard indexShard, @Nullable Path globalBlobPath, ThreadPool threadPool) {
        this.indexShard = indexShard;
        logger = Loggers.getLogger(BlobShard.class, indexShard.indexSettings(), indexShard.shardId());
        blobDir = getBlobDataDir(indexShard.indexSettings(), indexShard.shardPath(), globalBlobPath);
        logger.info("creating BlobContainer at {}", blobDir);
        this.blobContainer = new BlobContainer(blobDir);
        registerShardStatsCollector(threadPool);
    }

    private void registerShardStatsCollector(ThreadPool threadPool) {
        int attempt = 0;
        while (attempt < 5) {
            attempt++;
            try {
                threadPool.scheduleWithFixedDelay(
                    new ShardStatsCollector(), timeValueSeconds(5L), ThreadPool.Names.GENERIC
                );
                break;
            } catch (EsRejectedExecutionException e) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e1) {
                }
            }
        }

        if (attempt == 5) {
            logger.warn("Unable to register stats collector for shard {}", indexShard.shardId());
        }
    }

    /**
     * Watches for fs changes for the shard's blobs and incrementally computes the total size and count for
     * this blob shard.
     */
    private class ShardStatsCollector implements Runnable {

        private WatchService watchService;
        private final Map<WatchKey, Path> keys;

        public ShardStatsCollector() {
            keys = new HashMap<>();
        }

        @Override
        public void run() {
            if (watchService == null) {
                try {
                    watchService = FileSystems.getDefault().newWatchService();
                    calculateCurrentTotalSize(blobDir);
                } catch (IOException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            } else {
                processEvents();
            }
        }

        private void calculateCurrentTotalSize(Path blobDir) throws IOException {
            Files.walkFileTree(blobDir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    count += 1;
                    long fileSize = attrs.size();
                    totalSize += fileSize;
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    WatchKey key = dir.register(watchService, ENTRY_CREATE, ENTRY_DELETE);
                    keys.put(key, dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        void processEvents() {
            while (true) {
                WatchKey key;
                try {
                    key = watchService.poll(300, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    break;
                }

                if (key == null) {
                    break;
                }

                Path dir = keys.get(key);
                if (dir == null) {
                    continue;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind kind = event.kind();

                    if (kind == OVERFLOW) {
                        continue;
                    }

                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path name = ev.context();
                    Path child = dir.resolve(name);

                    if (kind == ENTRY_CREATE) {
                        count += 1;
                        try {
                            long childSize = Files.size(child);
                            totalSize += childSize;
                        } catch (IOException e) {
                            // totalSize might not be entirely accurate because of this, but don't want to propagate
                            // this failure
                        }
                    } else if (kind == ENTRY_DELETE) {
                        count -= 1;
                        Long blobSize = deletedBlobAndSize.get(child.toString());
                        if (blobSize != null) {
                            totalSize -= blobSize;
                            deletedBlobAndSize.remove(child.toString());
                        }
                    }

                    boolean valid = key.reset();
                    if (!valid) {
                        keys.remove(key);
                        if (keys.isEmpty()) {
                            break;
                        }
                    }
                }
            }
        }
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getCount() {
        return count;
    }

    public Path getBlobDir() {
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
                deletedBlobAndSize.put(blobPath.toString(), blobSize);
            }
            return deleted;
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

    void deleteShard() {
        Path baseDirectory = blobContainer.getBaseDirectory();
        try {
            IOUtils.rm(baseDirectory);
        } catch (IOException e) {
            logger.warn("Could not delete blob directory: {} {}", baseDirectory, e);
        }
    }

    private Path getBlobDataDir(Settings indexSettings, ShardPath shardPath, @Nullable Path globalBlobPath) {
        String tableBlobPath = indexSettings.get(BlobIndicesService.SETTING_INDEX_BLOBS_PATH);

        Path blobPath;
        if (tableBlobPath == null) {
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
