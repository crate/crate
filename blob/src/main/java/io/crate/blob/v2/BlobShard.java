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

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.*;

public class BlobShard {

    private static final String BLOBS_SUB_PATH = "blobs";

    private final BlobContainer blobContainer;
    private final IndexShard indexShard;
    private final ESLogger logger;
    private long totalSize = 0;
    private long count = 0;
    private final Path blobDir;

    public BlobShard(IndexShard indexShard, @Nullable Path globalBlobPath, ScheduledExecutorService scheduler) {
        this.indexShard = indexShard;
        logger = Loggers.getLogger(BlobShard.class, indexShard.indexSettings(), indexShard.shardId());
        blobDir = getBlobDataDir(indexShard.indexSettings(), indexShard.shardPath(), globalBlobPath);
        logger.info("creating BlobContainer at {}", blobDir);
        this.blobContainer = new BlobContainer(blobDir);
        registerShardStatsCollector(scheduler);
    }

    private void registerShardStatsCollector(ScheduledExecutorService scheduler) {
        int attempt = 0;
        while (attempt < 5) {
            attempt++;
            try {
                scheduler.scheduleWithFixedDelay(new ShardStatsCollector(), 1L, 5L, TimeUnit.SECONDS);
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

        private final WatchService watchService;
        // todo do we really need this keys map?
        private final Map<WatchKey, Path> keys;
        // Map of each blob folder and its size. The sum of all the values in this map should equal the total size of
        // the blob shard
        private final Map<Path, Long> dirsAndSize;

        public ShardStatsCollector() {
            keys = new HashMap<>();
            dirsAndSize = new HashMap<>();
            try {
                watchService = FileSystems.getDefault().newWatchService();
                registerDirectoryAndChildren(blobDir);
            } catch (IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        private void registerDirectoryAndChildren(Path blobDir) throws IOException {
            Files.walkFileTree(blobDir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (dir.equals(blobContainer.getTmpDirectory())) {
                        // ignore the tmp directory as it just generates noise
                        return FileVisitResult.CONTINUE;
                    }

                    WatchKey key = dir.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
                    keys.put(key, dir);
                    dirsAndSize.put(dir, 0L);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    count += 1;
                    long fileSize = attrs.size();
                    totalSize += fileSize;
                    dirsAndSize.compute(file.getParent(), (s, currentDirSize) -> currentDirSize + fileSize);
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        @Override
        public void run() {
            processEvents();
        }

        void processEvents() {
            // todo: maybe this loop needs to be smarter as a dir with many changes can run for a long time
            // todo:have reductions number maybe ?!
            Set<Path> dirsWithRemovedFiles = new HashSet<>();
            Map<Path, Long> dirToAddedFilesSize = new HashMap<>();
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
                            // increment size for this folder
                            dirToAddedFilesSize.compute(dir, (s, currentDirSize) -> {
                                if (currentDirSize == null) {
                                    return childSize;
                                } else {
                                    return currentDirSize + childSize;
                                }
                            });
                        } catch (IOException e) {
                            // totalSize might not be entirely accurate because of this, but don't want to propagate
                            // this failure
                        }
                    } else if (kind == ENTRY_DELETE) {
                        count -= 1;
                        // mark folder as having removed files
                        dirsWithRemovedFiles.add(dir);
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

            for (Map.Entry<Path, Long> pathLongEntry : dirToAddedFilesSize.entrySet()) {
                Path dir = pathLongEntry.getKey();
                if (dirsWithRemovedFiles.contains(dir) == false) {
                    // items were not removed from this folder so just add the folder's added files size to total
                    Long addedFilesSize = pathLongEntry.getValue();
                    totalSize += addedFilesSize;
                    dirsAndSize.compute(dir, (s, currentDirSize) -> currentDirSize + addedFilesSize);
                } else {
                    updateTotalForDirWithDeletions(dir);
                    dirsWithRemovedFiles.remove(dir);
                }
            }

            for (Path pathWithDeletedFiles : dirsWithRemovedFiles) {
                updateTotalForDirWithDeletions(pathWithDeletedFiles);
            }
        }

        private void updateTotalForDirWithDeletions(Path dir) {
            try {
                long recomputedSize = computeDirSize(dir);
                Long previousSize = dirsAndSize.get(dir);
                totalSize -= previousSize;
                totalSize += recomputedSize;
                dirsAndSize.put(dir, recomputedSize);
            } catch (IOException e) {
                // cannot "walk" the folder, size will be bigger than actual but will be recalculated on next
                // delete operation
            }
        }

        private long computeDirSize(Path dir) throws IOException {
            return Files.walk(dir).mapToLong(p -> {
                if (p.equals(dir)) {
                    // ignore current "."
                    return 0;
                }
                return p.toFile().length();
            }).sum();
        }
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getCount() {
        return count;
    }

    public IndexShard indexShard() {
        return indexShard;
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
