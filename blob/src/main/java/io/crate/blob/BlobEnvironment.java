/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.blob;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Locale;
import java.util.stream.Stream;

public class BlobEnvironment {

    public static final String SETTING_BLOBS_PATH = "blobs.path";
    public static final String BLOBS_SUB_PATH = "blobs";

    private final NodeEnvironment nodeEnvironment;

    @Nullable
    private final Path blobsPath;

    @Inject
    public BlobEnvironment(Settings settings, NodeEnvironment nodeEnvironment) {
        this.nodeEnvironment = nodeEnvironment;
        String customBlobsPathRoot = settings.get(SETTING_BLOBS_PATH);
        if (customBlobsPathRoot != null) {
            blobsPath = PathUtils.get(customBlobsPathRoot);
            ensureExistsAndWritable(blobsPath);
        } else {
            blobsPath = null;
        }
    }

    @Nullable
    public Path blobsPath() {
        return blobsPath;
    }

    /**
     * Return the index location respecting global blobs data path value
     */
    public Path indexLocation(Index index) {
        if (blobsPath == null) {
            return nodeEnvironment.indexPaths(index)[0];
        }
        return indexLocation(index, blobsPath);
    }

    /**
     * Return the index location according to the given base path
     */
    public Path indexLocation(Index index, Path path) {
        Path indexLocation = nodeEnvironment.indexPaths(index)[0];
        Path dataPath = nodeEnvironment.nodeDataPaths()[0];
        return path.resolve(dataPath.relativize(indexLocation));
    }

    /**
     * Return the shard location respecting global blobs data path value
     */
    public Path shardLocation(ShardId shardId) {
        if (blobsPath == null) {
            return nodeEnvironment.availableShardPaths(shardId)[0].resolve(BLOBS_SUB_PATH);
        }
        return shardLocation(shardId, blobsPath);
    }

    /**
     * Return the shard location according to the given base path
     */
    public Path shardLocation(ShardId shardId, Path path) {
        Path shardLocation = nodeEnvironment.availableShardPaths(shardId)[0];
        Path dataPath = nodeEnvironment.nodeDataPaths()[0];
        return path.resolve(dataPath.relativize(shardLocation));
    }

    /**
     * Validates a given blobs data path
     */
    public static void ensureExistsAndWritable(Path blobsPath) {
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
    }

    /**
     * Check if a given blob data path contains no indices and non crate related path
     */
    public static boolean isCustomBlobPathEmpty(Path root) throws IOException {
        if (root == null) {
            return false;
        }
        return isCustomBlobPathEmpty(root, true);
    }

    private static boolean isCustomBlobPathEmpty(Path file, boolean isRoot) throws IOException {
        if (!Files.isDirectory(file)) {
            return false;
        }
        if (isRoot) {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(file)) {
                Iterator<Path> it = files.iterator();
                if (it.hasNext()) {
                    Path firstFile = it.next();
                    if (it.hasNext()) {
                        return false;
                    }
                    if (firstFile.getFileName().toString().equals("indices")) {
                        return isCustomBlobPathEmpty(file, false);
                    }
                }
            }
            return true;
        } else {
            try (Stream<Path> files = Files.list(file)) {
                return files.anyMatch(i -> true);
            }
        }
    }
}
