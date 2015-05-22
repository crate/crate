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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.File;
import java.nio.file.Path;
import java.util.Locale;

public class BlobEnvironment {

    public static final String SETTING_BLOBS_PATH = "blobs.path";
    public static final String BLOBS_SUB_PATH = "blobs";

    private final NodeEnvironment nodeEnvironment;
    private final ClusterName clusterName;

    @Nullable
    private File blobsPath;

    @Inject
    public BlobEnvironment(NodeEnvironment nodeEnvironment, ClusterName clusterName) {
        this.nodeEnvironment = nodeEnvironment;
        this.clusterName = clusterName;
    }

    @Nullable
    public File blobsPath() {
        return blobsPath;
    }

    public void blobsPath(File blobPath) {
        validateBlobsPath(blobPath);
        this.blobsPath = blobPath;
    }

    /**
     * Return the index location respecting global blobs data path value
     */
    public File indexLocation(Index index) {
        if (blobsPath == null) {
            return nodeEnvironment.indexLocations(index)[0];
        }
        return indexLocation(index, blobsPath);
    }

    /**
     * Return the index location according to the given base path
     */
    public File indexLocation(Index index, File path) {
        File indexLocation = nodeEnvironment.indexLocations(index)[0];
        String dataPath = nodeEnvironment.nodeDataPaths()[0].toString();
        String indexLocationSuffix = indexLocation.getAbsolutePath().substring(dataPath.length());
        return new File(path, indexLocationSuffix);
    }

    /**
     * Return the shard location respecting global blobs data path value
     */
    public File shardLocation(ShardId shardId) {
        if (blobsPath == null) {
            return new File(nodeEnvironment.shardLocations(shardId)[0], BLOBS_SUB_PATH);
        }
        return shardLocation(shardId, blobsPath);
    }

    /**
     * Return the shard location according to the given base path
     *
     */
    public File shardLocation(ShardId shardId, File path) {
        Path shardLocation = nodeEnvironment.shardPaths(shardId)[0];
        Path dataPath = nodeEnvironment.nodeDataPaths()[0];
        String shardLocationSuffix = shardLocation.toAbsolutePath().toString().substring(dataPath.toString().length());
        return new File(new File(path, shardLocationSuffix), BLOBS_SUB_PATH);
    }

    /**
     * Validates a given blobs data path
     */
    public void validateBlobsPath(File blobsPath) {
        if (blobsPath.exists()) {
            if (blobsPath.isFile()) {
                throw new SettingsException(
                        String.format(Locale.ENGLISH, "blobs path '%s' is a file, must be a directory", blobsPath.getAbsolutePath()));
            }
            if (!blobsPath.canWrite()) {
                throw new SettingsException(
                        String.format(Locale.ENGLISH, "blobs path '%s' is not writable", blobsPath.getAbsolutePath()));
            }
        } else {
            if(!FileSystemUtils.mkdirs(blobsPath)) {
                throw new SettingsException(
                        String.format(Locale.ENGLISH, "blobs path '%s' could not be created", blobsPath.getAbsolutePath()));
            }
        }
    }

    /**
     * Check if a given blob data path contains no indices and non crate related path
     */
    public boolean isCustomBlobPathEmpty(File root) {
        return isCustomBlobPathEmpty(root, true);
    }

    private boolean isCustomBlobPathEmpty(File file, boolean isRoot) {
        if (file == null || !file.exists() || !file.isDirectory()) {
            return false;
        }
        File[] children = file.listFiles();
        if (children == null || children.length == 0) {
            return true;
        }
        //noinspection SimplifiableIfStatement
        if (isRoot && children.length == 1 && children[0].getName().equals("indices")) {
            return isCustomBlobPathEmpty(children[0], false);
        }
        return false;
    }
}
