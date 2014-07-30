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
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.File;
import java.util.Locale;

public class BlobEnvironment {

    public static final String SETTING_PATH_BLOBS = "path.blobs";

    private final Settings settings;
    private final NodeEnvironment nodeEnvironment;

    @Nullable
    private File blobsPath;

    @Inject
    public BlobEnvironment(Settings settings, NodeEnvironment nodeEnvironment) {
        this.settings = settings;
        this.nodeEnvironment = nodeEnvironment;
    }

    @Nullable
    public File blobsPath() {
        return blobsPath;
    }

    public void blobsPath(File blobPath) {
        validateBlobsPath(blobPath);
        this.blobsPath = blobPath;
    }

    public File indexLocation(Index index) {
        if (blobsPath == null) {
            return nodeEnvironment.indexLocations(index)[0];
        }
        return indexLocation(index, blobsPath);
    }

    public File indexLocation(Index index, File blobsPath) {
        File indexLocation = nodeEnvironment.indexLocations(index)[0];
        String dataPath = settings.getAsArray("path.data")[0];
        String indexLocationSuffix = indexLocation.getAbsolutePath().substring(dataPath.length());
        return new File(blobsPath, indexLocationSuffix);
    }

    public File shardLocations(ShardId shardId) {
        if (blobsPath == null) {
            return nodeEnvironment.shardLocations(shardId)[0];
        }
        return shardLocations(shardId, blobsPath);
    }

    public File shardLocations(ShardId shardId, File blobsPath) {
        File shardLocation = nodeEnvironment.shardLocations(shardId)[0];
        String dataPath = settings.getAsArray("path.data")[0];
        String shardLocationSuffix = shardLocation.getAbsolutePath().substring(dataPath.length());
        return new File(new File(blobsPath, shardLocationSuffix), "blobs");
    }

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
}
