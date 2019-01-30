/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.blobstore.fs;

import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FsBlobStore extends AbstractComponent implements BlobStore {

    private final Path path;

    private final int bufferSizeInBytes;

    private final boolean readOnly;

    public FsBlobStore(Settings settings, Path path) throws IOException {
        super(settings);
        this.path = path;
        this.readOnly = settings.getAsBoolean("readonly", false);
        if (!this.readOnly) {
            Files.createDirectories(path);
        }
        this.bufferSizeInBytes = (int) settings.getAsBytesSize("repositories.fs.buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).getBytes();
    }

    @Override
    public String toString() {
        return path.toString();
    }

    public Path path() {
        return path;
    }

    public int bufferSizeInBytes() {
        return this.bufferSizeInBytes;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        try {
            return new FsBlobContainer(this, path, buildAndCreate(path));
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to create blob container", ex);
        }
    }

    @Override
    public void delete(BlobPath path) throws IOException {
        IOUtils.rm(buildPath(path));
    }

    @Override
    public void close() {
        // nothing to do here...
    }

    private synchronized Path buildAndCreate(BlobPath path) throws IOException {
        Path f = buildPath(path);
        if (!readOnly) {
            Files.createDirectories(f);
        }
        return f;
    }

    private Path buildPath(BlobPath path) {
        String[] paths = path.toArray();
        if (paths.length == 0) {
            return path();
        }
        Path blobPath = this.path.resolve(paths[0]);
        if (paths.length > 1) {
            for (int i = 1; i < paths.length; i++) {
                blobPath = blobPath.resolve(paths[i]);
            }
        }
        return blobPath;
    }
}
