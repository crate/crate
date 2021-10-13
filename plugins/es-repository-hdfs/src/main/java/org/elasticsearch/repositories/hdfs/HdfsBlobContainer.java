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

package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Path;
import javax.annotation.Nullable;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;
import org.elasticsearch.repositories.hdfs.HdfsBlobStore.Operation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class HdfsBlobContainer extends AbstractBlobContainer {
    private final HdfsBlobStore store;
    private final HdfsSecurityContext securityContext;
    private final Path path;
    private final int bufferSize;

    HdfsBlobContainer(BlobPath blobPath, HdfsBlobStore store, Path path, int bufferSize, HdfsSecurityContext hdfsSecurityContext) {
        super(blobPath);
        this.store = store;
        this.securityContext = hdfsSecurityContext;
        this.path = path;
        this.bufferSize = bufferSize;
    }

    @Override
    public void delete() throws IOException {
        store.execute(fileContext -> fileContext.delete(path, true));
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(final List<String> blobNames) throws IOException {
        IOException ioe = null;
        for (String blobName : blobNames) {
            try {
                store.execute(fileContext -> fileContext.delete(new Path(path, blobName), true));
            } catch (final FileNotFoundException ignored) {
                // This exception is ignored
            } catch (IOException e) {
                if (ioe == null) {
                    ioe = e;
                } else {
                    ioe.addSuppressed(e);
                }
            }
        }
        if (ioe != null) {
            throw ioe;
        }
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        // FSDataInputStream does buffering internally
        // FSDataInputStream can open connections on read() or skip() so we wrap in
        try {
            return store.execute(fileContext -> fileContext.open(new Path(path, blobName), bufferSize));
        } catch (FileNotFoundException fnfe) {
            throw new NoSuchFileException("[" + blobName + "] blob not found");
        }
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        store.execute((Operation<Void>) fileContext -> {
            Path blob = new Path(path, blobName);
            // we pass CREATE, which means it fails if a blob already exists.
            EnumSet<CreateFlag> flags = failIfAlreadyExists ? EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK) :
                EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE, CreateFlag.SYNC_BLOCK);
            CreateOpts[] opts = {CreateOpts.bufferSize(bufferSize)};
            try (FSDataOutputStream stream = fileContext.create(blob, flags, opts)) {
                int bytesRead;
                byte[] buffer = new byte[bufferSize];
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    stream.write(buffer, 0, bytesRead);
                    //  For safety we also hsync each write as well, because of its docs:
                    //  SYNC_BLOCK - to force closed blocks to the disk device
                    // "In addition Syncable.hsync() should be called after each write,
                    //  if true synchronous behavior is required"
                    stream.hsync();
                }
            } catch (org.apache.hadoop.fs.FileAlreadyExistsException faee) {
                throw new FileAlreadyExistsException(blob.toString(), null, faee.getMessage());
            }
            return null;
        });
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(@Nullable final String prefix) throws IOException {
        FileStatus[] files = store.execute(
            fileContext -> fileContext.util().listStatus(path, path -> prefix == null || path.getName().startsWith(prefix)));
        Map<String, BlobMetadata> map = new LinkedHashMap<>();
        for (FileStatus file : files) {
            if (file.isFile()) {
                map.put(file.getPath().getName(), new PlainBlobMetadata(file.getPath().getName(), file.getLen()));
            }
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        FileStatus[] files = store.execute(fileContext -> fileContext.util().listStatus(path));
        Map<String, BlobContainer> map = new LinkedHashMap<>();
        for (FileStatus file : files) {
            if (file.isDirectory()) {
                final String name = file.getPath().getName();
                map.put(name, new HdfsBlobContainer(path().add(name), store, new Path(path, name), bufferSize, securityContext));
            }
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }
}
