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

package io.crate.gcs;

import static com.google.cloud.storage.Storage.BlobListOption.currentDirectory;
import static com.google.cloud.storage.Storage.BlobListOption.prefix;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageException;


public class GCSBlobContainer extends AbstractBlobContainer {

    private final GCSBlobStore blobStore;

    public GCSBlobContainer(BlobPath path, GCSBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
    }

    @Override
    public boolean blobExists(String blobName) throws StorageException {
        return getBlob(blobName) != null;
    }

    @Nullable
    private Blob getBlob(String blobName) throws StorageException {
        return blobStore.storage().get(blobStore.bucketName(), buildKey(blobName));
    }

    @Override
    public InputStream readBlob(String blobName) throws StorageException, IOException {
        Blob blob = getBlob(blobName);
        if (blob != null) {
            final ReadChannel channel = blob.reader();
            return Channels.newInputStream(channel);
        } else {
            throw new IOException("blob `" + blobName + "` does not exist");
        }
    }

    private String buildKey(String blobName) {
        return path().buildAsString() + blobName;
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        if (position < 0L) {
            throw new IllegalArgumentException("position must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        } else {
            Blob blob = getBlob(blobName);
            if (blob != null) {
                final ReadChannel channel = blob.reader();
                channel.seek(position);
                channel.limit(Math.addExact(position, length - 1));
                return Channels.newInputStream(channel);
            } else {
                throw new IOException("blob `" + blobName + "` does not exist");
            }
        }
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream,
                          long blobSize, boolean failIfAlreadyExists) throws IOException {
        Blob blob = createBlob(blobName, failIfAlreadyExists);
        try (WriteChannel channel = blob.writer()) {
            final OutputStream outputStream = Channels.newOutputStream(channel);
            inputStream.transferTo(outputStream);
        }
    }

    @Override
    public void delete() {
        String basePath = path().buildAsString();
        var blobs = blobStore.storage().list(blobStore.bucketName(), prefix(basePath));
        for (Blob blob : blobs.iterateAll()) {
            blob.delete();
        }
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) {
        for (var blobName : blobNames) {
            final Blob blob = getBlob(blobName);
            if (blob != null) {
                blob.delete();
            }
        }
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        String prefix = path().buildAsString();
        var result = new HashMap<String, BlobContainer>();
        var blobs = blobStore.storage().list(blobStore.bucketName(), currentDirectory(), prefix(prefix));
        for (var blob : blobs.iterateAll()) {
            if (blob.isDirectory()) {
                assert blob.getName().startsWith(prefix);
                assert blob.getName().endsWith("/");
                final String suffixName = blob.getName().substring(prefix.length(), blob.getName().length() - 1);
                if (suffixName.isEmpty() == false) {
                    result.put(suffixName, new GCSBlobContainer(path().add(suffixName), blobStore));
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() {
        return listBlobsByPrefix("");
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) {
        String prefix = buildKey(blobNamePrefix);
        var blobs = blobStore.storage().list(blobStore.bucketName(), prefix(prefix));
        var result = new HashMap<String, BlobMetadata>();
        for (var blob : blobs.iterateAll()) {
            var name = blob.getName().substring(prefix.length());
            result.put(name, new PlainBlobMetadata(name, blob.getSize()));
        }
        return result;
    }

    private Blob createBlob(String blobName, boolean failIfAlreadyExists) throws IOException {
        Blob blob = getBlob(blobName);
        if (blob != null) {
            if (failIfAlreadyExists) {
                throw new IOException("blob `" + blobName + "` already exists");
            } else {
                return blob;
            }
        }
        var blobInfo = BlobInfo.newBuilder(blobStore.bucketName(), buildKey(blobName)).build();
        return blobStore.storage().create(blobInfo, new byte[]{});
    }

}
