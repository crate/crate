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

package org.elasticsearch.repositories.azure;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jetbrains.annotations.Nullable;

import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;

import com.microsoft.azure.storage.StorageException;

import io.crate.common.io.Streams;

/**
 * In memory storage for unit tests
 */
public class AzureStorageServiceMock extends AzureStorageService {

    protected final Map<String, ByteArrayOutputStream> blobs = new ConcurrentHashMap<>();

    AzureStorageServiceMock(AzureStorageSettings storageSettings) {
        super(storageSettings);
    }

    @Override
    public boolean blobExists(String container, String blob) {
        return blobs.containsKey(blob);
    }

    @Override
    public void deleteBlob(String container, String blob) throws StorageException {
        if (blobs.remove(blob) == null) {
            throw new StorageException("BlobNotFound", "[" + blob + "] does not exist.", 404, null, null);
        }
    }

    @Override
    public InputStream getInputStream(String container, String blob, long position, @Nullable Long length)
        throws NoSuchFileException {

        if (!blobExists(container, blob)) {
            throw new NoSuchFileException("missing blob [" + blob + "]");
        }
        return new ByteArrayInputStream(blobs.get(blob).toByteArray());
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String container, String keyPath, String prefix) {
        final var blobsBuilder = new HashMap<String, BlobMetadata>();
        blobs.forEach((String blobName, ByteArrayOutputStream bos) -> {
            final String checkBlob;
            if (keyPath != null && !keyPath.isEmpty()) {
                // strip off key path from the beginning of the blob name
                checkBlob = blobName.replace(keyPath, "");
            } else {
                checkBlob = blobName;
            }
            if (prefix == null || startsWithIgnoreCase(checkBlob, prefix)) {
                blobsBuilder.put(blobName, new PlainBlobMetadata(checkBlob, bos.size()));
            }
        });
        return Map.copyOf(blobsBuilder);
    }

    @Override
    public void writeBlob(String container,
                          String blobName,
                          InputStream inputStream,
                          long blobSize,
                          boolean failIfAlreadyExists) throws StorageException, FileAlreadyExistsException {
        if (failIfAlreadyExists && blobs.containsKey(blobName)) {
            throw new FileAlreadyExistsException(blobName);
        }
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            blobs.put(blobName, outputStream);
            Streams.copy(inputStream, outputStream);
        } catch (IOException e) {
            throw new StorageException("MOCK", "Error while writing mock stream", e);
        }
    }

    /**
     * Test if the given String starts with the specified prefix,
     * ignoring upper/lower case.
     *
     * @param str    the String to check
     * @param prefix the prefix to look for
     * @see java.lang.String#startsWith
     */
    private static boolean startsWithIgnoreCase(String str, String prefix) {
        if (str == null || prefix == null) {
            return false;
        }
        if (str.startsWith(prefix)) {
            return true;
        }
        if (str.length() < prefix.length()) {
            return false;
        }
        String lcStr = str.substring(0, prefix.length()).toLowerCase(Locale.ROOT);
        String lcPrefix = prefix.toLowerCase(Locale.ROOT);
        return lcStr.equals(lcPrefix);
    }

    @Override
    public ClientOpCtx client() {
        return null;
    }
}
