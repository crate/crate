/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.opendal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.OpenDALException.Code;
import org.apache.opendal.Operator;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;

public class OpenDALBlobContainer implements BlobContainer {

    private static final Logger LOGGER = LogManager.getLogger(OpenDALBlobContainer.class);

    private final BlobPath path;
    private final Operator operator;
    private final int bufferSize;

    public OpenDALBlobContainer(BlobPath path, Operator operator, int bufferSize) {
        this.path = path;
        this.operator = operator;
        this.bufferSize = bufferSize;
    }

    @Override
    public BlobPath path() {
        return path;
    }

    @Override
    public boolean blobExists(String blobName) throws IOException {
        String path = this.path.buildAsString() + blobName;
        try {
            operator.stat(path);
            return true;
        } catch (OpenDALException ex) {
            if (ex.getCode() != Code.NotFound) {
                LOGGER.error("blobExists stat error on {}", path, ex);
            }
            return false;
        }
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        String path = this.path.buildAsString() + blobName;
        return operator.createInputStream(path);
    }


    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        String path = this.path.buildAsString() + blobName;
        int size = blobSize < bufferSize ? Math.toIntExact(blobSize) : bufferSize;
        if (failIfAlreadyExists && blobExists(blobName)) {
            throw new FileAlreadyExistsException(path);
        }
        try (var out = operator.createOutputStream(path, size)) {
            inputStream.transferTo(out);
            out.flush();
        }
    }

    @Override
    public void delete() throws IOException {
        operator.removeAll(path.buildAsString());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        String pathStr = this.path.buildAsString();
        for (String blobName : blobNames) {
            String path = pathStr + blobName;
            operator.removeAll(path);
        }
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        HashMap<String, BlobContainer> result = new HashMap<>();
        String pathStr = path.buildAsString();
        for (var entry : operator.list(pathStr)) {
            if (entry.getMetadata().isDir()) {
                String suffix = entry.path.substring(pathStr.length(), entry.path.length() - 1);
                if (!suffix.isEmpty()) {
                    result.put(suffix, new OpenDALBlobContainer(path.add(suffix), operator, bufferSize));
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return listBlobsByPrefix("");
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        String fullPath = this.path.buildAsString() + blobNamePrefix;
        HashMap<String, BlobMetadata> result = new HashMap<>();
        for (var entry : operator.list(fullPath)) {
            String path = entry.getPath();
            var blobMetadata = new BlobMetadata(path, entry.getMetadata().getContentLength());
            result.put(path, blobMetadata);
        }
        return result;
    }
}
