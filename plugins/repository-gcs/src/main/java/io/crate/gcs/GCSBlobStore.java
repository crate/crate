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

import java.io.IOException;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

import com.google.cloud.storage.Storage;


public class GCSBlobStore implements BlobStore {

    private final Storage storage;
    private final String bucketName;

    GCSBlobStore(Storage storage, String bucketName) {
        this.storage = storage;
        this.bucketName = bucketName;
    }

    public Storage storage() {
        return storage;
    }

    public String bucketName() {
        return bucketName;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new GCSBlobContainer(path, this);
    }

    @Override
    public void close() throws IOException {
        try {
            storage.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
