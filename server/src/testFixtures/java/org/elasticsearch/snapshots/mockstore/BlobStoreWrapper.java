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
package org.elasticsearch.snapshots.mockstore;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

import java.io.IOException;

public class BlobStoreWrapper implements BlobStore {

    private BlobStore delegate;

    public BlobStoreWrapper(BlobStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return delegate.blobContainer(path);
    }

    @Override
    public void delete(BlobPath path) throws IOException {

    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    protected BlobStore delegate() {
        return delegate;
    }

}
