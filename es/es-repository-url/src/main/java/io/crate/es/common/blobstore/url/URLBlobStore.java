/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.es.common.blobstore.url;

import io.crate.es.common.unit.ByteSizeUnit;
import io.crate.es.common.unit.ByteSizeValue;
import io.crate.es.common.blobstore.BlobContainer;
import io.crate.es.common.blobstore.BlobPath;
import io.crate.es.common.blobstore.BlobStore;
import io.crate.es.common.blobstore.BlobStoreException;
import io.crate.es.common.component.AbstractComponent;
import io.crate.es.common.settings.Settings;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Read-only URL-based blob store
 */
public class URLBlobStore extends AbstractComponent implements BlobStore {

    private final URL path;

    private final int bufferSizeInBytes;

    /**
     * Constructs new read-only URL-based blob store
     * <p>
     * The following settings are supported
     * <dl>
     * <dt>buffer_size</dt>
     * <dd>- size of the read buffer, defaults to 100KB</dd>
     * </dl>
     *
     * @param settings settings
     * @param path     base URL
     */
    public URLBlobStore(Settings settings, URL path) {
        super(settings);
        this.path = path;
        this.bufferSizeInBytes = (int) settings.getAsBytesSize("repositories.uri.buffer_size",
            new ByteSizeValue(100, ByteSizeUnit.KB)).getBytes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return path.toString();
    }

    /**
     * Returns base URL
     *
     * @return base URL
     */
    public URL path() {
        return path;
    }

    /**
     * Returns read buffer size
     *
     * @return read buffer size
     */
    public int bufferSizeInBytes() {
        return this.bufferSizeInBytes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlobContainer blobContainer(BlobPath path) {
        try {
            return new URLBlobContainer(this, path, buildPath(path));
        } catch (MalformedURLException ex) {
            throw new BlobStoreException("malformed URL " + path, ex);
        }
    }

    /**
     * This operation is not supported by URL Blob Store
     */
    @Override
    public void delete(BlobPath path) {
        throw new UnsupportedOperationException("URL repository is read only");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // nothing to do here...
    }

    /**
     * Builds URL using base URL and specified path
     *
     * @param path relative path
     * @return Base URL + path
     */
    private URL buildPath(BlobPath path) throws MalformedURLException {
        String[] paths = path.toArray();
        if (paths.length == 0) {
            return path();
        }
        URL blobPath = new URL(this.path, paths[0] + "/");
        if (paths.length > 1) {
            for (int i = 1; i < paths.length; i++) {
                blobPath = new URL(blobPath, paths[i] + "/");
            }
        }
        return blobPath;
    }
}
