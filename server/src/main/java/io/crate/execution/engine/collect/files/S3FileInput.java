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

package io.crate.execution.engine.collect.files;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.crate.external.S3ClientHelper;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class S3FileInput implements FileInput {

    private AmazonS3 client; // to prevent early GC during getObjectContent() in getStream()
    private static final Logger LOGGER = LogManager.getLogger(S3FileInput.class);

    final S3ClientHelper clientBuilder;

    public S3FileInput() {
        clientBuilder = new S3ClientHelper();
    }

    public S3FileInput(S3ClientHelper clientBuilder) {
        this.clientBuilder = clientBuilder;
    }

    @Override
    public List<URI> listUris(URI uri, Predicate<URI> uriPredicate) throws IOException {
        String bucketName = uri.getHost();
        if (client == null) {
            client = clientBuilder.client(uri);
        }
        String prefix = uri.getPath().length() > 1 ? uri.getPath().substring(1) : "";
        List<URI> uris = new ArrayList<>();
        ObjectListing list = client.listObjects(bucketName, prefix);
        addKeyUris(uris, list, uri, uriPredicate);
        while (list.isTruncated()) {
            list = client.listNextBatchOfObjects(list);
            addKeyUris(uris, list, uri, uriPredicate);
        }

        return uris;
    }

    private void addKeyUris(List<URI> uris, ObjectListing list, URI uri, Predicate<URI> uriPredicate) {
        List<S3ObjectSummary> summaries = list.getObjectSummaries();
        for (S3ObjectSummary summary : summaries) {
            String key = summary.getKey();
            if (!key.endsWith("/")) {
                URI keyUri = uri.resolve("/" + key);
                if (uriPredicate.test(keyUri)) {
                    uris.add(keyUri);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("{}", keyUri);
                    }
                }
            }
        }
    }

    @Override
    public InputStream getStream(URI uri) throws IOException {
        if (client == null) {
            client = clientBuilder.client(uri);
        }
        S3Object object = client.getObject(uri.getHost(), uri.getPath().substring(1));

        if (object != null) {
            return object.getObjectContent();
        }
        throw new IOException("Failed to load S3 URI: " + uri.toString());
    }

    @Override
    public boolean sharedStorageDefault() {
        return true;
    }
}
