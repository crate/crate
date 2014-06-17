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

package io.crate.operation.collect.files;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Predicate;
import io.crate.external.S3ClientHelper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class S3FileInput implements FileInput {

    final S3ClientHelper clientBuilder;

    public S3FileInput(S3ClientHelper clientBuilder) {
        this.clientBuilder = clientBuilder;
    }

    @Override
    public List<URI> listUris(URI uri, Predicate<URI> uriPredicate) throws IOException {
        String bucketName = uri.getHost();
        AmazonS3 client = clientBuilder.client(uri);
        String prefix = uri.getPath().length() > 1 ? uri.getPath().substring(1) : "";
        ObjectListing list = client.listObjects(bucketName, prefix);
        List<URI> uris = new ArrayList<>();
        do {
            List<S3ObjectSummary> summaries = list.getObjectSummaries();
            for (S3ObjectSummary summary : summaries) {
                URI keyUri = uri.resolve("/" + summary.getKey());
                if (uriPredicate.apply(keyUri)) {
                    uris.add(keyUri);
                }
            }
            list = client.listNextBatchOfObjects(list);
        } while (list.isTruncated());

        return uris;
    }

    @Override
    public InputStream getStream(URI uri) throws IOException {
        AmazonS3 client = clientBuilder.client(uri);
        S3Object object = client.getObject(uri.getHost(), uri.getPath().substring(1));
        if (object != null) {
            return object.getObjectContent();
        }
        return null;
    }

    @Override
    public boolean sharedStorageDefault() {
        return true;
    }
}
