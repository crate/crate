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

package io.crate.copy.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.crate.execution.engine.collect.files.FileInput;
import io.crate.execution.engine.collect.files.FileReadingIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.crate.analyze.CopyStatementSettings.PROTOCOL_SETTING;

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
    public List<URI> listUris(@Nullable final URI fileUri,
                              final URI preGlobUri,
                              final Predicate<URI> uriPredicate,
                              Map<String, Object> withClauseOptions) throws IOException {
        S3URI preGlobS3Uri = new S3URI(preGlobUri);
        if (client == null) {
            client = clientBuilder.client(preGlobS3Uri.uri, withClauseOptions);
        }
        List<URI> uris = new ArrayList<>();
        ObjectListing list = client.listObjects(preGlobS3Uri.bucket, preGlobS3Uri.key);
        addKeyUris(uris, list, preGlobUri, uriPredicate);
        while (list.isTruncated()) {
            list = client.listNextBatchOfObjects(list);
            addKeyUris(uris, list, preGlobUri, uriPredicate);
        }

        return uris;
    }

    private void addKeyUris(List<URI> uris, ObjectListing list, URI uri, Predicate<URI> uriPredicate) {
        List<S3ObjectSummary> summaries = list.getObjectSummaries();
        for (S3ObjectSummary summary : summaries) {
            String key = summary.getKey();
            if (!key.endsWith("/")) {
                URI keyUri = uri.resolve("/" + summary.getBucketName() + "/" + key);
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
    public InputStream getStream(URI uri, Map<String, Object> withClauseOptions) throws IOException {
        S3URI s3URI = new S3URI(uri);
        if (client == null) {
            client = clientBuilder.client(s3URI.uri, withClauseOptions);
        }
        S3Object object = client.getObject(s3URI.bucket, s3URI.key);
        if (object != null) {
            return object.getObjectContent();
        }
        throw new IOException("Failed to load S3 URI: " + uri.toString());
    }

    @Override
    public boolean sharedStorageDefault() {
        return true;
    }

    @Override
    public Function<String, URI> uriFormatter() {
        return fileUri -> S3URI.reformat(FileReadingIterator.toURI(fileUri));
    }

    @Override
    public Set<String> validWithClauseOptions() {
        return Set.of(PROTOCOL_SETTING.getKey());
    }
}
