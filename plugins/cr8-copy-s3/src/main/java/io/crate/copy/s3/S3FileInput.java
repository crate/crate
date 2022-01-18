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
import io.crate.common.annotations.VisibleForTesting;
import io.crate.execution.engine.collect.files.FileInput;
import io.crate.execution.engine.collect.files.Globs;
import io.crate.external.S3ClientHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3FileInput implements FileInput {

    private static final Pattern HAS_GLOBS_PATTERN = Pattern.compile("^((s3://)[^\\*]*/)[^\\*]*\\*.*");

    private AmazonS3 client; // to prevent early GC during getObjectContent() in getStream()
    private static final Logger LOGGER = LogManager.getLogger(S3FileInput.class);

    private final S3ClientHelper clientBuilder;

    @Nonnull
    private final URI uri;
    @Nullable
    private final URI preGlobUri;
    @Nonnull
    private final Predicate<URI> uriPredicate;

    public S3FileInput(URI uri) {
        this(new S3ClientHelper(), uri);
    }

    public S3FileInput(S3ClientHelper clientBuilder, URI uri) {
        this.clientBuilder = clientBuilder;
        this.uri = uri;
        this.preGlobUri = toPreGlobUri(uri);
        this.uriPredicate = new GlobPredicate(uri);
    }

    @Override
    public boolean isGlobbed() {
        return preGlobUri != null;
    }

    @Override
    public URI uri() {
        return uri;
    }

    @Override
    public List<URI> expandUri() throws IOException {
        if (isGlobbed() == false) {
            return List.of(uri);
        }
        if (client == null) {
            client = clientBuilder.client(preGlobUri);
        }
        String bucketName = preGlobUri.getHost();
        String prefix = preGlobUri.getPath().length() > 1 ? preGlobUri.getPath().substring(1) : "";
        List<URI> uris = new ArrayList<>();
        ObjectListing list = client.listObjects(bucketName, prefix);
        addKeyUris(uris, list);
        while (list.isTruncated()) {
            list = client.listNextBatchOfObjects(list);
            addKeyUris(uris, list);
        }
        return uris;
    }

    private void addKeyUris(List<URI> uris, ObjectListing list) {
        assert preGlobUri != null;
        List<S3ObjectSummary> summaries = list.getObjectSummaries();
        for (S3ObjectSummary summary : summaries) {
            String key = summary.getKey();
            if (!key.endsWith("/")) {
                URI keyUri = preGlobUri.resolve("/" + key);
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
        String key = uri.getPath().length() > 1 ? uri.getPath().substring(1) : "";
        S3Object object = client.getObject(uri.getHost(), key);
        if (object != null) {
            return object.getObjectContent();
        }
        throw new IOException("Failed to load S3 URI: " + uri.toString());
    }

    @Override
    public boolean sharedStorageDefault() {
        return true;
    }

    @VisibleForTesting
    @Nullable
    static URI toPreGlobUri(URI fileUri) {
        Matcher hasGlobMatcher = HAS_GLOBS_PATTERN.matcher(fileUri.toString());
        URI preGlobUri = null;
        if (hasGlobMatcher.matches()) {
            preGlobUri = URI.create(hasGlobMatcher.group(1));
        }
        return preGlobUri;
    }

    private static class GlobPredicate implements Predicate<URI> {
        private final Pattern globPattern;

        GlobPredicate(URI fileUri) {
            this.globPattern = Pattern.compile(Globs.toUnixRegexPattern(fileUri.toString()));
        }

        @Override
        public boolean test(@Nullable URI input) {
            return input != null && globPattern.matcher(input.toString()).matches();
        }
    }
}
