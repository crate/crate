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
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.copy.s3.common.S3ClientHelper;
import io.crate.copy.s3.common.S3URI;
import io.crate.execution.engine.collect.files.FileInput;
import io.crate.execution.engine.collect.files.Globs.GlobPredicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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

    @NotNull
    private final S3URI normalizedS3URI;
    @Nullable
    private final S3URI preGlobUri;
    @NotNull
    private final Predicate<String> uriPredicate;
    @Nullable
    private final String protocolSetting;

    public S3FileInput(URI uri, String protocol) {
        this.clientBuilder = new S3ClientHelper();
        this.normalizedS3URI = S3URI.toS3URI(uri);
        this.preGlobUri = toPreGlobUri(this.normalizedS3URI);
        this.uriPredicate = new GlobPredicate(this.normalizedS3URI.toString());
        this.protocolSetting = protocol;
    }

    @VisibleForTesting
    S3FileInput(S3ClientHelper clientBuilder, URI uri, String protocol) {
        this.clientBuilder = clientBuilder;
        this.normalizedS3URI = S3URI.toS3URI(uri);
        this.preGlobUri = toPreGlobUri(this.normalizedS3URI);
        this.uriPredicate = new GlobPredicate(this.normalizedS3URI.toString());
        this.protocolSetting = protocol;
    }

    @Override
    public boolean isGlobbed() {
        return preGlobUri != null;
    }

    @Override
    public URI uri() {
        return normalizedS3URI.uri();
    }

    @Override
    public List<URI> expandUri() throws IOException {
        if (isGlobbed() == false) {
            return List.of(normalizedS3URI.uri());
        }
        if (client == null) {
            client = clientBuilder.client(preGlobUri, protocolSetting);
        }
        List<URI> uris = new ArrayList<>();
        ObjectListing list = client.listObjects(preGlobUri.bucket(), preGlobUri.key());
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
                S3URI keyUri = preGlobUri.replacePath(summary.getBucketName(), key);
                if (uriPredicate.test(keyUri.toString())) {
                    uris.add(keyUri.uri());
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("{}", keyUri);
                    }
                }
            }
        }
    }

    @Override
    public InputStream getStream(URI uri) throws IOException {
        S3URI s3URI = S3URI.toS3URI(uri);
        if (client == null) {
            client = clientBuilder.client(s3URI, protocolSetting);
        }
        S3Object object = client.getObject(s3URI.bucket(), s3URI.key());
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
    static S3URI toPreGlobUri(S3URI uri) {
        Matcher hasGlobMatcher = HAS_GLOBS_PATTERN.matcher(uri.toString());
        S3URI preGlobUri = null;
        if (hasGlobMatcher.matches()) {
            preGlobUri = S3URI.toS3URI(URI.create(hasGlobMatcher.group(1)));
        }
        return preGlobUri;
    }
}
