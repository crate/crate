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

package io.crate.copy.azure;

import static io.crate.copy.azure.AzureCopyPlugin.NAME;
import static io.crate.copy.azure.AzureFileOutput.resourcePath;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Entry;
import org.apache.opendal.Operator;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.CopyStatementSettings;
import io.crate.execution.engine.collect.files.FileInput;
import io.crate.execution.engine.collect.files.Globs.GlobPredicate;

/**
 * File reading components operate with URI.
 * All URI-s in the public API follow the contract "outgoing/incoming" URI is Azure compatible.
 * This is accomplished by transforming user provided URI to the Azure compatible format only once.
 * Outgoing URI-s are then used by other components and sent back to this component,
 * so outgoing format (expandURI) implicitly dictates incoming URI-s format (getStream).
 */
public class AzureFileInput implements FileInput {

    private static final Pattern HAS_GLOBS_PATTERN = Pattern.compile("^((azblob://)[^\\*]*/)[^\\*]*\\*.*");

    private final Map<String, String> config;
    private final URI uri;
    private final GlobPredicate uriPredicate;
    @Nullable
    private final String preGlobPath;
    private final Operator operator;
    private final SharedAsyncExecutor sharedAsyncExecutor;

    /**
     * @param uri is in user provided format (azblob://path/to/dir)
     */
    public AzureFileInput(SharedAsyncExecutor sharedAsyncExecutor, URI uri, Settings settings) {
        this.config = AzureBlobStorageSettings.openDALConfig(settings, CopyStatementSettings.commonCopyFromSettings);
        this.sharedAsyncExecutor = sharedAsyncExecutor;
        this.operator = AsyncOperator.of(NAME, config, sharedAsyncExecutor.asyncExecutor()).blocking();
        // Pre-glob path operates with user-provided URI as GLOB pattern reflects user facing COPY FROM uri format.
        this.preGlobPath = toPreGlobPath(uri);

        String resourcePath = resourcePath(uri);
        this.uri = URI.create(resourcePath);
        // Glob predicate operates with normalized resource URI to reflect OpenDAL List API response format.
        // List API returns entries without leading backslash.
        this.uriPredicate = new GlobPredicate(resourcePath.substring(1));
    }

    /**
     * @return List<URI> in Azure compatible format.
     */
    @Override
    public List<URI> expandUri() throws IOException {
        var operator = operator(); // Only for tests, otherwise could use this.operator
        if (isGlobbed() == false) {
            return List.of(uri);
        }
        List<URI> uris = new ArrayList<>();
        List<Entry> entries = operator.list(preGlobPath);
        for (Entry entry : entries) {
            var path = entry.getPath();
            if (uriPredicate.test(path)) {
                uris.add(URI.create(path));
            }
        }
        return uris;
    }

    @VisibleForTesting
    Operator operator() {
        return AsyncOperator.of(NAME, config, sharedAsyncExecutor.asyncExecutor()).blocking();
    }

    /**
     * @param uri is resource path without "azblob" schema.
     */
    @Override
    public InputStream getStream(URI uri) throws IOException {
        return operator.createInputStream(uri.toString());
    }

    @Override
    public boolean isGlobbed() {
        return preGlobPath != null;
    }

    @Override
    public URI uri() {
        return uri;
    }

    @Override
    public boolean sharedStorageDefault() {
        return true;
    }

    @Override
    public void close() {
        assert operator != null : "Operator must be created before FileInput is closed";
        operator.close();
    }

    /**
     * @return pre-glob path in Azure compatible format.
     */
    @Nullable
    public static String toPreGlobPath(URI uri) {
        Matcher hasGlobMatcher = HAS_GLOBS_PATTERN.matcher(uri.toString());
        if (hasGlobMatcher.matches()) {
            return resourcePath(URI.create(hasGlobMatcher.group(1)));
        }
        return null;
    }
}
