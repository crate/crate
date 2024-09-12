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

import static io.crate.copy.azure.AzureBlobStorageSettings.REQUIRED_SETTINGS;
import static io.crate.copy.azure.AzureBlobStorageSettings.SUPPORTED_SETTINGS;
import static io.crate.copy.azure.AzureBlobStorageSettings.validate;
import static io.crate.copy.azure.AzureCopyPlugin.NAME;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.opendal.Entry;
import org.apache.opendal.Operator;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

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

    /**
     * @param uri is in user provided format (azblob:///path/to/dir)
     */
    public AzureFileInput(TriFunction<String, Map<String, String>, SharedAsyncExecutor, Operator> createOperator,
                          SharedAsyncExecutor sharedAsyncExecutor,
                          URI uri,
                          Settings settings) {
        validate(settings, true);

        config = new HashMap<>();
        for (Setting<String> setting : SUPPORTED_SETTINGS) {
            var value = setting.get(settings);
            var key = setting.getKey();
            if (value != null) {
                config.put(key, value);
            } else if (REQUIRED_SETTINGS.contains(key)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Setting %s must be provided", key)
                );
            }
        }

        this.operator = createOperator.apply(NAME, config, sharedAsyncExecutor);
        // Pre-glob path operates with user-provided URI as GLOB pattern reflects user facing COPY FROM uri format.
        this.preGlobPath = toPreGlobPath(uri);

        this.uri = uri;
        // Glob predicate operates with normalized resource URI to reflect OpenDAL List API response format.
        // List API returns entries without leading backslash.
        this.uriPredicate = new GlobPredicate(uri.getPath().substring(1));
    }

    /**
     * @return List<URI> in Azure compatible format.
     */
    @Override
    public List<URI> expandUri() throws IOException {
        if (isGlobbed() == false) {
            return List.of(uri);
        }
        List<URI> uris = new ArrayList<>();
        assert preGlobPath != null : "List API must be used only for a globbed URI.";
        List<Entry> entries = operator.list(preGlobPath);
        for (Entry entry : entries) {
            var path = entry.getPath();
            if (uriPredicate.test(path)) {
                uris.add(URI.create(path));
            }
        }
        return uris;
    }

    /**
     * @param uri is resource path without "azblob" schema.
     */
    @Override
    public InputStream getStream(URI uri) throws IOException {
        return operator.createInputStream(uri.getPath());
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
            return URI.create(hasGlobMatcher.group(1)).getPath();
        }
        return null;
    }
}
