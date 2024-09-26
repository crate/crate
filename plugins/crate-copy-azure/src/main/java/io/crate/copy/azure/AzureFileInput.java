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

import static io.crate.copy.azure.AzureCopyPlugin.OPEN_DAL_SCHEME;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Entry;
import org.apache.opendal.Operator;
import org.elasticsearch.common.settings.Settings;

import io.crate.execution.engine.collect.files.FileInput;

/**
 * File reading components operate with URI.
 * All URI-s in the public API follow the contract "outgoing/incoming" URI is Azure compatible.
 * This is accomplished by transforming user provided URI to the Azure compatible format only once.
 * Outgoing URI-s are then used by other components and sent back to this component,
 * so outgoing format (expandURI) implicitly dictates incoming URI-s format (getStream).
 */
public class AzureFileInput implements FileInput {

    private final Map<String, String> config;
    private final AzureURI azureURI;
    private final URI uri;
    private final Operator operator;

    public AzureFileInput(SharedAsyncExecutor sharedAsyncExecutor,
                          URI uri,
                          Settings settings) {

        this.azureURI = AzureURI.of(uri);
        this.config = OperatorHelper.config(azureURI, settings);

        this.operator = AsyncOperator.of(OPEN_DAL_SCHEME, config, sharedAsyncExecutor.asyncExecutor()).blocking();

        String resourceURI = azureURI.resourcePath();
        this.uri = URI.create(resourceURI);
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
        var preGlobPath = azureURI.preGlobPath();
        assert preGlobPath != null : "List API must be used only for a globbed URI.";
        List<Entry> entries = operator.list(preGlobPath);
        for (Entry entry : entries) {
            var path = entry.getPath();
            if (azureURI.matchesGlob(path)) {
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
        return azureURI.preGlobPath() != null;
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
}
