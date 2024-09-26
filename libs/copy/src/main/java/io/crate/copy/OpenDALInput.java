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

package io.crate.copy;

import static io.crate.analyze.CopyStatementSettings.COMMON_COPY_FROM_SETTINGS;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Entry;
import org.apache.opendal.Operator;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.collections.Lists;
import io.crate.execution.engine.collect.files.FileInput;

public abstract class OpenDALInput<T extends OpenDalURI> implements FileInput {

    private final Operator operator;
    private final OpenDalURI openDalURI;

    public OpenDALInput(String scheme,
                        T openDalURI,
                        Configuration<T> configuration,
                        SharedAsyncExecutor sharedAsyncExecutor,
                        Settings settings) {

        configuration.validate(settings, true);
        // Common validation of COPY FROM settings.
        // Scheme specific validation is done in the concrete implementation.
        List<String> validSettings = Lists.concat(
            configuration.supportedSettings().stream().map(Setting::getKey).toList(),
            COMMON_COPY_FROM_SETTINGS
        );
        for (String key : settings.keySet()) {
            if (validSettings.contains(key) == false) {
                throw new IllegalArgumentException("Setting '" + key + "' is not supported");
            }
        }

        this.openDalURI = openDalURI;
        Map<String, String> config = configuration.fromURIAndSettings(openDalURI, settings);
        this.operator = AsyncOperator.of(scheme, config, sharedAsyncExecutor.asyncExecutor()).blocking();
    }

    @Override
    public List<URI> expandUri() throws IOException {
        if (isGlobbed() == false) {
            return List.of(openDalURI.uri());
        }
        List<URI> uris = new ArrayList<>();
        var preGlobPath = openDalURI.preGlobPath();
        assert preGlobPath != null : "List API must be used only for a globbed URI.";
        List<Entry> entries = operator.list(preGlobPath);
        for (Entry entry : entries) {
            var path = entry.getPath();
            if (openDalURI.matchesGlob(path)) {
                uris.add(URI.create(path));
            }
        }
        return uris;
    }

    @Override
    public InputStream getStream(URI uri) throws IOException {
        return operator.createInputStream(uri.getPath());
    }

    @Override
    public boolean isGlobbed() {
        return openDalURI.preGlobPath() != null;
    }

    @Override
    public URI uri() {
        return openDalURI.uri();
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
