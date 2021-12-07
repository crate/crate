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

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

class URLFileInput implements FileInput {

    private final URI fileUri;

    public URLFileInput(URI fileUri) {
        this.fileUri = fileUri;
    }

    @Override
    public List<URI> listUris(URI fileUri, URI preGlobUri, Predicate<URI> uriPredicate,
                              @Nullable String protocolSetting) throws IOException {
        // If the full fileUri contains a wildcard the fileUri passed as argument here is the fileUri up to the wildcard
        // for URLs listing directory contents is not supported so always return the full fileUri for now
        return Collections.singletonList(this.fileUri);
    }

    @Override
    public InputStream getStream(URI uri, @Nullable String protocolSetting) throws IOException {
        URL url = uri.toURL();
        return url.openStream();
    }

    @Override
    public Function<String, URI> uriFormatter() {
        return null;
    }

    @Override
    public boolean sharedStorageDefault() {
        return true;
    }
}
