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

package io.crate.opendal;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.opendal.Metadata;
import org.apache.opendal.Operator;
import org.jspecify.annotations.Nullable;

import io.crate.execution.engine.collect.files.FileInput;
import io.crate.execution.engine.collect.files.Globs;

public class OpenDALFileInput implements FileInput {

    private final Operator operator;
    private final URI uri;
    private final String preGlobPath;
    private final Globs.@Nullable GlobPredicate globPredicate;

    /// @param operator OpenDAL operator. Is closed when the FileInput is closed
    public OpenDALFileInput(Operator operator,
                            URI uri,
                            @Nullable String preGlobPath,
                            Globs.@Nullable GlobPredicate globPredicate) {
        assert globPredicate == null || preGlobPath != null
            : "Must provide preGlobPath if globPredicate is present";
        this.operator = operator;
        this.uri = uri;
        this.preGlobPath = preGlobPath;
        this.globPredicate = globPredicate;
    }

    private void addMatches(List<URI> result, String path) {
        for (var entry : operator.list(path)) {
            String entryPath = entry.getPath();
            if (globPredicate.test(entryPath)) {
                Metadata stat = operator.stat(entryPath);
                if (stat.isDir()) {
                    addMatches(result, entryPath);
                } else {
                    result.add(URI.create(entryPath));
                }
            }
        }
    }

    @Override
    public List<URI> expandUri() throws IOException {
        if (!isGlobbed()) {
            return List.of(uri);
        }
        ArrayList<URI> result = new ArrayList<>();
        addMatches(result, preGlobPath);
        return result;
    }

    @Override
    public InputStream getStream(URI uri) throws IOException {
        return operator.createInputStream(uri.getPath());
    }

    @Override
    public boolean isGlobbed() {
        return globPredicate != null;
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
        operator.close();
    }
}
