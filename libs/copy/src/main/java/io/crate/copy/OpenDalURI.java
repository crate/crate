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

import java.net.URI;

import org.jetbrains.annotations.Nullable;

import io.crate.execution.engine.collect.files.Globs;

public abstract class OpenDalURI {

    private final URI uri;
    private final String resourcePath;
    private final Globs.GlobPredicate globPredicate;

    protected OpenDalURI(URI uri, String resourcePath, Globs.GlobPredicate globPredicate) {
        this.uri = uri;
        this.resourcePath = resourcePath;
        this.globPredicate = globPredicate;
    }

    public final URI uri() {
        return uri;
    }

    public final String resourcePath() {
        return resourcePath;
    }

    @Nullable
    public final String preGlobPath() {
        int asteriskIndex = resourcePath.indexOf("*");
        if (asteriskIndex < 0) {
            return null;
        }
        int lastBeforeAsterisk = 0;
        for (int i = asteriskIndex; i >= 0; i--) {
            if (resourcePath.charAt(i) == '/') {
                lastBeforeAsterisk = i;
                break;
            }
        }
        assert resourcePath.charAt(0) == '/' : "Resource path must start with the forwarding slash.";
        return resourcePath.substring(0, lastBeforeAsterisk + 1); // Returns "/" if glob matches the whole container.
    }

    public final boolean matchesGlob(String path) {
        return globPredicate.test(path);
    }
}
