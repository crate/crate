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

package io.crate.copy.gcs;

import static io.crate.copy.gcs.GCSCopyPlugin.USER_FACING_SCHEME;

import java.net.URI;

import org.jspecify.annotations.Nullable;

import io.crate.execution.engine.collect.files.Globs;

public record GCSURI(
    String bucket,
    String resourcePath,
    Globs.GlobPredicate globPredicate
) {

    public static GCSURI of(URI uri) {

        if (uri.getScheme().equals(USER_FACING_SCHEME) == false) {
            throw new IllegalArgumentException(
                "Invalid URI. URI must look like 'gs://bucket/path/to/file'");
        }

        String bucket = uri.getHost();
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalArgumentException(
                "Invalid URI. Bucket name is required: 'gs://bucket/path/to/file'");
        }

        String path = uri.getPath();
        if (path == null || path.length() < 2) {
            throw new IllegalArgumentException(
                "Invalid URI. Path after bucket cannot be empty");
        }

        assert path.charAt(0) == '/' : "URI path starts with /";

        var globPredicate = new Globs.GlobPredicate(path.substring(1));
        return new GCSURI(bucket, path, globPredicate);
    }

    public String resourcePath() {
        return resourcePath;
    }

    @Nullable
    public String preGlobPath() {
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
        return resourcePath.substring(0, lastBeforeAsterisk + 1);
    }

    public boolean matchesGlob(String path) {
        return globPredicate.test(path);
    }
}
