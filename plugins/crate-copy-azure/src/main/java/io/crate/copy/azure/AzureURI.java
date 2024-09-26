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

import static io.crate.copy.azure.AzureCopyPlugin.USER_FACING_SCHEME;

import java.net.URI;

import io.crate.copy.OpenDalURI;
import io.crate.execution.engine.collect.files.Globs;

/**
 * Represents URI with 'az' scheme.
 */
public class AzureURI extends OpenDalURI {

    private final String account;
    private final String container;
    private final String endpoint;

    private AzureURI(URI uri,
                     String resourcePath,
                     Globs.GlobPredicate globPredicate,
                     String account,
                     String container,
                     String endpoint) {
        super(uri, resourcePath, globPredicate);
        this.account = account;
        this.container = container;
        this.endpoint = endpoint;
    }

    public static AzureURI of(URI uri) {
        if (uri.getScheme().equals(USER_FACING_SCHEME) == false) {
            throw new IllegalArgumentException("Invalid URI. URI must look like 'az://account.endpoint_suffix/container/path/to/file'");
        }
        String endpoint = uri.getHost();
        var port = uri.getPort();
        if (port != -1) {
            endpoint += ":" + port;
        }
        String path = uri.getPath();

        int dotIndex = endpoint.indexOf(".");
        if (dotIndex < 0) {
            throw new IllegalArgumentException("Invalid URI. URI must look like 'az://account.endpoint_suffix/container/path/to/file'");
        }
        String account = endpoint.substring(0, dotIndex);

        assert path.charAt(0) == '/' : "URI path starts with /";
        int secondSlashIndex = path.indexOf('/', 1); // Skip first slash;
        if (secondSlashIndex < 0) {
            throw new IllegalArgumentException("Invalid URI. URI must look like 'az://account.endpoint_suffix/container/path/to/file'");
        }

        if (path.length() - secondSlashIndex < 2) {
            // Impossible for COPY TO because even specifying root of the container leads to effective URI having also file name appended.
            // Impossible for COPY FROM because even specifying root of the container must be followed by '*'.
            throw new IllegalArgumentException("Invalid URI. Path after container cannot be empty");
        }

        String container = path.substring(1, secondSlashIndex);

        String resourcePath = path.substring(secondSlashIndex); // At least one symbol as path cannot be empty.
        // List API returns entries without leading backslash.
        var globPredicate = new Globs.GlobPredicate(resourcePath.substring(1));

        return new AzureURI(uri, resourcePath, globPredicate, account, container, endpoint);
    }

    public String account() {
        return account;
    }

    public String container() {
        return container;
    }

    public String endpoint() {
        return endpoint;
    }
}
