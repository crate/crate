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

package io.crate.copy.s3.common;


import java.net.URI;

import org.jspecify.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;

public record S3URI(String bucket,
                    String path,
                    /* endpoint is without http[s]:// prefix */
                    @Nullable String endpoint,
                    @Nullable String accessKey,
                    @Nullable String secretKey) {

    private static final String INVALID_URI_MSG = "Invalid URI. Please make sure that given URI is encoded properly.";

    /// Parse a S3 URI as described in https://crate.io/docs/crate/reference/en/latest/sql/statements/copy-from.html#sql-copy-from-s3
    public static S3URI of(URI uri) {
        // Due to the optional host:port part, the bucketname is either the host or in the path of the URI
        // If there is no port as in `s3://example/foo/bar` then `example` is assumed to be the bucketname
        String host = uri.getHost();
        int port = uri.getPort();
        String accessKey = null;
        String secretKey = null;
        String userInfo = uri.getUserInfo();
        String authority = uri.getAuthority();
        int atIdx = authority == null
            ? -1
            : authority.indexOf("@");
        if (userInfo == null && atIdx >= 0) {
            userInfo = authority.substring(0, atIdx);
        }
        if (userInfo != null) {
            String[] parts = userInfo.split(":");
            accessKey = parts[0];
            if (parts.length > 1) {
                secretKey = parts[1];
            }
        }
        boolean bucketInPath = host != null && port > -1 || atIdx > -1 || authority == null;
        String bucket;
        String path;
        String endpoint;
        String uriPath = uri.getPath();
        if (uriPath.startsWith("/")) {
            uriPath = uriPath.substring(1);
        }
        if (bucketInPath) {
            int bucketEnd = uriPath.indexOf("/");
            if (bucketEnd == -1) {
                bucket = uriPath;
                path = "";
            } else {
                bucket = uriPath.substring(0, bucketEnd);
                path = uriPath.substring(bucketEnd + 1);
            }
            endpoint = port == -1 ? host : host + ":" + Integer.toString(port);
        } else {
            bucket = authority;
            path = uriPath;
            endpoint = null;
        }
        return new S3URI(
            bucket,
            path.endsWith("/") ? path.substring(0, path.length() - 1) : path,
            endpoint,
            accessKey,
            secretKey
        );
    }

    @VisibleForTesting
    static String getUserInfo(URI uri) {
        // userInfo is provided but is contained in authority, NOT in userInfo. happens when host is not provided
        String userInfo = null;
        if (uri.getHost() == null && uri.getPort() == -1 && uri.getUserInfo() == null) {
            var authority = uri.getAuthority();
            if (authority != null) {
                int idx = authority.indexOf('@');
                if (idx != authority.length() - 1) {
                    throw new IllegalArgumentException(INVALID_URI_MSG);
                }
                userInfo = uri.getAuthority().substring(0, idx);
            }
        } else {
            userInfo = uri.getUserInfo();
        }
        return userInfo;
    }
}
