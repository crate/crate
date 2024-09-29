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

import org.jetbrains.annotations.VisibleForTesting;

import java.net.URI;
import java.util.Objects;

import io.crate.copy.OpenDalURI;
import io.crate.execution.engine.collect.files.Globs;

public class S3URI extends OpenDalURI {
    private static final String INVALID_URI_MSG = "Invalid URI. Please make sure that given URI is encoded properly.";

    private final URI uri;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;
    private final String endpoint;

    private S3URI(URI uri,
                  String resourcePath,
                  Globs.GlobPredicate globPredicate,
                  String accessKey,
                  String secretKey,
                  String bucket,
                  String endpoint) {
        super(uri, resourcePath, globPredicate);
        this.uri = uri;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
        this.endpoint = endpoint;
    }

    public static S3URI toS3URI(URI uri) {
        URI normalizedURI = normalize(uri);
        String userInfo = getUserInfo(normalizedURI);
        String accessKey = null;
        String secretKey = null;
        if (userInfo != null) {
            String[] userInfoParts = userInfo.split(":");
            try {
                accessKey = userInfoParts[0];
                secretKey = userInfoParts[1];
            } catch (ArrayIndexOutOfBoundsException e) {
                // ignore
            }
            // if the URI contains '@' and ':', a UserInfo is in fact given, but could not
            // be parsed properly because the URI is not valid (e.g. not properly encoded).
        } else if (normalizedURI.toString().contains("@") && normalizedURI.toString().contains(":")) {
            throw new IllegalArgumentException(INVALID_URI_MSG);
        }

        String bucket;
        String resourcePath;
        String path = normalizedURI.getPath().substring(1);
        int splitIndex = path.indexOf('/');
        if (splitIndex == -1) {
            bucket = path;
            resourcePath = "";
        } else {
            bucket = path.substring(0, splitIndex);
            resourcePath = path.substring(splitIndex);
        }

        String endpoint = null;
        if (normalizedURI.getHost() != null) {
            endpoint = normalizedURI.getHost() + ":" + normalizedURI.getPort();
        }

        // List API returns entries without leading backslash.
        var globPredicate = new Globs.GlobPredicate(resourcePath.substring(1));

        return new S3URI(normalizedURI, resourcePath, globPredicate, accessKey, secretKey, bucket, endpoint);
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

    /**
     * As shown in <a href="https://crate.io/docs/crate/reference/en/latest/sql/statements/copy-from.html#sql-copy-from-s3">CrateDB Reference</a>,
     * the accepted s3 uri format is:
     * <pre>{@code
     * s3://[<accesskey>:<secretkey>@][<host>:<port>/]<bucketname>/<path>}
     * </pre>
     * which is inconsistent with {@link URI}.
     * <p>
     * For example, s3://bucket is an acceptable s3 uri to CrateDB but {@link URI} parses "bucket" as the host instead of the path.
     * <p>
     * Another example is, s3://bucket/key1/key2, another valid CrateDB s3 uri. Again, URI parses "bucket" as the host and "/key1/key2" as the path.
     * <p>
     * This method resolved this inconsistency such that URI can be utilized.
     *
     * @param uri a valid s3 uri
     * @return a normalized uri such that the path component contains the bucket and the key
     */
    private static URI normalize(URI uri) {
        assert "s3".equals(uri.getScheme());
        if (uri.getHost() != null) {
            if (uri.getPath() == null || uri.getPort() == -1) {
                return URI.create("s3://"
                                  + (uri.getRawUserInfo() == null ? "" : uri.getRawUserInfo() + "@")
                                  + "/"
                                  + uri.getHost()
                                  + uri.getPath());
            }
        }
        return uri;
    }

    @Override
    public String toString() {
        return uri.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof S3URI that)) {
            return false;
        }
        return uri.equals(that.uri) &&
               Objects.equals(accessKey, that.accessKey) &&
               Objects.equals(secretKey, that.secretKey) &&
               Objects.equals(bucket, that.bucket) &&
               Objects.equals(endpoint, that.endpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, accessKey, secretKey, bucket, endpoint);
    }

    public String bucket() {
        return bucket;
    }

    public String accessKey() {
        return accessKey;
    }

    public String secretKey() {
        return secretKey;
    }

    public String endpoint() {
        return endpoint;
    }
}
