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

package io.crate.execution.engine.collect;

import io.crate.execution.engine.collect.files.FileReadingIterator;

import java.net.URI;

public class S3URIToBeRemoved {
    public final URI uri;
    public final String bucket;
    public final String key;

    public S3URIToBeRemoved(URI uri) {
        this.uri = reformat(uri);
        String path = this.uri.getPath().substring(1);
        int splitIndex = path.indexOf('/');
        if (splitIndex == -1) {
            this.bucket = path;
            this.key = "";
        } else {
            this.bucket = path.substring(0, splitIndex);
            this.key = path.substring(splitIndex + 1);
        }
    }

    static URI reformat(URI uri) {
        if (uri.getHost() != null) {
            if (uri.getPath() == null || uri.getPort() == -1) {
                // This addresses the following issues:
                // ex1) s3://bucket
                //      URI considers 'bucket' as a host name,
                //      but CrateDB (which accepts s3:// formatting) expects 'bucket' to be part of path component and eventually as bucket name
                // ex2) s3://bucket/key1/key2
                //      URI parses this into host: bucket and path: /key1/key2
                //      but CrateDB expects this to become path: /bucket/key1/key2 and eventually as bucket: bucket and key: /key1/key2
                return FileReadingIterator.toURI("s3://"
                                                 + (uri.getRawUserInfo() == null ? "" : uri.getRawUserInfo() + "@")
                                                 + "/"
                                                 + uri.getHost()
                                                 + uri.getPath()
                );
            }
        }
        return uri;
    }
}
