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

import java.net.URI;

/**
 * Convert S3:// formatted bucket reference to {@link java.net.URI}
 */
//https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html#accessing-a-bucket-using-S3-format
public class URIHelper {

    public static String convertToURI(String s3FormattedBucketReference) {
        URI brokenURI = FileReadingIterator.toURI(s3FormattedBucketReference);

        if (brokenURI.getHost() != null) {
            if (brokenURI.getPath() == null || brokenURI.getPort() == -1) {
                // This addresses the following issues:
                // ex1) s3://bucket
                //      URI considers 'bucket' as a host name,
                //      but CrateDB (which accepts s3:// formatting) expects 'bucket' to be part of path component and eventually as bucket name
                // ex2) s3://bucket/key1/key2
                //      URI parses this into host: bucket and path: /key1/key2
                //      but CrateDB expects this to become path: /bucket/key1/key2 and eventually as bucket: bucket and key: /key1/key2
                return "s3://"
                       + (brokenURI.getRawUserInfo() == null ? "" : brokenURI.getRawUserInfo() + "@")
                       + "/"
                       + brokenURI.getHost()
                       + brokenURI.getPath();
            }
        }
        return s3FormattedBucketReference;
    }
}
