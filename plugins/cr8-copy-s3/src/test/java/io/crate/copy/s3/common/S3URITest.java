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

import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.crate.copy.s3.common.S3URI.getUserInfo;
import static org.assertj.core.api.Assertions.assertThat;

class S3URITest {

    @Test
    public void testToS3URIMethod() {
        assertValidS3URI(
            "s3:///hostname:9000/*/*/a", // bucket named 'hostname:9000'
            null, null, null, -1, "hostname:9000", "*/*/a");
        assertValidS3URI(
            "s3://minioadmin:minioadmin@play.min.io:9000/mj.myb/localhost:9000/",
            "minioadmin", "minioadmin", "play.min.io", 9000, "mj.myb", "localhost:9000/");
        assertValidS3URI(
            "s3://minioadmin:minioadmin@/mj.myb/localhost:9000/",
            "minioadmin", "minioadmin", null, -1, "mj.myb", "localhost:9000/");
        assertValidS3URI(
            "s3://b/prefix/", // trailing '/'
            null, null, null, -1, "b", "prefix/");
        assertValidS3URI(
            "s3:/b/", // no key
            null, null, null, -1, "b", "");
        assertValidS3URI(
            "s3:/b", // no key
            null, null, null, -1, "b", "");
        assertValidS3URI(
            "s3://h:7/b/k",
            null, null, "h", 7, "b", "k");
        assertValidS3URI(
            "s3://@h:7/b/*",
            null, null, "h", 7, "b", "*");
        assertValidS3URI(
            "s3://access%2F:secret%2F@a/b", // url-encoded access & secret keys are decoded
            "access/", "secret/", null, -1, "a", "b");
        assertValidS3URI(
            // host should be present with a port otherwise it will be assumed to be a bucket name
            "s3://host/bucket/key/*",
            null,
            null,
            null,
            -1,
            "host",
            "bucket/key/*");
        assertValidS3URI(
            "s3:///",
            null, null, null, -1, "", "");
    }

    private void assertValidS3URI(String toBeParsed,
                                  String accessKey,
                                  String secretKey,
                                  String host,
                                  int port,
                                  String bucketName,
                                  String key) {
        S3URI s3URI = S3URI.toS3URI(URI.create(toBeParsed));
        URI uri = s3URI.uri();
        if (accessKey != null) {
            assertThat(secretKey).isNotNull();
            assertThat(s3URI.accessKey() + ":" + s3URI.secretKey()).isEqualTo(accessKey + ":" + secretKey);
            assertThat(getUserInfo(uri)).isEqualTo(accessKey + ":" + secretKey);
        }
        if (host != null) {
            assertThat(uri.getHost()).isEqualTo(host);
            assertThat(s3URI.endpoint()).startsWith(uri.getHost() + ":");
            assertThat(s3URI.endpoint()).endsWith(":" + uri.getPort());
        }
        assertThat(uri.getPort()).isEqualTo(port);
        if (bucketName != null) {
            assertThat(uri.getPath()).startsWith("/" + bucketName);
            assertThat(bucketName).isEqualTo(s3URI.bucket());
        }
        if (key != null) {
            assertThat(uri.getPath()).endsWith(key);
            assertThat(key).isEqualTo(s3URI.key());
        }
    }

    @Test
    public void testReplacePathMethod() {
        S3URI s3URI = S3URI.toS3URI(URI.create("s3://minioadmin:minio%2Fadmin@mj.myb/localhost:9000/"));
        S3URI replacedURI = s3URI.replacePath("new.Bucket", "newKey");
        assertThat(replacedURI.uri().toString()).isEqualTo("s3://minioadmin:minio%2Fadmin@/new.Bucket/newKey");

        s3URI = S3URI.toS3URI(URI.create("s3://host:123/myb"));
        replacedURI = s3URI.replacePath("new.Bucket", "newKey");
        assertThat(replacedURI.uri().toString()).isEqualTo("s3://host:123/new.Bucket/newKey");
    }
}
