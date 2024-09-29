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
import java.util.List;

import static io.crate.copy.s3.common.S3URI.getUserInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3URITest {

    @Test
    public void testToS3URIMethod() {
        assertValidS3URI(
            "s3:///hostname:9000/*/*/a", // bucket named 'hostname:9000'
            null, null, null, -1, "hostname:9000", "/*/*/a");
        assertValidS3URI(
            "s3://minioadmin:minioadmin@play.min.io:9000/mj.myb/localhost:9000/",
            "minioadmin", "minioadmin", "play.min.io", 9000, "mj.myb", "/localhost:9000/");
        assertValidS3URI(
            "s3://minioadmin:minioadmin@/mj.myb/localhost:9000/",
            "minioadmin", "minioadmin", null, -1, "mj.myb", "/localhost:9000/");
        assertValidS3URI(
            "s3://b/prefix/", // trailing '/'
            null, null, null, -1, "b", "/prefix/");
        assertValidS3URI(
            "s3:/b/", // no key
            null, null, null, -1, "b", "/");
        assertValidS3URI(
            "s3://h:7/b/k",
            null, null, "h", 7, "b", "/k");
        assertValidS3URI(
            "s3://@h:7/b/*",
            null, null, "h", 7, "b", "/*");
        assertValidS3URI(
            "s3://access%2F:secret%2F@a/b", // url-encoded access & secret keys are decoded
            "access/", "secret/", null, -1, "a", "/b");
        assertValidS3URI(
            // host should be present with a port otherwise it will be assumed to be a bucket name
            "s3://host/bucket/key/*",
            null,
            null,
            null,
            -1,
            "host",
            "/bucket/key/*");
//        assertValidS3URI(
//            "s3:///",
//            null, null, null, -1, "", "//");
    }

    @Test
    public void testWrongURIEncodingSecretKey() {
        // 'inv/alid' should be 'inv%2Falid'
        // see http://en.wikipedia.org/wiki/UTF-8#Codepage_layout
        assertThatThrownBy(
            () -> S3URI.toS3URI(new URI("s3://foo:inv/alid@/baz/path/to/file")))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. Please make sure that given URI is encoded properly.");
    }

    @Test
    public void testWrongURIEncodingAccessKey() {
        // 'fo/o' should be 'fo%2Fo'
        // see http://en.wikipedia.org/wiki/UTF-8#Codepage_layout
        assertThatThrownBy(
            () -> S3URI.toS3URI(new URI("s3://fo/o:inv%2Falid@/baz")))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. Please make sure that given URI is encoded properly.");
    }

    @Test
    public void test_toPreGlobUri() {
        var uris =
            List.of(
                "s3:///fakeBucket3/prefix*/*.json",
                "s3://fakeBucket3/*/prefix2/prefix3/a.json",
                "s3://fakeBucket3/prefix/p*x/*/*.json",
                "s3://fake.Bucket/prefix/key*",
                "s3://minio:minio@play.min.io:9000/myBucket/myKey/*",
                "s3://play.min.io:9000/myBucket/myKey/*",
                "s3://minio:minio@myBucket/myKey/*"
            );
        var preGlobURIs = uris.stream()
            .map(URI::create)
            .map(S3URI::toS3URI)
            .map(S3URI::preGlobPath)
            .toList();
        assertThat(preGlobURIs).isEqualTo(List.of(
            "/",
            "/",
            "/prefix/",
            "/prefix/",
            "/myKey/",
            "/myKey/",
            "/myKey/"
        ));
    }

    @Test
    public void test_match_glob_pattern() throws Exception {
        List<String> entries = List.of(
            "prefix/dir1/dir2/match1.json",
            // Too many subdirectories, see https://cratedb.com/docs/crate/reference/en/latest/sql/statements/copy-from.html#uri-globbing
            "prefix/dir1/dir2/dir3/no_match.json",
            "prefix/dir2/dir1/no_match.json",
            "prefix/dir1/dir0/dir2/no_match.json"
        );

        S3URI s3URI = S3URI.toS3URI(URI.create("s3://fakeBucket/prefix/dir1/dir2/*"));

        assertThat(s3URI.preGlobPath()).isNotNull();

        List<String> matches = entries.stream().filter(s3URI::matchesGlob).toList();
        assertThat(matches).containsExactly("prefix/dir1/dir2/match1.json");
    }

    private void assertValidS3URI(String toBeParsed,
                                  String accessKey,
                                  String secretKey,
                                  String host,
                                  int port,
                                  String bucketName,
                                  String resourcePath) {
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
        if (resourcePath != null) {
            assertThat(uri.getPath()).endsWith(resourcePath);
            assertThat(resourcePath).isEqualTo(s3URI.resourcePath());
        }
    }
}
