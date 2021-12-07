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

package io.crate.copy.s3;

import io.crate.execution.engine.collect.files.FileReadingIterator;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3URITest {

    @Test
    public void testValidS3URIs() {
        assertValidS3URI(
            "s3:///hostname:9000/*/*/a", // bucket named 'hostname:9000'
            null, null, null, -1, "hostname:9000", "*/*/a");
        assertValidS3URI(
            "s3://minioadmin:minioadmin@play.min.io:9000/mjmyb/localhost:9000/", // key named 'hostname:9000'
            "minioadmin", "minioadmin", "play.min.io", 9000, "mjmyb", "localhost:9000/");
        assertValidS3URI(
            "s3:///b/k",
            null, null, null, -1, "b", "k");
        assertValidS3URI(
            "s3:/b/k",
            null, null, null, -1, "b", "k");
        assertValidS3URI(
            "s3:/b/", // no key
            null, null, null, -1, "b", "");
        assertValidS3URI(
            "s3:/b", // no key
            null, null, null, -1, "b", "");
        assertValidS3URI(
            "s3://@/b/k",
            null, null, null, -1, "b", "k");
        assertValidS3URI(
            "s3://h:7/b/k",
            null, null, "h", 7, "b", "k");
        assertValidS3URI(
            "s3://@h:7/b/*",
            null, null, "h", 7, "b", "*");
        assertValidS3URI(
            "s3://@h:7/*/*",
            null, null, "h", 7, "*", "*");
        assertValidS3URI(
            // host should be presented with a port otherwise it will be assumed to be a bucket name
            "s3://@host/bucket/key/*",
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
        S3URI fixedS3URI = new S3URI(FileReadingIterator.toURI(toBeParsed));
        URI fixedURI = fixedS3URI.uri;
        if (accessKey != null) {
            assertNotNull(secretKey);
            assertThat(fixedURI.getRawUserInfo(), is(accessKey + ":" + secretKey));
        }
        if (host != null) {
            assertThat(fixedURI.getHost(), is(host));
        }
        assertThat(fixedURI.getPort(), is(port));
        if (bucketName != null) {
            assertTrue(fixedURI.getPath().startsWith("/" + bucketName));
            assertEquals(bucketName, fixedS3URI.bucket);
        }
        if (key != null) {
            assertTrue(fixedURI.getPath().endsWith(key));
            assertEquals(key, fixedS3URI.key);
        }
    }
}
