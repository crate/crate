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

package io.crate.copy.s3.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;

import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Test;

public class S3ClientHelperTest extends ESTestCase {

    private final S3ClientHelper s3ClientHelper = new S3ClientHelper();

    @After
    public void cleanUpS3() {
        s3ClientHelper.close();
    }

    @Test
    public void testClient() throws Exception {
        assertThat(s3ClientHelper.client(S3URI.toS3URI(new URI("s3:///baz")))).isNotNull();
        assertThat(s3ClientHelper.client(S3URI.toS3URI(new URI("s3:///baz/path/to/file")))).isNotNull();
        assertThat(s3ClientHelper.client(S3URI.toS3URI(new URI("s3://foo:inv%2Falid@/baz")))).isNotNull();
        assertThat(s3ClientHelper.client(S3URI.toS3URI(new URI("s3://foo:inv%2Falid@/baz/path/to/file")))).isNotNull();
        assertThat(s3ClientHelper.client(S3URI.toS3URI(new URI("s3://foo:inv%2Falid@host:9000/baz/path/to/file")))).isNotNull();
        assertThat(s3ClientHelper.client(S3URI.toS3URI(new URI("s3://host:9000/baz/path/to/file")))).isNotNull();
        assertThat(s3ClientHelper.client(S3URI.toS3URI(new URI("s3://secret:access@/bucket/key")))).isNotNull();
    }

    @Test
    public void testWrongURIEncodingSecretKey() {
        // 'inv/alid' should be 'inv%2Falid'
        // see http://en.wikipedia.org/wiki/UTF-8#Codepage_layout
        assertThatThrownBy(
            () -> s3ClientHelper.client(S3URI.toS3URI(new URI("s3://foo:inv/alid@/baz/path/to/file"))))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. Please make sure that given URI is encoded properly.");
    }

    @Test
    public void testWrongURIEncodingAccessKey() {
        // 'fo/o' should be 'fo%2Fo'
        // see http://en.wikipedia.org/wiki/UTF-8#Codepage_layout
        assertThatThrownBy(
            () -> s3ClientHelper.client(S3URI.toS3URI(new URI("s3://fo/o:inv%2Falid@/baz"))))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. Please make sure that given URI is encoded properly.");
    }
}
