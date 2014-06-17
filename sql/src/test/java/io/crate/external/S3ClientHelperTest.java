/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.external;

import com.amazonaws.services.s3.internal.Constants;
import org.junit.Test;

import java.net.URI;

import static junit.framework.Assert.assertNotNull;

public class S3ClientHelperTest {

    private final S3ClientHelper clientHelper = new S3ClientHelper(Constants.S3_HOSTNAME);
    private final AmazonS3ClientHelper s3ClientHelper = new AmazonS3ClientHelper();
    private final GoogleS3ClientHelper gsClientHelper = new GoogleS3ClientHelper();

    @Test
    public void testS3Client() throws Exception {
        assertNotNull(s3ClientHelper.client(new URI("s3://baz")));
        assertNotNull(s3ClientHelper.client(new URI("s3://baz/path/to/file")));
        assertNotNull(s3ClientHelper.client(new URI("s3://foo:inv%2Falid@baz")));
        assertNotNull(s3ClientHelper.client(new URI("s3://foo:inv%2Falid@baz/path/to/file")));
    }

    @Test
    public void testGSClient() throws Exception {
        assertNotNull(gsClientHelper.client(new URI("gs://baz")));
        assertNotNull(gsClientHelper.client(new URI("gs://baz/path/to/file")));
        assertNotNull(gsClientHelper.client(new URI("gs://foo:inv%2Falid@baz")));
        assertNotNull(gsClientHelper.client(new URI("gs://foo:inv%2Falid@baz/path/to/file")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongURIEncodingSecretKey() throws Exception {
        // 'inv/alid' should be 'inv%2Falid'
        // see http://en.wikipedia.org/wiki/UTF-8#Codepage_layout
        clientHelper.client(new URI("s3://foo:inv/alid@baz/path/to/file"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongURIEncodingAccessKey() throws Exception {
        // 'fo/o' should be 'fo%2Fo'
        // see http://en.wikipedia.org/wiki/UTF-8#Codepage_layout
        clientHelper.client(new URI("s3://fo/o:inv%2Falid@baz"));
    }

}
